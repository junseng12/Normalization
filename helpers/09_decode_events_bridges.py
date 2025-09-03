# helpers/run_pipeline_with_etl.py
# - 중간 파일은 OS 임시 폴더에만 잠깐 생성/즉시 삭제. 최종물만 out/ 저장
# - ethereum-etl==2.4.2 가정: receipts는 --transaction-hashes만 사용
# - 429(CUPS) 자동 재시도(동시성/배치 다운시프트), web3 미사용

import os, sys, json, subprocess, tempfile, math, shlex, time
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_utils import to_checksum_address
import requests

load_dotenv(dotenv_path=".env")
RPC_URL         = os.getenv("RPC_URL")
OUT_DIR         = Path(os.getenv("OUT_DIR", "out"))
OUTPUT_FORMAT   = os.getenv("OUTPUT_FORMAT", "parquet").lower()
SAFE_LAG        = int(os.getenv("SAFE_LAG", "12"))

# 보수적 기본값(Alchemy 무료/체험 안전)
ETL_MAX_WORKERS = int(os.getenv("ETL_MAX_WORKERS", "1"))
ETL_BATCH_SIZE  = int(os.getenv("ETL_BATCH_SIZE",  "2"))
RECEIPTS_MAX_WORKERS = int(os.getenv("RECEIPTS_MAX_WORKERS", ETL_MAX_WORKERS))
RECEIPTS_BATCH_SIZE  = int(os.getenv("RECEIPTS_BATCH_SIZE",  ETL_BATCH_SIZE))
TX_HASH_CHUNK   = int(os.getenv("TX_HASH_CHUNK", "500"))
USE_TXS_FROM_BLOCKS = os.getenv("USE_TXS_FROM_BLOCKS", "0") == "1"

ETL_BIN   = os.getenv("ETL_BIN", 'python -m pipx run --spec "ethereum-etl==2.4.2" ethereumetl')
ETL_PREFIX = shlex.split(ETL_BIN)

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- JSON-RPC ----------
def rpc(method, params=None, timeout=30):
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params or []}
    r = requests.post(RPC_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]

def latest_safe_block():
    latest_hex = rpc("eth_blockNumber")
    latest = int(latest_hex, 16)
    return max(0, latest - SAFE_LAG)

# ---------- 실행 헬퍼 ----------
def run_cli(args_list):
    full = ETL_PREFIX + args_list
    print(">>", " ".join(shlex.quote(a) for a in full))
    cp = subprocess.run(full, capture_output=True, text=True, shell=False)
    if cp.stdout:
        print(cp.stdout, end="")
    if cp.stderr:
        print(cp.stderr, end="", file=sys.stderr)
    return cp.returncode

def run_cli_or_raise(args_list):
    code = run_cli(args_list)
    if code != 0:
        raise RuntimeError(f"Command failed (exit={code})")

def save_df(df: pd.DataFrame, name: str):
    path = OUT_DIR / f"{name}.{'parquet' if OUTPUT_FORMAT=='parquet' else 'csv'}"
    if OUTPUT_FORMAT == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    print(f"[saved] {path}")

# ---------- 디코딩 유틸 ----------
def topic_to_addr(topic_hex: str) -> str:
    try:
        if not isinstance(topic_hex, str) or not topic_hex.startswith("0x"): return ""
        return to_checksum_address("0x" + topic_hex[-40:])
    except Exception:
        return ""

def ensure_topic_cols(logs: pd.DataFrame):
    # topic0..topic2 없고 'topics'만 있으면 파싱해서 생성
    need = any(c not in logs.columns for c in ["topic0","topic1","topic2"])
    if need and "topics" in logs.columns:
        def pick(topic_str, idx):
            try:
                arr = json.loads(topic_str) if isinstance(topic_str, str) else []
                return arr[idx] if len(arr) > idx else None
            except Exception:
                return None
        logs["topic0"] = logs["topics"].apply(lambda s: pick(s,0))
        logs["topic1"] = logs["topics"].apply(lambda s: pick(s,1))
        logs["topic2"] = logs["topics"].apply(lambda s: pick(s,2))
    for c in ["topic0","topic1","topic2","data","address","transaction_hash","log_index"]:
        if c not in logs.columns:
            logs[c] = pd.NA
    return logs

def decode_all_df(logs: pd.DataFrame, topics_json: str, addresses_json: str):
    topics = json.load(open(topics_json))
    addrs  = json.load(open(addresses_json))
    BRIDGES = {a.lower() for a in addrs.get("bridges", [])}

    logs = logs.copy()
    logs.columns = [c.lower() for c in logs.columns]
    logs = ensure_topic_cols(logs)

    # ERC20 Transfer
    tf = logs[logs["topic0"].astype(str).str.lower() == topics["erc20_transfer"].lower()].copy()
    tf["from"] = tf["topic1"].astype(str).apply(topic_to_addr).str.lower()
    tf["to"]   = tf["topic2"].astype(str).apply(topic_to_addr).str.lower()
    tf["amount_raw"] = tf["data"].astype(str).apply(lambda x: int(x,16) if isinstance(x,str) and x.startswith("0x") else 0)
    transfers = tf[["transaction_hash","log_index","address","from","to","amount_raw"]].rename(
        columns={"transaction_hash":"tx_hash","address":"token_address"})

    # Uniswap V2/V3
    v2 = logs[logs["topic0"].astype(str).str.lower() == topics["univ2_swap"].lower()].copy()
    v3 = logs[logs["topic0"].astype(str).str.lower() == topics["univ3_swap"].lower()].copy()

    def _decode_v2(row):
        d = row["data"]
        if isinstance(d, str) and d.startswith("0x"):
            a0i,a1i,a0o,a1o = abi_decode(["uint256","uint256","uint256","uint256"], bytes.fromhex(d[2:]))
            return pd.Series([a0i,a1i,a0o,a1o])
        return pd.Series([0,0,0,0])
    if not v2.empty:
        v2[["a0i","a1i","a0o","a1o"]] = v2.apply(_decode_v2, axis=1)
    else:
        v2[["a0i","a1i","a0o","a1o"]] = 0

    def _decode_v3(row):
        d = row["data"]
        if isinstance(d, str) and d.startswith("0x"):
            a0,a1,_,_,_ = abi_decode(["int256","int256","uint160","uint128","int24"], bytes.fromhex(d[2:]))
            return pd.Series([a0,a1])
        return pd.Series([0,0])
    if not v3.empty:
        v3[["a0","a1"]] = v3.apply(_decode_v3, axis=1)
    else:
        v3[["a0","a1"]] = 0

    swaps_v2 = v2[["transaction_hash","log_index","address","a0i","a1i","a0o","a1o"]].rename(
        columns={"transaction_hash":"tx_hash","address":"pair_or_pool"})
    swaps_v2["dex"]="UNI-V2"

    swaps_v3 = v3[["transaction_hash","log_index","address","a0","a1"]].rename(
        columns={"transaction_hash":"tx_hash","address":"pair_or_pool"})
    swaps_v3["dex"]="UNI-V3"

    swaps = pd.concat([swaps_v2, swaps_v3], ignore_index=True)

    # Wormhole (sequence/nonce 간이 추출)
    worm = logs[logs["topic0"].astype(str).str.lower() == topics["wormhole_log"].lower()].copy()
    def _decode_worm(row):
        d = row["data"]
        if isinstance(d, str) and d.startswith("0x"):
            try:
                seq, nonce = abi_decode(["uint64","uint32"], bytes.fromhex(d[2:12*2]))
                return pd.Series([str(seq), str(nonce)])
            except Exception:
                pass
        return pd.Series([None, None])
    if not worm.empty:
        worm[["sequence","nonce"]] = worm.apply(_decode_worm, axis=1)
    bridges = worm[["transaction_hash","log_index","sequence","nonce"]].rename(columns={"transaction_hash":"tx_hash"})

    if not bridges.empty and not transfers.empty and BRIDGES:
        tsub = transfers[transfers["to"].isin(BRIDGES)]
        est = tsub.groupby("tx_hash").head(1)[["tx_hash","amount_raw","token_address"]]
        bridges = bridges.merge(est, on="tx_hash", how="left")

    return transfers, swaps, bridges

# ---------- 429 대응: receipts 청크 실행 + 다운시프트 ----------
def run_receipts_chunk(txfile: Path, rcpt_path: Path, logs_path: Path,
                       max_workers: int, batch_size: int,
                       max_retries: int = 4, backoff: float = 2.0):
    attempt = 0
    mw, bs = max_workers, batch_size
    while True:
        attempt += 1
        code = run_cli([
            "export_receipts_and_logs",
            "--transaction-hashes", str(txfile),
            "--provider-uri", RPC_URL,
            "--max-workers",  str(mw),
            "--batch-size",   str(bs),
            "--receipts-output", str(rcpt_path),
            "--logs-output",     str(logs_path),
        ])
        if code == 0:
            return
        # stderr에 429가 찍혔는지 여부는 여기선 코드만 보고 보수적으로 다운시프트
        if mw > 1:
            mw = max(1, mw // 2)
        elif bs > 1:
            bs = max(1, bs // 2)
        else:
            raise RuntimeError(f"receipts chunk failed (exit={code}) and no more downshift")
        sleep_s = backoff ** attempt
        print(f"[retry {attempt}] receipts downshift → workers={mw}, batch={bs}; {sleep_s:.1f}s sleep")
        time.sleep(sleep_s)

# ---------- 메인 ----------
def main(start_block: int, end_block: int, safe: bool = True):
    if safe:
        end_block = min(end_block, latest_safe_block())

    with tempfile.TemporaryDirectory() as tmpdir:
        TMP = Path(tmpdir)

        # 1) 토큰 전송 기반 해시 수집
        token_csv = TMP/"token_transfers.csv"
        run_cli_or_raise([
            "export_token_transfers",
            "--start-block", str(start_block),
            "--end-block",   str(end_block),
            "--provider-uri", RPC_URL,
            "--max-workers",  str(ETL_MAX_WORKERS),
            "--batch-size",   str(ETL_BATCH_SIZE),
            "--output",       str(token_csv),
        ])
        tx_hashes = set()
        if token_csv.exists():
            df_tt = pd.read_csv(token_csv, usecols=["transaction_hash"])
            tx_hashes = set(df_tt["transaction_hash"].dropna().tolist())

        # (옵션) 블록 전체 TX 포함
        if USE_TXS_FROM_BLOCKS:
            txs_csv = TMP/f"txs_{start_block}_{end_block}.csv"
            blocks_csv = TMP/f"blocks_{start_block}_{end_block}.csv"
            run_cli_or_raise([
                "export_blocks_and_transactions",
                "--start-block", str(start_block),
                "--end-block",   str(end_block),
                "--provider-uri", RPC_URL,
                "--max-workers",  str(ETL_MAX_WORKERS),
                "--batch-size",   str(ETL_BATCH_SIZE),
                "--blocks-output",       str(blocks_csv),
                "--transactions-output", str(txs_csv),
            ])
            df_txs = pd.read_csv(txs_csv, usecols=["hash"])
            tx_hashes |= set(df_txs["hash"].dropna().tolist())

        tx_hashes = sorted(tx_hashes)
        if not tx_hashes:
            print("no tx found in the given range"); return

        # 2) receipts/logs: 청크 + 즉시 로드 + 즉시 삭제
        receipts_list = []
        logs_list = []
        total = len(tx_hashes)
        chunks = math.ceil(total / TX_HASH_CHUNK)
        for i in range(chunks):
            sub = tx_hashes[i*TX_HASH_CHUNK:(i+1)*TX_HASH_CHUNK]
            txfile = TMP/f"tx_{i}.txt"
            rcpt_csv = TMP/f"rcpt_{i}.csv"
            logs_csv = TMP/f"logs_{i}.csv"
            Path(txfile).write_text("\n".join(sub))
            run_receipts_chunk(
                txfile=txfile,
                rcpt_path=rcpt_csv,
                logs_path=logs_csv,
                max_workers=RECEIPTS_MAX_WORKERS,
                batch_size=RECEIPTS_BATCH_SIZE,
            )
            receipts_list.append(pd.read_csv(rcpt_csv))
            logs_list.append(pd.read_csv(logs_csv))
            # 청크 파일 즉시 삭제
            for p in [txfile, rcpt_csv, logs_csv]:
                try: p.unlink()
                except: pass

        rcpt_df = pd.concat(receipts_list, ignore_index=True)
        logs_df = pd.concat(logs_list, ignore_index=True)

        # 3) blocks & transactions — receipts 등장 블록만
        r = rcpt_df[["block_number","transaction_hash","status","gas_used","effective_gas_price"]].copy()
        bmin, bmax = int(r["block_number"].min()), int(r["block_number"].max())
        blocks_csv2 = TMP/f"blocks_{bmin}_{bmax}.csv"
        txs_csv2    = TMP/f"txs_{bmin}_{bmax}.csv"
        run_cli_or_raise([
            "export_blocks_and_transactions",
            "--start-block", str(bmin),
            "--end-block",   str(bmax),
            "--provider-uri", RPC_URL,
            "--max-workers",  str(ETL_MAX_WORKERS),
            "--batch-size",   str(ETL_BATCH_SIZE),
            "--blocks-output",       str(blocks_csv2),
            "--transactions-output", str(txs_csv2),
        ])
        blocks = pd.read_csv(blocks_csv2, usecols=["number","hash","timestamp"]).rename(
            columns={"number":"block_number","hash":"block_hash","timestamp":"ts_utc"})
        txs = pd.read_csv(txs_csv2, usecols=["hash","block_number","from_address","to_address","value","input","type"]).rename(
            columns={"hash":"tx_hash","from_address":"from","to_address":"to","value":"value_wei"})
        # 즉시 삭제
        for p in [blocks_csv2, txs_csv2]:
            try: Path(p).unlink()
            except: pass

        # 4) 정규화 (최종만 저장)
        r = r.rename(columns={"transaction_hash":"tx_hash"})
        txs = txs[txs["tx_hash"].isin(tx_hashes)]
        df = (txs.merge(blocks, on="block_number", how="left")
                 .merge(r,      on="tx_hash",      how="left"))
        df["gas_fee_eth"]    = (df["gas_used"].fillna(0) * df["effective_gas_price"].fillna(0)) / 1e18
        df["input_selector"] = df["input"].fillna("0x").str.slice(0,10)
        df["to_is_contract"] = pd.NA
        cols = ["block_number","block_hash","ts_utc","tx_hash","from","to","type","status","value_wei",
                "gas_used","effective_gas_price","gas_fee_eth","input","input_selector","to_is_contract"]
        save_df(df[cols], "normalized")

        # 5) 디코딩(메모리 로그 → 최종만 저장)
        transfers, swaps, bridges = decode_all_df(
            logs=logs_df,
            topics_json=str(Path("config/topics.json")),
            addresses_json=str(Path("config/addresses.json")),
        )
        if not transfers.empty: save_df(transfers, "transfers")
        if not swaps.empty:     save_df(swaps, "dex_swaps")
        if not bridges.empty:   save_df(bridges, "bridge_events")

        print("DONE",
              len(df), "normalized;",
              0 if transfers is None else len(transfers), "transfers;",
              0 if swaps is None else len(swaps), "swaps;",
              0 if bridges is None else len(bridges), "bridges.")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end",   type=int, required=True)
    p.add_argument("--safe",  action="store_true")
    a = p.parse_args()
    t0 = time.perf_counter()
    if a.safe:
        print(f"[safe] latest_safe={latest_safe_block()}")
    main(a.start, a.end, a.safe)
    print(f"\n⏱ {time.perf_counter() - t0:.2f}s")
