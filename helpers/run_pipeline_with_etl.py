# helpers/run_pipeline_with_etl.py  (robust subprocess + chunking + fallback)
import os, sys, json, subprocess, tempfile, math, shlex
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_utils import to_checksum_address
import requests

load_dotenv()
RPC_URL        = os.getenv("RPC_URL")
OUT_DIR        = Path(os.getenv("OUT_DIR", "out"))
OUTPUT_FORMAT  = os.getenv("OUTPUT_FORMAT", "parquet").lower()
SAFE_LAG       = int(os.getenv("SAFE_LAG", "12"))
ETL_BATCH_SIZE = os.getenv("ETL_BATCH_SIZE", "10")
ETL_MAX_WORKERS= os.getenv("ETL_MAX_WORKERS", "6")
ETL_BIN        = os.getenv("ETL_BIN", 'python -m pipx run --spec "ethereum-etl==2.4.2" ethereumetl')

# 옵션: 큰 tx 목록일 때 분할 처리
TX_HASH_CHUNK  = int(os.getenv("TX_HASH_CHUNK", "5000"))
# 옵션: 영구 tmp 폴더(디버깅에 유용). 비우면 TemporaryDirectory 사용
TMP_DIR        = os.getenv("TMP_DIR", "").strip()
# 옵션: tx-hashes 대신 블록 범위 모드로 receipts/logs 추출
USE_BLOCK_RANGE_FOR_RECEIPTS = os.getenv("USE_BLOCK_RANGE_FOR_RECEIPTS", "0") == "1"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------- JSON-RPC (web3 없이 최신 블록) ----------
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

# --------- 안전 실행기 (args 리스트 방식) ----------
ETL_PREFIX = shlex.split(ETL_BIN)  # ex) ['python','-m','pipx','run','--spec','ethereum-etl==2.4.2','ethereumetl']

def run_args(args_list):
    full = ETL_PREFIX + args_list
    print(">>", " ".join(shlex.quote(a) for a in full))
    cp = subprocess.run(full, capture_output=True, text=True, shell=False)
    if cp.stdout:
        print(cp.stdout, end="")
    if cp.stderr:
        # stderr는 빨간색으로 보일 수 있도록 그대로 출력
        print(cp.stderr, end="", file=sys.stderr)
    if cp.returncode != 0:
        raise RuntimeError(f"Command failed with exit {cp.returncode}")

def save_df(df: pd.DataFrame, name: str):
    path = OUT_DIR / f"{name}.{'parquet' if OUTPUT_FORMAT=='parquet' else 'csv'}"
    if OUTPUT_FORMAT == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    print(f"[saved] {path}")

def topic_to_addr(topic_hex: str) -> str:
    try:
        if not isinstance(topic_hex, str) or not topic_hex.startswith("0x"): return ""
        return to_checksum_address("0x" + topic_hex[-40:])
    except Exception:
        return ""

def decode_all(logs_csv: str, topics_json: str, addresses_json: str):
    topics = json.load(open(topics_json))
    addrs  = json.load(open(addresses_json))
    BRIDGES = {a.lower() for a in addrs.get("bridges", [])}

    logs = pd.read_csv(logs_csv)
    logs.columns = [c.lower() for c in logs.columns]

    for c in ["transaction_hash","log_index","address","data","topic0","topic1","topic2"]:
        if c not in logs.columns: logs[c] = pd.NA

    # ERC20 Transfer
    tf = logs[logs["topic0"].astype(str).str.lower() == topics["erc20_transfer"].lower()].copy()
    tf["from"] = tf["topic1"].astype(str).apply(topic_to_addr).str.lower()
    tf["to"]   = tf["topic2"].astype(str).apply(topic_to_addr).str.lower()
    tf["amount_raw"] = tf["data"].astype(str).apply(lambda x: int(x,16) if isinstance(x,str) and x.startswith("0x") else 0)
    transfers = tf[["transaction_hash","log_index","address","from","to","amount_raw"]].rename(
        columns={"transaction_hash":"tx_hash","address":"token_address"})

    # UniV2 / UniV3
    v2 = logs[logs["topic0"].astype(str).str.lower() == topics["univ2_swap"].lower()].copy()
    v3 = logs[logs["topic0"].astype(str).str.lower() == topics["univ3_swap"].lower()].copy()

    def _decode_v2(row):
        data = row["data"]
        if isinstance(data, str) and data.startswith("0x"):
            a0i,a1i,a0o,a1o = abi_decode(["uint256","uint256","uint256","uint256"], bytes.fromhex(data[2:]))
            return pd.Series([a0i,a1i,a0o,a1o])
        return pd.Series([0,0,0,0])
    if not v2.empty:
        v2[["a0i","a1i","a0o","a1o"]] = v2.apply(_decode_v2, axis=1)
    else:
        v2[["a0i","a1i","a0o","a1o"]] = 0

    def _decode_v3(row):
        data = row["data"]
        if isinstance(data, str) and data.startswith("0x"):
            a0,a1,_,_,_ = abi_decode(["int256","int256","uint160","uint128","int24"], bytes.fromhex(data[2:]))
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

    # Wormhole
    worm = logs[logs["topic0"].astype(str).str.lower() == topics["wormhole_log"].lower()].copy()
    def _decode_worm(row):
        data = row["data"]
        if isinstance(data, str) and data.startswith("0x"):
            try:
                seq, nonce = abi_decode(["uint64","uint32"], bytes.fromhex(data[2:12*2]))
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

def main(start_block: int, end_block: int, safe: bool = True):
    if safe:
        end_block = min(end_block, latest_safe_block())

    # tmp 디렉터리 결정
    if TMP_DIR:
        Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
        tmp_ctx = None
        TMP = Path(TMP_DIR)
    else:
        tmp_ctx = tempfile.TemporaryDirectory()
        TMP = Path(tmp_ctx.name)

    try:
        # 1) 선별: token_transfers
        token_out = TMP/"token_transfers.csv"
        run_args([
            "export_token_transfers",
            "--start-block", str(start_block),
            "--end-block",   str(end_block),
            "--provider-uri", RPC_URL,
            "--max-workers",  str(ETL_MAX_WORKERS),
            "--batch-size",   str(ETL_BATCH_SIZE),
            "--output",       str(token_out)
        ])
        tt = pd.read_csv(token_out, usecols=["transaction_hash","block_number"])
        tx_hashes = sorted(set(tt["transaction_hash"].dropna().tolist()))
        if not tx_hashes:
            print("no tx found by token_transfers in range"); return

        # 2) receipts/logs
        if USE_BLOCK_RANGE_FOR_RECEIPTS:
            # 작은 범위면 이게 가장 안정적
            rcpt_out = TMP/"receipts.csv"
            logs_out = TMP/"logs.csv"
            run_args([
                "export_receipts_and_logs",
                "--start-block", str(start_block),
                "--end-block",   str(end_block),
                "--provider-uri", RPC_URL,
                "--max-workers",  str(ETL_MAX_WORKERS),
                "--batch-size",   str(ETL_BATCH_SIZE),
                "--receipts-output", str(rcpt_out),
                "--logs-output",     str(logs_out)
            ])
        else:
            # 큰 tx 목록 → 청크 분할 실행 후 병합
            rcpt_out = TMP/"receipts.csv"
            logs_out = TMP/"logs.csv"
            all_receipts = []
            all_logs = []
            total = len(tx_hashes)
            chunks = math.ceil(total / TX_HASH_CHUNK)
            for i in range(chunks):
                sub = tx_hashes[i*TX_HASH_CHUNK:(i+1)*TX_HASH_CHUNK]
                txfile = TMP/f"tx_hashes_{i}.txt"
                Path(txfile).write_text("\n".join(sub))
                sub_rcpt = TMP/f"receipts_{i}.csv"
                sub_logs = TMP/f"logs_{i}.csv"
                run_args([
                    "export_receipts_and_logs",
                    "--transaction-hashes", str(txfile),
                    "--provider-uri", RPC_URL,
                    "--max-workers",  str(ETL_MAX_WORKERS),
                    "--batch-size",   str(ETL_BATCH_SIZE),
                    "--receipts-output", str(sub_rcpt),
                    "--logs-output",     str(sub_logs)
                ])
                all_receipts.append(pd.read_csv(sub_rcpt))
                all_logs.append(pd.read_csv(sub_logs))
            pd.concat(all_receipts, ignore_index=True).to_csv(rcpt_out, index=False)
            pd.concat(all_logs,     ignore_index=True).to_csv(logs_out, index=False)

        # 3) blocks & transactions (receipts에서 실제 등장한 블록 구간)
        r = pd.read_csv(rcpt_out, usecols=["block_number","transaction_hash","status","gas_used","effective_gas_price"])
        bmin, bmax = int(r["block_number"].min()), int(r["block_number"].max())
        blocks_out = TMP/f"blocks_{bmin}_{bmax}.csv"
        txs_out    = TMP/f"txs_{bmin}_{bmax}.csv"
        run_args([
            "export_blocks_and_transactions",
            "--start-block", str(bmin),
            "--end-block",   str(bmax),
            "--provider-uri", RPC_URL,
            "--max-workers",  str(ETL_MAX_WORKERS),
            "--batch-size",   str(ETL_BATCH_SIZE),
            "--blocks-output",       str(blocks_out),
            "--transactions-output", str(txs_out)
        ])

        # 4) 정규화
        blocks = pd.read_csv(blocks_out, usecols=["number","hash","timestamp"]).rename(
            columns={"number":"block_number","hash":"block_hash","timestamp":"ts_utc"})
        txs = pd.read_csv(txs_out, usecols=["hash","block_number","from_address","to_address","value","input","type"]).rename(
            columns={"hash":"tx_hash","from_address":"from","to_address":"to","value":"value_wei"})
        rcpt = r.rename(columns={"transaction_hash":"tx_hash"})
        txs = txs[txs["tx_hash"].isin(tx_hashes)]
        df = (txs.merge(blocks, on="block_number", how="left")
                 .merge(rcpt,   on="tx_hash",      how="left"))
        df["gas_fee_eth"]    = (df["gas_used"].fillna(0) * df["effective_gas_price"].fillna(0)) / 1e18
        df["input_selector"] = df["input"].fillna("0x").str.slice(0,10)
        df["to_is_contract"] = pd.NA
        cols = ["block_number","block_hash","ts_utc","tx_hash","from","to","type","status","value_wei",
                "gas_used","effective_gas_price","gas_fee_eth","input","input_selector","to_is_contract"]
        save_df(df[cols], "normalized")

        # 5) 디코딩(ETL logs.csv)
        transfers, swaps, bridges = decode_all(
            logs_csv=str(logs_out),
            topics_json=str(Path("config/topics.json")),
            addresses_json=str(Path("config/addresses.json")),
        )
        if not transfers.empty: save_df(transfers, "transfers")
        if not swaps.empty:     save_df(swaps, "dex_swaps")
        if not bridges.empty:   save_df(bridges, "bridge_events")

        print("DONE", len(df), "normalized;",
              0 if transfers is None else len(transfers), "transfers;",
              0 if swaps is None else len(swaps), "swaps;",
              0 if bridges is None else len(bridges), "bridges.")
    finally:
        if tmp_ctx is not None:
            tmp_ctx.cleanup()

if __name__ == "__main__":
    import argparse, time
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end",   type=int, required=True)
    p.add_argument("--safe",  action="store_true")
    a = p.parse_args()
    # ★ 여기서부터/여기까지가 전체 시간 측정
    start_time = time.perf_counter()
    main(a.start, a.end, a.safe)
    elapsed = time.perf_counter() - start_time
    print(f"\n⏱ 전체 실행 시간: {elapsed:.2f} 초")



