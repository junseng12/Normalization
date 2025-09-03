# helpers/07_decode_events_transfers.py  (schema-robust v3)
import os, glob, time, json, ast, math
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()
RPC = os.getenv("RPC_URL")
SLEEP_MS = int(os.getenv("SLEEP_MS","350"))
w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 30}))
assert w3.is_connected(), "RPC 연결 실패"

# ── 입력 로그 파일 선택 ──────────────────────────────────────────
LOG_FILES = sorted(glob.glob("out/chunks/logs_*.csv"))
if not LOG_FILES and os.path.exists("out/logs.csv"):
    LOG_FILES = ["out/logs.csv"]
if not LOG_FILES:
    raise SystemExit("logs 파일을 찾지 못했습니다. ③단계(export_receipts_and_logs)를 먼저 실행하세요.")

# ── 설정/캐시 경로 ───────────────────────────────────────────────
TOPIC_TRANSFER = json.load(open("config/topics.json"))["erc20_transfer"]
META_PATH = "out/token_meta.csv"
os.makedirs("out", exist_ok=True)

# ── 토큰 메타 캐시 ───────────────────────────────────────────────
meta = {}
if os.path.exists(META_PATH):
    dfm = pd.read_csv(META_PATH)
    for _, r in dfm.iterrows():
        meta[str(r["address"]).lower()] = (r["symbol"], int(r["decimals"]))

def topic_to_addr(topic_hex: str) -> str:
    if not isinstance(topic_hex, str):
        return ""
    s = topic_hex.strip()
    if not s.startswith("0x") or len(s) < 42:
        return ""
    return Web3.to_checksum_address("0x" + s[-40:])

def get_token_meta(addr_cs):
    a = addr_cs.lower()
    if a in meta: return meta[a]
    abi = [
      {"name":"symbol","outputs":[{"type":"string"}],"inputs":[],"stateMutability":"view","type":"function"},
      {"name":"decimals","outputs":[{"type":"uint8"}],"inputs":[],"stateMutability":"view","type":"function"}
    ]
    c = w3.eth.contract(address=addr_cs, abi=abi)
    try: sym = c.functions.symbol().call()
    except Exception: sym = "UNK"
    try: dec = c.functions.decimals().call()
    except Exception: dec = 18
    meta[a] = (sym, int(dec))
    pd.DataFrame([{"address": addr_cs, "symbol": sym, "decimals": int(dec)}]).to_csv(
        META_PATH, mode="a", header=not os.path.exists(META_PATH), index=False
    )
    time.sleep(SLEEP_MS/1000.0)
    return meta[a]

def normalize_topics(val, row=None):
    """
    어떤 형태든 topics를 ['0x..','0x..',...] 리스트로 반환.
    우선순위: topics 칼럼 -> topic0..3 칼럼

    지원 형태:
      - JSON/리터럴 리스트: ["0x..","0x.."] / ('0x..',...)
      - 콤마-구분 문자열: "0x..,0x..,0x.."   ← Ethereum-ETL logs.csv 표준이 'string'이라 이 케이스가 많음
      - 단일 문자열/정수/bytes
      - fallback: topic0..topic3 칼럼
    """
    # 1) topics 칼럼 값 사용
    if val is not None and not (isinstance(val, float) and (math.isnan(val))):
        if isinstance(val, (list, tuple)):
            arr = list(val)
        else:
            s = str(val).strip()
            if s == "" or s.lower() in ("nan","none","null"):
                arr = []
            elif (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
                # JSON/리터럴
                try:
                    arr = json.loads(s)
                except Exception:
                    try:
                        arr = ast.literal_eval(s)
                    except Exception:
                        arr = []
            elif ("," in s) and ("0x" in s):
                # ★ 핵심 추가: 콤마-구분 문자열 → split
                arr = [p.strip() for p in s.split(",") if p.strip()]
            else:
                # 단일 값(문자열/정수 등)
                arr = [s]
        # 요소 정규화: int→hex, bytes→0xhex, str은 그대로
        norm = []
        for x in arr:
            if isinstance(x, bytes):
                norm.append("0x"+x.hex())
            elif isinstance(x, int):
                norm.append(hex(x))
            elif isinstance(x, str):
                norm.append(x.strip())
        if norm:
            return norm

    # 2) topic0..3 폴백
    if row is not None:
        norm = []
        for k in ("topic0","topic1","topic2","topic3"):
            if k in row.index and pd.notna(row[k]):
                norm.append(str(row[k]).strip())
        return norm
    return []

rows = []
for f in LOG_FILES:
    # dtype 강제: topics/data/address/tx 해시를 문자열로 고정
    logs = pd.read_csv(
        f,
        dtype={"topics":"string", "data":"string", "address":"string", "transaction_hash":"string"},
        low_memory=False
    )
    for c in ["transaction_hash","log_index","address","data"]:
        if c not in logs.columns:
            logs[c] = "" if c != "log_index" else -1

    matched = 0
    for _, r in logs.iterrows():
        topics = normalize_topics(r.get("topics", None), r)
        if not topics:
            continue
        if str(topics[0]).lower() != TOPIC_TRANSFER.lower():
            continue

        # 주소/데이터 안전 처리
        try:
            token_cs = Web3.to_checksum_address(str(r["address"]))
        except Exception:
            continue

        from_addr = topic_to_addr(topics[1]) if len(topics) > 1 else ""
        to_addr   = topic_to_addr(topics[2]) if len(topics) > 2 else ""

        data_hex  = str(r["data"]) if isinstance(r["data"], str) else "0x"
        if not data_hex.startswith("0x"):
            data_hex = "0x" + data_hex
        try:
            amount_raw = int(data_hex, 16)
        except Exception:
            amount_raw = 0

        sym, dec = get_token_meta(token_cs)
        amount_norm = float(amount_raw) / (10**dec) if dec >= 0 else None

        rows.append({
            "tx_hash": str(r["transaction_hash"]),
            "log_index": int(r["log_index"]) if pd.notna(r["log_index"]) else -1,
            "token_address": token_cs,
            "symbol": sym,
            "decimals": dec,
            "from": from_addr.lower(),
            "to": to_addr.lower(),
            "amount_raw": amount_raw,
            "amount_norm": amount_norm,
            "token_alias": f"{sym}.ETH"
        })
        matched += 1
    print(f"[{os.path.basename(f)}] matched transfers: {matched}")

# 안전 출력(빈 결과도 헤더만 생성)
cols = ["tx_hash","log_index","token_address","symbol","decimals",
        "from","to","amount_raw","amount_norm","token_alias"]
out = pd.DataFrame(rows, columns=cols)
if not out.empty:
    out = out.sort_values(["tx_hash","log_index"]).reset_index(drop=True)
out.to_csv("out/transfers.csv", index=False)
print("transfers:", len(out), "rows -> out/transfers.csv")
