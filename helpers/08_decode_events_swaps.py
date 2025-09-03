# helpers/08_decode_events_swaps.py  (schema-robust v2)
import os, glob, json, time, ast, math
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3
from eth_abi import decode as abi_decode

load_dotenv()
RPC = os.getenv("RPC_URL")
SLEEP_MS = int(os.getenv("SLEEP_MS","350"))
w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 30}))
assert w3.is_connected(), "RPC 연결 실패"

# 입력 로그 파일
LOG_FILES = sorted(glob.glob("out/chunks/logs_*.csv"))
if not LOG_FILES and os.path.exists("out/logs.csv"):
    LOG_FILES = ["out/logs.csv"]
if not LOG_FILES:
    raise SystemExit("logs 파일이 없습니다. ③단계를 먼저 실행하세요.")

# 토픽/캐시
topics = json.load(open("config/topics.json"))
TOPIC_V2 = topics["univ2_swap"].lower()
TOPIC_V3 = topics["univ3_swap"].lower()

TOK_CACHE = "out/pool_tokens.csv"
META_CACHE = "out/token_meta.csv"
os.makedirs("out", exist_ok=True)

# 캐시 적재
pool = {}
if os.path.exists(TOK_CACHE):
    for _, r in pd.read_csv(TOK_CACHE).iterrows():
        pool[str(r["pool"]).lower()] = (r["token0"], r["token1"])

meta = {}
if os.path.exists(META_CACHE):
    for _, r in pd.read_csv(META_CACHE).iterrows():
        meta[str(r["address"]).lower()] = (r["symbol"], int(r["decimals"]))

def token_meta(addr_cs):
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
        META_CACHE, mode="a", header=not os.path.exists(META_CACHE), index=False
    )
    time.sleep(SLEEP_MS/1000.0)
    return meta[a]

def tokens_of_pool(addr_cs):
    a = addr_cs.lower()
    if a in pool: return pool[a]
    abi = [
      {"name":"token0","outputs":[{"type":"address"}],"inputs":[],"stateMutability":"view","type":"function"},
      {"name":"token1","outputs":[{"type":"address"}],"inputs":[],"stateMutability":"view","type":"function"}
    ]
    c = w3.eth.contract(address=addr_cs, abi=abi)
    try:
        t0 = c.functions.token0().call()
        t1 = c.functions.token1().call()
    except Exception:
        t0 = t1 = "0x0000000000000000000000000000000000000000"
    t0, t1 = Web3.to_checksum_address(t0), Web3.to_checksum_address(t1)
    pool[a] = (t0, t1)
    pd.DataFrame([{"pool": addr_cs, "token0": t0, "token1": t1}]).to_csv(
        TOK_CACHE, mode="a", header=not os.path.exists(TOK_CACHE), index=False
    )
    time.sleep(SLEEP_MS/1000.0)
    return pool[a]

def normalize_topics(val, row=None):
    # 07과 동일: topics → ['0x..','0x..',...] 로 표준화
    if val is not None and not (isinstance(val, float) and math.isnan(val)):
        if isinstance(val, (list, tuple)):
            arr = list(val)
        else:
            s = str(val).strip()
            if s in ("", "NaN", "nan", "None", "null"): arr = []
            elif (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
                try: arr = json.loads(s)
                except Exception:
                    try: arr = ast.literal_eval(s)
                    except Exception: arr = []
            elif ("," in s) and ("0x" in s):
                arr = [p.strip() for p in s.split(",") if p.strip()]
            else:
                arr = [s]
        norm=[]
        for x in arr:
            if isinstance(x, bytes): norm.append("0x"+x.hex())
            elif isinstance(x, int): norm.append(hex(x))
            else: norm.append(str(x).strip())
        if norm: return norm

    if row is not None:
        norm=[]
        for k in ("topic0","topic1","topic2","topic3"):
            if k in row.index and pd.notna(row[k]): norm.append(str(row[k]).strip())
        return norm
    return []

def get_hex_data(s):
    if not isinstance(s, str): return "0x"
    s=s.strip()
    if s.startswith("0x"): return s
    if s=="": return "0x"
    return "0x"+s

rows=[]
file_counts={"v2":0,"v3":0}
for f in LOG_FILES:
    logs = pd.read_csv(
        f,
        dtype={"topics":"string","data":"string","address":"string","transaction_hash":"string"},
        low_memory=False
    )
    # 결측 보강
    for c in ["transaction_hash","log_index","address","data"]:
        if c not in logs.columns:
            logs[c] = "" if c != "log_index" else -1

    matched=0
    for _, r in logs.iterrows():
        tps = normalize_topics(r.get("topics", None), r)
        if not tps: continue
        topic0 = str(tps[0]).lower()
        if topic0 not in (TOPIC_V2, TOPIC_V3):  # 스왑이 아니면 패스
            continue

        try:
            pool_addr = Web3.to_checksum_address(str(r["address"]))
        except Exception:
            continue

        data_hex = get_hex_data(r["data"])[2:]
        if topic0 == TOPIC_V2:
            try:
                a0i,a1i,a0o,a1o = abi_decode(
                    ["uint256","uint256","uint256","uint256"],
                    bytes.fromhex(data_hex)
                )
            except Exception:
                continue
            t0,t1 = tokens_of_pool(pool_addr)
            s0,d0 = token_meta(t0); s1,d1 = token_meta(t1)
            def n(v,d): return float(v)/(10**d)
            if a0i>0:
                token_in, amt_in  = f"{s0}.ETH", n(a0i,d0)
                token_out, amt_out= f"{s1}.ETH", n(a1o,d1)
            else:
                token_in, amt_in  = f"{s1}.ETH", n(a1i,d1)
                token_out, amt_out= f"{s0}.ETH", n(a0o,d0)
            rows.append({
                "tx_hash": str(r["transaction_hash"]),
                "log_index": int(r["log_index"]) if pd.notna(r["log_index"]) else -1,
                "dex": "UNI-V2",
                "pair_or_pool": pool_addr,
                "token_in": token_in, "token_out": token_out,
                "amount_in": amt_in, "amount_out": amt_out
            })
            file_counts["v2"]+=1; matched+=1
        else:
            try:
                a0,a1,_,_,_ = abi_decode(
                    ["int256","int256","uint160","uint128","int24"],
                    bytes.fromhex(data_hex)
                )
            except Exception:
                continue
            t0,t1 = tokens_of_pool(pool_addr)
            s0,d0 = token_meta(t0); s1,d1 = token_meta(t1)
            if a0 < 0:
                token_in, amt_in  = f"{s0}.ETH", float(-a0)/(10**d0)
                token_out, amt_out= f"{s1}.ETH", float(a1)/(10**d1)
            else:
                token_in, amt_in  = f"{s1}.ETH", float(-a1)/(10**d1)
                token_out, amt_out= f"{s0}.ETH", float(a0)/(10**d0)
            rows.append({
                "tx_hash": str(r["transaction_hash"]),
                "log_index": int(r["log_index"]) if pd.notna(r["log_index"]) else -1,
                "dex": "UNI-V3",
                "pair_or_pool": pool_addr,
                "token_in": token_in, "token_out": token_out,
                "amount_in": amt_in, "amount_out": amt_out
            })
            file_counts["v3"]+=1; matched+=1
    print(f"[{os.path.basename(f)}] matched swaps: {matched}")

# 안전 출력(빈 결과여도 헤더 생성)
cols = ["tx_hash","log_index","dex","pair_or_pool","token_in","token_out","amount_in","amount_out"]
out = pd.DataFrame(rows, columns=cols)
if not out.empty:
    out = out.sort_values(["tx_hash","log_index"]).reset_index(drop=True)
out.to_csv("out/dex_swaps.csv", index=False)
print("dex_swaps:", len(out), "rows -> out/dex_swaps.csv")
print("  breakdown:", file_counts)
