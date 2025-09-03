import os, glob, json, ast, math
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
# ── 설정 로드 ──────────────────────────────────────
topics_cfg = json.load(open("config/topics.json"))
TOPIC_WORM = topics_cfg.get("wormhole_log")
if not TOPIC_WORM:
    raise SystemExit("config/topics.json 에 'wormhole_log' 시그니처가 없습니다.")

# (선택) 브리지 컨트랙트 주소 리스트: token_in 추정에 사용
addresses_cfg = {}
if os.path.exists("config/addresses.json"):
    addresses_cfg = json.load(open("config/addresses.json"))
BRIDGE_ADDRS = {a.lower() for a in addresses_cfg.get("bridges", [])}

# ── 입력 로그 파일 선택 ───────────────────────────
LOG_FILES = sorted(glob.glob("out/chunks/logs_*.csv"))
if not LOG_FILES and os.path.exists("out/logs.csv"):
    LOG_FILES = ["out/logs.csv"]
if not LOG_FILES:
    raise SystemExit("logs 파일을 찾지 못했습니다. ③단계(export_receipts_and_logs)를 먼저 실행하세요.")

# (있으면 transfers로 amount/token 추정)
transfers = pd.read_csv("out/transfers.csv") if os.path.exists("out/transfers.csv") else pd.DataFrame(
    columns=["tx_hash","from","to","token_alias","amount_norm"]
)

# ── 유틸: topics 정규화 ───────────────────────────
def normalize_topics(val, row=None):
    """
    ['0x..','0x..', ...] 형태로 반환.
    우선순위: topics 칼럼 → topic0..topic3 칼럼
    """
    if val is not None and not (isinstance(val, float) and math.isnan(val)):
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
                # 콤마-문자열
                arr = [p.strip() for p in s.split(",") if p.strip()]
            else:
                arr = [s]
        norm = []
        for x in arr:
            if isinstance(x, bytes): norm.append("0x"+x.hex())
            elif isinstance(x, int): norm.append(hex(x))
            else: norm.append(str(x).strip())
        if norm:
            return norm

    if row is not None:
        norm = []
        for k in ("topic0","topic1","topic2","topic3"):
            if k in row.index and pd.notna(row[k]):
                norm.append(str(row[k]).strip())
        return norm
    return []

# ── 본 처리 ───────────────────────────────────────
rows = []
for path in LOG_FILES:
    logs = pd.read_csv(
        path,
        dtype={"topics":"string","data":"string","address":"string","transaction_hash":"string"},
        low_memory=False
    )
    # 결측 컬럼 보강
    for c in ["transaction_hash","log_index","address","data"]:
        if c not in logs.columns:
            logs[c] = "" if c != "log_index" else -1

    matched = 0
    for _, r in logs.iterrows():
        tps = normalize_topics(r.get("topics", None), r)
        if not tps: 
            continue
        if str(tps[0]).lower() != TOPIC_WORM.lower():
            continue

        txh = str(r["transaction_hash"])
        li  = int(r["log_index"]) if pd.notna(r["log_index"]) else -1

        # (선택) transfers에서 같은 tx 내 브리지 수신을 찾아 token_in/amount_in 추정
        token_in = None; amount_in = None
        if not transfers.empty and BRIDGE_ADDRS:
            tsub = transfers[(transfers["tx_hash"] == txh) & (transfers["to"].str.lower().isin(BRIDGE_ADDRS))]
            if not tsub.empty:
                hit = tsub.iloc[0]
                token_in  = hit.get("token_alias", None)
                amount_in = hit.get("amount_norm", None)

        # fields: 빠르게 sequence/nonce만 추출하고, 나머지는 raw data 저장(정교 디코딩은 후속 단계에서)
        data_hex = str(r["data"])[2:] if isinstance(r["data"], str) and r["data"].startswith("0x") else ""
        fields = {"raw_data_prefix": data_hex[:128]}  # 안전하게 앞부분만 남김(옵션)

        rows.append({
            "tx_hash": txh,
            "log_index": li,
            "bridge_id": "wormhole",
            "fields": json.dumps(fields, ensure_ascii=False),
            "token_in": token_in,
            "amount_in": amount_in
        })
        matched += 1

    print(f"[{os.path.basename(path)}] matched wormhole: {matched}")

# ── 안전 출력(빈 결과도 헤더 유지) ─────────────────
cols = ["tx_hash","log_index","bridge_id","fields","token_in","amount_in"]
df = pd.DataFrame(rows, columns=cols)
if not df.empty:
    df = df.sort_values(["tx_hash","log_index"]).reset_index(drop=True)
os.makedirs("out", exist_ok=True)
df.to_csv("out/bridge_events.csv", index=False)
print("bridge_events:", len(df), "rows -> out/bridge_events.csv")
