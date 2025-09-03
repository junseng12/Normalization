# helpers/06_build_normalized.py  (robust to optional/missing columns)
import os, glob, pandas as pd

def read_concat_csv(pattern, use=None, rename=None, drop_dupe_on=None):
    """각 파일 헤더를 보고 교집합만 읽고, 누락 컬럼은 이후 보강."""
    frames = []
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"No files matched: {pattern}")
    for f in files:
        cols = pd.read_csv(f, nrows=0).columns.str.strip().tolist()
        take = [c for c in (use or cols) if c in cols]
        df = pd.read_csv(f, usecols=take)
        if rename:
            df.rename(columns=rename, inplace=True)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    if drop_dupe_on:
        out = out.drop_duplicates(subset=drop_dupe_on)
    return out

# 1) blocks (우리가 쓰는 최소 컬럼만)
blocks = read_concat_csv(
    "out/blocks_*.csv",
    use=["number","hash","timestamp"],
    rename={"number":"block_number","hash":"block_hash","timestamp":"ts_utc"},
    drop_dupe_on=["block_number"]
)

# 2) transactions (옵션 컬럼 존재 여부에 대응)
#    최소 필수: hash, block_number, from_address, to_address, value, input
tx_use_min = ["hash","block_number","from_address","to_address","value","input"]
tx_opt     = ["type"]  # 있으면 읽고, 없으면 나중에 생성
# 헤더를 한 번 보고 opt 중 실제로 있는 것만 추가
any_tx = sorted(glob.glob("out/transactions_*.csv"))[:1]
if not any_tx:
    raise SystemExit("No transactions files. Run 04_export_blocks_txs.py first.")
tx_cols0 = pd.read_csv(any_tx[0], nrows=0).columns.str.strip().tolist()
tx_use = tx_use_min + [c for c in tx_opt if c in tx_cols0]

txs = read_concat_csv(
    "out/transactions_*.csv",
    use=tx_use,
    rename={"hash":"tx_hash","from_address":"from","to_address":"to","value":"value_wei"},
    drop_dupe_on=["tx_hash"]
)

# 빠진 컬럼 보강
for c in tx_opt:
    if c not in txs.columns:
        txs[c] = pd.NA

# 3) receipts (effective_gas_price 없을 수도 있음)
rcpt_use_min = ["transaction_hash","status","gas_used"]
rcpt_opt     = ["effective_gas_price"]
any_r = sorted(glob.glob("out/chunks/receipts_*.csv"))[:1]
if not any_r:
    raise SystemExit("No receipts files. Run 03_export_receipts_and_logs.py first.")
r_cols0 = pd.read_csv(any_r[0], nrows=0).columns.str.strip().tolist()
rcpt_use = rcpt_use_min + [c for c in rcpt_opt if c in r_cols0]

rcpt = read_concat_csv(
    "out/chunks/receipts_*.csv",
    use=rcpt_use,
    rename={"transaction_hash":"tx_hash"},
    drop_dupe_on=["tx_hash"]
)
if "effective_gas_price" not in rcpt.columns:
    rcpt["effective_gas_price"] = pd.NA

# 4) 선별 TX로 제한 (필수는 아니지만 용량 감소)
if os.path.exists("out/tx_hashes.txt"):
    sel = set(open("out/tx_hashes.txt").read().splitlines())
    txs = txs[txs["tx_hash"].isin(sel)]

# 5) 조인 & 파생
df = txs.merge(blocks, on="block_number", how="left") \
        .merge(rcpt, on="tx_hash",      how="left")

# 수치 파생
df["gas_used"] = pd.to_numeric(df["gas_used"], errors="coerce")
df["effective_gas_price"] = pd.to_numeric(df["effective_gas_price"], errors="coerce")
df["gas_fee_eth"] = (df["gas_used"].fillna(0) * df["effective_gas_price"].fillna(0)) / 1e18
df["input"] = df["input"].fillna("0x")
df["input_selector"] = df["input"].str.slice(0,10)

# 6) to_is_contract 조인 (lower 키로 안전하게)
if os.path.exists("out/contract_flags.csv"):
    flags = pd.read_csv("out/contract_flags.csv")  # address, address_lower, to_is_contract
    # 주소가 없는 경우 대비
    df["to_lower"] = df["to"].fillna("").str.lower()
    flags = flags.drop_duplicates(subset=["address_lower"])
    df = df.merge(flags[["address_lower","to_is_contract"]],
                  left_on="to_lower", right_on="address_lower", how="left") \
           .drop(columns=["address_lower"])
else:
    df["to_is_contract"] = pd.NA

# 7) 출력 스키마(우리 표)
cols = [
    "block_number","block_hash","ts_utc",
    "tx_hash","from","to","type",
    "status","value_wei",
    "gas_used","effective_gas_price","gas_fee_eth",
    "input","input_selector","to_is_contract"
]
# 일부 파일에 'type'이 없던 케이스 보강
for c in cols:
    if c not in df.columns:
        df[c] = pd.NA

os.makedirs("out", exist_ok=True)
df[cols].to_csv("out/normalized.csv", index=False)
print("normalized rows:", len(df))
