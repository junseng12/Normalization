import os, time, glob
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()
RPC = os.getenv("RPC_URL")
SLEEP_MS = int(os.getenv("SLEEP_MS", "350"))

w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 30}))
assert w3.is_connected(), "RPC 연결 실패"

# 1) transactions_* 병합 (또는 선택본이 있으면 그걸 사용)
tx_files = sorted(glob.glob("out/transactions_*.csv"))
txfile = "out/transactions_selected.csv" if os.path.exists("out/transactions_selected.csv") else None
if txfile:
    tx = pd.read_csv(txfile, usecols=["hash", "to_address"])
else:
    if not tx_files:
        raise SystemExit("out/transactions_*.csv 가 없습니다. 4단계를 먼저 실행하세요.")
    tx = pd.concat([pd.read_csv(f, usecols=["hash", "to_address"]) for f in tx_files], ignore_index=True)

# 2) 우리가 선별한 TX만 남기기
if os.path.exists("out/tx_hashes.txt"):
    sel = set(x.strip() for x in open("out/tx_hashes.txt").read().splitlines() if x.strip())
    tx = tx[tx["hash"].isin(sel)]

# 3) 주소 정리: None/빈값 제거 + 길이/형식 검증
def is_hex_addr(s: str) -> bool:
    if not isinstance(s, str): return False
    s = s.strip()
    return s.startswith("0x") and len(s) == 42

cand = sorted(set(a for a in tx["to_address"].dropna().tolist() if is_hex_addr(a)))

# 4) 체크섬 변환 + getCode
rows = []
for i, a in enumerate(cand, 1):
    try:
        # checksum 변환 (소문자/대문자 무관)
        cs = Web3.to_checksum_address(a)
    except Exception:
        print(f"[skip] invalid address format: {a}")
        continue

    # 컨트랙트 코드 유무
    try:
        code = w3.eth.get_code(cs)
        is_contract = (code not in (b"", b"0x"))
    except Exception as e:
        print(f"[warn] get_code 실패({cs}): {e}; false로 표기")
        is_contract = False

    rows.append({
        "address": cs,                 # 체크섬 보존
        "address_lower": cs.lower(),   # 조인 편의용
        "to_is_contract": is_contract
    })

    if i % 5 == 0:
        time.sleep(SLEEP_MS / 1000.0)  # 무료 플랜: 천천히

df = pd.DataFrame(rows).drop_duplicates(subset=["address_lower"])
os.makedirs("out", exist_ok=True)
df.to_csv("out/contract_flags.csv", index=False)
print("contract flags written:", len(df))
