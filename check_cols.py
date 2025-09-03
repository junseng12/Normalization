# check_cols.py
import glob, pandas as pd
files = sorted(glob.glob("out/transactions_*.csv"))
if not files:
    raise SystemExit("out/transactions_*.csv 파일이 없습니다. 4단계(블록/트랜잭션 추출)를 먼저 실행하세요.")
f = files[0]
print("Sample file:", f)
print(pd.read_csv(f, nrows=0).columns.tolist())
