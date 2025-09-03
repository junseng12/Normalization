import pandas as pd, glob
files=sorted(glob.glob("out/chunks/logs_*.csv"))[:2]
for f in files:
    df=pd.read_csv(f,usecols=[c for c in ["topic0","topics"] if c in pd.read_csv(f,nrows=0).columns])
    print(f, "rows:", len(df))
    if "topic0" in df: print(" topic0 sample:", df["topic0"].head(3).tolist())
    if "topics" in df: print(" topics sample:", df["topics"].head(3).tolist())