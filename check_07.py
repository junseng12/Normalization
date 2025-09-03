import glob,pandas as pd, os, itertools as it
files = sorted(glob.glob("out/chunks/logs_*.csv")) or ["out/logs.csv"]
f = files[0]; print("file:", f)
df = pd.read_csv(f, nrows=10)
cols = df.columns.tolist(); print("cols:", cols)
if "topics" in cols:
    print("topics samples:", [str(x) for x in df["topics"].head(5).tolist()])
