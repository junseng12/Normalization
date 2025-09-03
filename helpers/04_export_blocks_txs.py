import os, time, subprocess
from dotenv import load_dotenv

load_dotenv()
RPC = os.getenv("RPC_URL")
SLEEP_MS = int(os.getenv("SLEEP_MS","350"))

start, end = map(int, open("out/blocks_range.txt").read().split(","))
STEP = 1000        # 더 작게
MAX_RETRY = 4      # 청크별 재시도 횟수

def run_chunk(s, e):
    blk_out = f"out/blocks_{s}_{e}.csv"
    tx_out  = f"out/transactions_{s}_{e}.csv"
    cmd = [
        "ethereumetl","export_blocks_and_transactions",
        "--start-block", str(s), "--end-block", str(e),
        "--provider-uri", RPC,
        "--blocks-output", blk_out,
        "--transactions-output", tx_out,
        "--max-workers", "1",
        "--batch-size", "10"
    ]
    for attempt in range(1, MAX_RETRY+1):
        try:
            print(f">> [{s}-{e}] attempt {attempt}: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
            time.sleep(SLEEP_MS/1000)
            return True
        except subprocess.CalledProcessError as exc:
            print(f"[warn] chunk {s}-{e} failed (attempt {attempt}): {exc}")
            time.sleep((SLEEP_MS/1000) * attempt)
    return False

def bisect_and_run(s, e):
    # 이진 분할로 더 쪼개서 시도
    if s >= e:
        return run_chunk(s, e)
    mid = (s + e) // 2
    ok1 = run_chunk(s, mid)
    ok2 = run_chunk(mid+1, e)
    if not ok1:
        if s < mid:
            bisect_and_run(s, mid)
    if not ok2:
        if mid+1 < e:
            bisect_and_run(mid+1, e)

for s in range(start, end+1, STEP):
    e = min(s+STEP-1, end)
    ok = run_chunk(s, e)
    if not ok:
        print(f"[split] bisecting {s}-{e}")
        bisect_and_run(s, e)

print("done blocks/transactions")
