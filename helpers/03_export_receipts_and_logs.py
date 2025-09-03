import os, subprocess, time, glob
from dotenv import load_dotenv

load_dotenv()
RPC_URL   = os.getenv("RPC_URL")
SLEEP_MS  = int(os.getenv("SLEEP_MS","350"))

chunk_files = sorted(glob.glob("out/chunks/tx_hashes_*.txt"))
for path in chunk_files:
    r_out = f"{path.replace('tx_hashes_','receipts_').replace('.txt','.csv')}"
    l_out = f"{path.replace('tx_hashes_','logs_').replace('.txt','.csv')}"
    cmd = [
      "ethereumetl","export_receipts_and_logs",
      "--transaction-hashes", path,
      "--provider-uri", RPC_URL,
      "--receipts-output", r_out,
      "--logs-output", l_out
    ]
    print(">>", " ".join(cmd))
    subprocess.run(cmd, check=True)
    time.sleep(SLEEP_MS/1000)
print("done receipts/logs")
