import os
from dotenv import load_dotenv
load_dotenv()
BATCH_SIZE = int(os.getenv("BATCH_SIZE","10"))

os.makedirs("out/chunks", exist_ok=True)
txs = [x.strip() for x in open("out/tx_hashes.txt").read().splitlines() if x.strip()]
for i in range(0, len(txs), BATCH_SIZE):
    part = txs[i:i+BATCH_SIZE]
    idx = i//BATCH_SIZE + 1
    with open(f"out/chunks/tx_hashes_{idx:04d}.txt","w") as f:
        f.write("\n".join(part))
print("chunks:", (len(txs)+BATCH_SIZE-1)//BATCH_SIZE)
