import os, sys, json, time
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()
RPC = os.getenv("RPC_URL")
SAFE_LAG = int(os.getenv("SAFE_LAG", "12"))
SLEEP_MS = int(os.getenv("SLEEP_MS", "350"))

w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 60}))
assert w3.is_connected(), "RPC 연결 실패"

def safe_end(end):
    latest = w3.eth.block_number
    return min(end, max(0, latest - SAFE_LAG))

def get_logs(params, max_retry=5):
    for i in range(max_retry):
        try:
            return w3.eth.get_logs(params)
        except Exception as e:
            msg = str(e).lower()
            # 과다응답/제한 → step 줄여 재시도하도록 상위 루프에서 처리
            if "too many results" in msg or "query returned more than" in msg:
                raise
            time.sleep((i+1) * 0.8)
    raise

def scan_range(start, end, topics, addresses=None, step=800):
    """ 작은 step으로 순차 스캔; 과다응답이면 step 반으로 """
    txs = set()
    cur = start
    end = safe_end(end)
    while cur <= end:
        to_ = min(cur + step - 1, end)
        params = {"fromBlock": cur, "toBlock": to_}
        if addresses: params["address"] = addresses
        if topics:    params["topics"]  = [topics] if len(topics)==1 else [topics]

        try:
            logs = get_logs(params)
            for lg in logs:
                txs.add(lg["transactionHash"].hex())
            print(f"[ok] {cur}-{to_} logs={len(logs)} acc_tx={len(txs)}")
            cur = to_ + 1
            time.sleep(SLEEP_MS/1000)
        except Exception as e:
            if step <= 20:
                print(f"[skip] step={step} at {cur}-{to_} err={e}")
                cur = to_ + 1
            else:
                step //= 2
                print(f"[split] reduce step to {step} due to: {e}")
    return sorted(txs)

def main():
    if len(sys.argv) < 3:
        print("Usage: python 01_collect_txhashes.py <start_block> <end_block> [--safe]")
        return
    start, end = int(sys.argv[1]), int(sys.argv[2])
    if "--safe" in sys.argv: end = safe_end(end)

    topics = json.load(open("config/topics.json"))
    addrs  = json.load(open("config/addresses.json"))

    os.makedirs("out", exist_ok=True)
    tx_all = []

    # A) ERC20 Transfer (작은 step)
    tx_all += scan_range(start, end, [topics["erc20_transfer"]], step=800)

    # B) DEX/브리지 주소+토픽 (주소가 있으면 정확도↑)
    for key, tkey in [("dex_pools_or_pairs","univ2_swap"),
                      ("dex_pools_or_pairs","univ3_swap"),
                      ("bridges","wormhole_log")]:
        add_list = addrs.get(key, [])
        if not add_list: continue
        tx_all += scan_range(start, end, [topics[tkey]], addresses=add_list, step=1000)

    txs = sorted(set(tx_all))
    open("out/tx_hashes.txt", "w").write("\n".join(txs))
    open("out/blocks_range.txt","w").write(f"{start},{end}\n")
    print("saved tx_hashes:", len(txs))

if __name__ == "__main__":
    main()
