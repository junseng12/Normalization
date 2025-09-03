import os, json, time
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3
from eth_abi import decode as abi_decode
import pandas as pd

load_dotenv()
RPC = os.getenv("RPC_URL")
w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 60}))
assert w3.is_connected()

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "out"; OUT.mkdir(exist_ok=True)

# ---- config ----
topics = json.load(open(ROOT/"config/topics.json"))
TOPIC_TRANSFER = topics["erc20_transfer"].lower()
TOPIC_V2       = topics["univ2_swap"].lower()
TOPIC_V3       = topics["univ3_swap"].lower()
SAFE_LAG = int(os.getenv("SAFE_LAG","12"))
BATCH = int(os.getenv("BATCH_SIZE","50"))   # 유료면 100~200까지 가능
SLEEP = int(os.getenv("SLEEP_MS","100"))

# ---- helpers ----
def topic_to_addr(t):
    s = str(t); 
    if not s.startswith("0x") or len(s)<42: return None
    return Web3.to_checksum_address("0x"+s[-40:])

def get_logs_range(fb, tb, topics0_list, addresses=None):
    params = {"fromBlock": fb, "toBlock": tb, "topics":[topics0_list] if isinstance(topics0_list, str) else [topics0_list]}
    if addresses: params["address"] = addresses
    return w3.eth.get_logs(params)

def batch_receipts(tx_hashes):
    out = []
    for i in range(0, len(tx_hashes), BATCH):
        chunk = tx_hashes[i:i+BATCH]
        with w3.batch_requests() as b:  # web3.py 공식
            calls = [b.add(w3.eth.get_transaction_receipt(h)) for h in chunk]
        out.extend([c for c in calls if c is not None])
        time.sleep(SLEEP/1000)
    return out

# ---- 1) 후보 tx 해시 수집 (Transfer + V2/V3 Swap) ----
def collect_tx_hashes(start, end):
    end = min(end, w3.eth.block_number - SAFE_LAG)
    txs=set()
    step=4000
    cur=start
    while cur<=end:
        to_ = min(cur+step-1, end)
        try:
            # ERC20 Transfer
            for lg in get_logs_range(cur, to_, TOPIC_TRANSFER):
                txs.add(lg["transactionHash"].hex())
            # (원한다면) DEX 풀/페어 주소 넣고 Swap 토픽으로 더 좁혀도 됨
            # for lg in get_logs_range(cur, to_, [TOPIC_V2, TOPIC_V3]): txs.add(lg["transactionHash"].hex())
            cur = to_+1
        except Exception:
            step = max(100, step//2);  # 과다응답 분할
    return list(txs)

# ---- 2) receipts in-memory + normalize + decode events ----
def run_fast(start, end):
    tx_hashes = collect_tx_hashes(start, end)
    rcpts = batch_receipts(tx_hashes)

    # 블록 타임스탬프 캐시
    blk_ts = {}
    def ts_of(bn):
        if bn in blk_ts: return blk_ts[bn]
        blk = w3.eth.get_block(bn, False)
        blk_ts[bn] = blk["timestamp"]; 
        return blk_ts[bn]

    # 캐시: token meta / pool tokens
    meta = {}; pools={}
    def token_meta(addr):
        a=addr.lower()
        if a in meta: return meta[a]
        abi=[{"name":"symbol","outputs":[{"type":"string"}],"inputs":[],"stateMutability":"view","type":"function"},
             {"name":"decimals","outputs":[{"type":"uint8"}],"inputs":[],"stateMutability":"view","type":"function"}]
        c=w3.eth.contract(address=addr, abi=abi)
        try: sym=c.functions.symbol().call()
        except: sym="UNK"
        try: dec=c.functions.decimals().call()
        except: dec=18
        meta[a]=(sym,int(dec)); return meta[a]

    def pool_tokens(addr):
        a=addr.lower()
        if a in pools: return pools[a]
        abi=[{"name":"token0","outputs":[{"type":"address"}],"inputs":[],"stateMutability":"view","type":"function"},
             {"name":"token1","outputs":[{"type":"address"}],"inputs":[],"stateMutability":"view","type":"function"}]
        c=w3.eth.contract(address=addr, abi=abi)
        try: t0=c.functions.token0().call(); t1=c.functions.token1().call()
        except: t0=t1="0x0000000000000000000000000000000000000000"
        pools[a]=(Web3.to_checksum_address(t0), Web3.to_checksum_address(t1))
        return pools[a]

    # 결과 버퍼
    norm, transfers, swaps, bridges = [], [], [], []

    # tx 본문은 receipt 안에 없음 → from/to/value/input은 tx 조회 필요
    # 효율 위해 unique block/tx 최소 조회
    tx_cache = {}
    def get_tx(txh):
        if txh in tx_cache: return tx_cache[txh]
        tx = w3.eth.get_transaction(txh)
        tx_cache[txh] = tx
        return tx

    for rc in rcpts:
        txh = rc["transactionHash"].hex()
        tx  = get_tx(txh)
        ts  = ts_of(tx["blockNumber"])
        eff = rc.get("effectiveGasPrice", tx.get("gasPrice", 0)) or 0
        fee = (rc["gasUsed"]*eff)/1e18
        # to_is_contract
        code = w3.eth.get_code(tx["to"]) if tx["to"] else b""
        to_is_contract = (code not in (b"", b"0x"))

        norm.append({
            "block_number": tx["blockNumber"],
            "block_hash": tx["blockHash"].hex(),
            "ts_utc": ts,
            "tx_hash": txh,
            "from": tx["from"], "to": tx["to"],
            "type": tx.get("type"),
            "status": rc.get("status"),
            "value_wei": int(tx.get("value",0)),
            "gas_used": rc.get("gasUsed",0),
            "effective_gas_price": eff,
            "gas_fee_eth": fee,
            "input": tx.get("input","0x"),
            "input_selector": tx.get("input","0x")[:10],
            "to_is_contract": to_is_contract
        })

        # 이벤트 디코딩
        for lg in rc.get("logs", []):
            addr = lg["address"]
            topics = [t.hex() for t in lg["topics"]]
            topic0 = topics[0].lower() if topics else ""
            data_hex = lg["data"][2:] if isinstance(lg["data"], str) else ""
            if topic0 == TOPIC_TRANSFER:
                token_cs = Web3.to_checksum_address(addr)
                from_a = topic_to_addr(topics[1]) if len(topics)>1 else ""
                to_a   = topic_to_addr(topics[2]) if len(topics)>2 else ""
                try: amount_raw = int(lg["data"],16)
                except: amount_raw = 0
                sym,dec = token_meta(token_cs)
                transfers.append({
                    "tx_hash": txh, "log_index": lg["logIndex"],
                    "token_address": token_cs, "symbol": sym, "decimals": dec,
                    "from": from_a.lower(), "to": to_a.lower(),
                    "amount_raw": amount_raw, "amount_norm": float(amount_raw)/(10**dec),
                    "token_alias": f"{sym}.ETH"
                })
            elif topic0 == TOPIC_V2:
                try: a0i,a1i,a0o,a1o = abi_decode(["uint256","uint256","uint256","uint256"], bytes.fromhex(data_hex))
                except: continue
                t0,t1 = pool_tokens(Web3.to_checksum_address(addr))
                s0,d0 = token_meta(t0); s1,d1=token_meta(t1)
                def n(v,d): return float(v)/(10**d)
                if a0i>0:
                    token_in,amt_in,token_out,amt_out = f"{s0}.ETH",n(a0i,d0),f"{s1}.ETH",n(a1o,d1)
                else:
                    token_in,amt_in,token_out,amt_out = f"{s1}.ETH",n(a1i,d1),f"{s0}.ETH",n(a0o,d0)
                swaps.append({"tx_hash":txh,"log_index":lg["logIndex"],"dex":"UNI-V2",
                              "pair_or_pool": Web3.to_checksum_address(addr),
                              "token_in":token_in,"token_out":token_out,
                              "amount_in":amt_in,"amount_out":amt_out})
            elif topic0 == TOPIC_V3:
                try: a0,a1,_,_,_ = abi_decode(["int256","int256","uint160","uint128","int24"], bytes.fromhex(data_hex))
                except: continue
                t0,t1 = pool_tokens(Web3.to_checksum_address(addr))
                s0,d0 = token_meta(t0); s1,d1=token_meta(t1)
                if a0<0:
                    token_in,amt_in,token_out,amt_out = f"{s0}.ETH",(-a0)/(10**d0),f"{s1}.ETH",(a1)/(10**d1)
                else:
                    token_in,amt_in,token_out,amt_out = f"{s1}.ETH",(-a1)/(10**d1),f"{s0}.ETH",(a0)/(10**d0)
                swaps.append({"tx_hash":txh,"log_index":lg["logIndex"],"dex":"UNI-V3",
                              "pair_or_pool": Web3.to_checksum_address(addr),
                              "token_in":token_in,"token_out":token_out,
                              "amount_in":float(amt_in),"amount_out":float(amt_out)})
            # (원하면 여기서 Wormhole 등 브리지도 함께 디코딩)

    # 출력(최종 산출물만)
    pd.DataFrame(norm).to_csv(OUT/"normalized.csv", index=False)
    pd.DataFrame(transfers).to_csv(OUT/"transfers.csv", index=False)
    pd.DataFrame(swaps).to_csv(OUT/"dex_swaps.csv", index=False)
    pd.DataFrame(bridges).to_csv(OUT/"bridge_events.csv", index=False)

if __name__ == "__main__":
    import sys
    if len(sys.argv)<3:
        print("Usage: python helpers/pipeline_fast_inmemory.py <start_block> <end_block>")
        raise SystemExit
    run_fast(int(sys.argv[1]), int(sys.argv[2]))
