import json

TOKEN_REGISTRY = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
    "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18},
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "decimals": 18},
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {"symbol": "WBTC", "decimals": 8},
}
EVENT_DECODERS = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": {
        "name": "Transfer", "type": "transfers",
        "decoder": lambda log: {"token_address": log['address'], "from": "0x" + log['topics'][1][26:], "to": "0x" + log['topics'][2][26:],"amount_raw": int(log['data'], 16)}
    },
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": {
        "name": "UniswapV2Swap", "type": "dex_swaps", "decoder": lambda log: { "dex": "UNI-V2", "pair_or_pool": log['address'] }
    },
    "0xc42079f94a635f29174924f928cc2ac818eb64fed8004e115fbcca67": {
        "name": "UniswapV3Swap", "type": "dex_swaps", "decoder": lambda log: { "dex": "UNI-V3", "pair_or_pool": log['address'] }
    },
    "0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ff90522b2b": {
        "name": "WormholeSend", "type": "bridge_events",
        "decoder": lambda log: {"bridge_id": "wormhole", "direction": "send", "fields": { "sender": "0x" + log['topics'][1][26:] }}
    }
}
def normalize_transfer(event_data):
    token_info = TOKEN_REGISTRY.get(event_data['token_address'], {"symbol": "UNKNOWN", "decimals": 18})
    amount_norm = event_data['amount_raw'] / (10 ** token_info['decimals'])
    return {"token_alias": f"{token_info['symbol']}.ETH", "token_address": event_data['token_address'], "from": event_data['from'], "to": event_data['to'], "amount_norm": round(amount_norm, 6)}
def process_logs(logs):
    events = {"transfers": [], "dex_swaps": [], "bridge_events": []}
    for log in logs:
        topic0 = log['topics'][0]
        decoder_info = EVENT_DECODERS.get(topic0)
        if not decoder_info: continue
        try:
            decoded_event = decoder_info['decoder'](log)
            event_type = decoder_info['type']
            if event_type == 'transfers':
                events['transfers'].append(normalize_transfer(decoded_event))
            else:
                events[event_type].append(decoded_event)
        except Exception as e:
            print(f"Warning: Could not decode event with topic {topic0}. Error: {e}")
    return events
def main():
    print("Python: Reading receipts.jsonl...")
    tx_packages = {}
    try:
        with open('receipts.jsonl', 'r') as f:
            for line in f:
                receipt = json.loads(line)
                tx_hash = receipt['transaction_hash']
                if tx_hash not in tx_packages:
                    # 필터링: 이벤트가 하나라도 있는지 확인
                    processed_events = process_logs(receipt['logs'])
                    if any(processed_events.values()):
                        tx_packages[tx_hash] = {
                            "chain_id": "eth", "block_number": receipt['block_number'],
                            "tx": {"hash": tx_hash, "from": receipt['from_address'], "to": receipt['to_address'], "status": receipt['status'], "gas_used": receipt['gas_used'], "effective_gas_price": receipt['effective_gas_price']},
                            "events": processed_events
                        }
    except FileNotFoundError:
        print("Error: receipts.jsonl not found. Step 1 might have failed to produce output.")
        return
    except json.JSONDecodeError:
        print("Error: receipts.jsonl is empty or corrupted. Step 1 likely failed.")
        return
    print(f"Python: Processed {len(tx_packages)} transactions. Saving to tx_packages.json...")
    with open('tx_packages.json', 'w') as f:
        json.dump(list(tx_packages.values()), f, indent=2)

if __name__ == "__main__":
    main()