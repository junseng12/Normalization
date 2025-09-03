#!/bin/bash

# =================================================================
# == 파란학기 온체인 데이터 추출 및 처리 스크립트 (Git Bash용) ==
# =================================================================

# 1. 설정 (⭐ 여기를 수정하세요! ⭐)
# -----------------------------------------------------------------
# 사용하시는 이더리움 노드 RPC URL을 입력하세요. (예: Infura, Alchemy)
export PROVIDER_URI="https://mainnet.infura.io/v3/cf2fac9972774853ad55d6d56a8b96f1"

# 데이터 수집 범위 (임의로 1000개 블록 설정)
START_BLOCK=18100000
END_BLOCK=18100999

# 2. 필터링 규칙 (자동 설정)
# -----------------------------------------------------------------
# 토픽 및 주소 파일 설정
TRANSFER_TOPIC="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
PROTO_TOPICS="0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822,0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67,0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ff90522b2b"
cat << EOF > addresses.txt
0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc
0xBb2b8038a1640196FbE3e38816F3e67Cba72D940
0x88e6A0c2dDD26FEEb64F039a2c411296FcB3f5640
0x8ad599c3a0ff1De082011EFDDc58f1908eb6e6D8
0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7
0xDC24316b9AE028F1497c275EB9192a3Ea0f67022
0x3ee18B2214AFF97000D974cf647E7C347E8fa585
EOF

# 3. 데이터 추출 (ETL)
# -----------------------------------------------------------------
echo "[1/5] ERC-20 Transfer 로그 수집 중..."
ethereum-etl export_logs --start-block $START_BLOCK --end-block $END_BLOCK --provider-uri "$PROVIDER_URI" --output logs_token.jsonl --topics $TRANSFER_TOPIC

echo "[2/5] DEX/브리지 프로토콜 로그 수집 중..."
ethereum-etl export_logs --start-block $START_BLOCK --end-block $END_BLOCK --provider-uri "$PROVIDER_URI" --output logs_proto.jsonl --contract-addresses addresses.txt --topics $PROTO_TOPICS

echo "[3/5] 고유 트랜잭션 해시 추출 중..."
# Git Bash에서는 jq를 사용하는 것이 더 편리하고 안정적입니다.
cat logs_token.jsonl logs_proto.jsonl | jq -r .transaction_hash | sort | uniq > unique_tx_hashes.txt

echo "[4/5] 필요한 영수증(Receipts)만 수집 중..."
ethereum-etl export_receipts_and_logs --transaction-hashes unique_tx_hashes.txt --provider-uri "$PROVIDER_URI" --output receipts.jsonl

# 4. 데이터 처리 및 정규화
# -----------------------------------------------------------------
echo "[5/5] 데이터 정규화 및 Tx 패키지 생성 중..."
python process.py # 파이썬 스크립트는 변경 없이 그대로 사용 가능합니다.

# 5. 임시 파일 정리
# -----------------------------------------------------------------
rm logs_token.jsonl