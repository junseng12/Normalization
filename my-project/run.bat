@echo off
setlocal

REM #####################################################################
REM ## 파란학기 온체인 데이터 추출 - 최종 단순화 버전
REM #####################################################################

REM ---------------------------------------------------------------------
REM -- [ 1단계: 여기를 수정하세요! ]
REM -- 본인의 실제 Alchemy 또는 Infura RPC URL을 아래 "" 안에 붙여넣으세요.
REM ---------------------------------------------------------------------
set "PROVIDER_URI=https://eth-mainnet.g.alchemy.com/v2/cMNecLGH4c3WLK60hP9dG_SSViLke0vW"

REM ---------------------------------------------------------------------
REM -- [ 2단계: CMD에서 이 파일(run.bat)을 직접 실행하세요. ]
REM -- 아래 내용은 수정할 필요 없습니다.
REM ---------------------------------------------------------------------

set "START_BLOCK=18100000"
set "END_BLOCK=18100099"

echo.
echo =====================================================================
echo == 데이터 추출을 시작합니다.
echo == 사용될 RPC URL: %PROVIDER_URI%
echo == 대상 블록 범위: %START_BLOCK% - %END_BLOCK%
echo =====================================================================
echo.

REM -- [STEP 1] 블록 범위 내 모든 영수증과 로그를 한 번에 추출
echo [1/2] 지정된 블록 범위의 모든 영수증과 로그를 직접 수집합니다...
ethereumetl export_receipts_and_logs --start-block %START_BLOCK% --end-block %END_BLOCK% --provider-uri "%PROVIDER_URI%" --receipts-output receipts.jsonl --logs-output temp_logs.jsonl --batch-size 10 --max-workers 1

REM -- [STEP 2] 추출된 데이터를 최종 포맷으로 정규화
echo [2/2] 최종 데이터를 정규화하고 Tx 패키지를 생성합니다...
python process.py

REM -- [STEP 3] 임시 파일 정리
del temp_logs.jsonl

echo.
echo =====================================================================
echo == 작업 완료! tx_packages.json 파일이 생성되었습니다. ==
echo =====================================================================
echo.

endlocal