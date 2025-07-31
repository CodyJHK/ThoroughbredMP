import os
import sys
import time
from datetime import datetime

import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from notion_client import Client

# ── 환경 변수 ─────────────────────────────────────────────
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID  = os.environ.get('DATABASE_ID')
if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN and DATABASE_ID must be set")
    sys.exit(1)

# ── Notion / TZ ───────────────────────────────────────────
notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone("Asia/Seoul")

# ── Yahoo Finance ─────────────────────────────────────────
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
BATCH_SIZE = 40          # 한번에 조회할 최대 티커 개수
BATCH_SLEEP_SEC = 2.0    # 배치 간 간격(429 완화)
SINGLE_RETRY_WAIT = 3.0  # 개별 재시도 대기

# ── HTTP 세션(재시도/백오프) ───────────────────────────────
def build_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,                 # 1s, 2s, 4s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (PriceBot/1.0; +https://github.com/)",
        "Accept": "application/json",
        "Connection": "keep-alive",
    })
    return s

SESSION = build_session()

# ── 데이터 수집 ────────────────────────────────────────────
def fetch_quotes_batch(symbols):
    """
    symbols(list[str])를 최대 BATCH_SIZE로 나눈 뒤
    Yahoo quote로 일괄 조회하여 dict 반환: {SYMBOL: {price, prev, mcap, name}}
    """
    out = {}

    def _parse_result(item):
        curr = item.get("regularMarketPrice")
        prev = item.get("regularMarketPreviousClose")
        mcap = item.get("marketCap") or 0
        name = item.get("longName") or item.get("shortName") or item.get("displayName") or item.get("symbol")
        # 시총 억 단위로 변환
        mcap_eok = round(mcap / 100_000_000) if mcap and mcap > 0 else 0
        return {
            "currentPrice": float(curr) if curr is not None else 0.0,
            "previousClose": float(prev) if prev is not None else 0.0,
            "marketCap": mcap_eok,
            "name": name,
        }

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i+BATCH_SIZE]
        try:
            r = SESSION.get(YF_QUOTE_URL, params={"symbols": ",".join(batch)}, timeout=15)
            r.raise_for_status()
            results = (r.json().get("quoteResponse") or {}).get("result", []) or []
            # 결과 매핑
            for it in results:
                sym = (it.get("symbol") or "").upper()
                if not sym:
                    continue
                out[sym] = _parse_result(it)
        except Exception as e:
            print(f"  배치 조회 오류({i//BATCH_SIZE+1}): {e}")
        # 배치 간 간격
        if i + BATCH_SIZE < len(symbols):
            time.sleep(BATCH_SLEEP_SEC)

    return out

def fetch_quote_single(symbol):
    """
    배치 누락/실패 티커에 대해 개별 보강 1회 시도
    """
    try:
        r = SESSION.get(YF_QUOTE_URL, params={"symbols": symbol}, timeout=15)
        r.raise_for_status()
        results = (r.json().get("quoteResponse") or {}).get("result", []) or []
        if not results:
            return None
        it = results[0]
        curr = it.get("regularMarketPrice")
        prev = it.get("regularMarketPreviousClose")
        mcap = it.get("marketCap") or 0
        name = it.get("longName") or it.get("shortName") or it.get("displayName") or it.get("symbol")
        mcap_eok = round(mcap / 100_000_000) if mcap and mcap > 0 else 0
        return {
            "currentPrice": float(curr) if curr is not None else 0.0,
            "previousClose": float(prev) if prev is not None else 0.0,
            "marketCap": mcap_eok,
            "name": name,
        }
    except Exception as e:
        print(f"  개별 조회 오류({symbol}): {e}")
        return None

# ── Notion ────────────────────────────────────────────────
def fetch_all_pages(database_id: str):
    pages, start = [], None
    while True:
        payload = {"database_id": database_id}
        if start:
            payload["start_cursor"] = start
        resp = notion.databases.query(**payload)
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start = resp.get("next_cursor")
    return pages

def update_notion_page(page_id: str, stock: dict):
    props = {
        "현재가": {"number": stock["currentPrice"]},
        "전일종가": {"number": stock["previousClose"] if stock["previousClose"] > 0 else stock["currentPrice"]},
        "시가총액": {"number": stock["marketCap"]},
        "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}},
    }
    if stock.get("name"):
        props["종목명"] = {"rich_text": [{"text": {"content": stock["name"]}}]}
    notion.pages.update(page_id=page_id, properties=props)

# ── 메인 ─────────────────────────────────────────────────
def main():
    print("=== 주식 가격 업데이트 시작 ===")
    print(f"시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}\n")

    pages = fetch_all_pages(DATABASE_ID)
    if not pages:
        print("데이터베이스에 항목이 없습니다.")
        return
    print(f"총 {len(pages)}개 종목 발견\n")

    # 1) 티커 수집/정규화/중복 제거
    page_rows = []
    symbols = []
    for page in pages:
        pid = page["id"]
        tp = page.get("properties", {}).get("티커", {})
        ticker = ""
        if tp.get("type") == "title":
            items = tp.get("title", [])
            if items:
                ticker = (items[0].get("text", {}) or {}).get("content", "") or ""
        ticker = ticker.strip().upper()
        if not ticker:
            print(f"티커 없음 → 건너뜀 ({pid})")
            continue
        page_rows.append((pid, ticker))
        symbols.append(ticker)

    if not page_rows:
        print("유효한 티커가 없습니다.")
        return

    uniq_syms = sorted(set(symbols))
    print(f"일괄 조회 대상: {len(uniq_syms)}개 티커\n")

    # 2) 배치 조회
    data_map = fetch_quotes_batch(uniq_syms)

    # 3) 페이지별 업데이트 (누락 티커는 개별 1회 재시도)
    ok = fail = 0
    for idx, (pid, sym) in enumerate(page_rows, start=1):
        info = data_map.get(sym)
        if info is None:
            # 개별 재시도(한 번만)
            print(f"[{idx}/{len(page_rows)}] {sym} 배치 누락 → 개별 재시도")
            time.sleep(SINGLE_RETRY_WAIT)
            info = fetch_quote_single(sym)

        if not info or info["currentPrice"] <= 0:
            print(f"[{idx}/{len(page_rows)}] {sym} ✗ 데이터 없음/오류")
            fail += 1
            continue

        try:
            update_notion_page(pid, info)
            chg = 0.0
            if info["previousClose"] > 0:
                chg = round((info["currentPrice"] - info["previousClose"]) / info["previousClose"] * 100, 2)
            mcap_log = f" | 시총 {info['marketCap']}억" if info["marketCap"] > 0 else ""
            name_log = f" | {info.get('name')}" if info.get("name") else ""
            print(f"[{idx}/{len(page_rows)}] {sym} ✓ {info['currentPrice']:.2f} ({chg:+.2f}%)" + mcap_log + name_log)
            ok += 1
        except Exception as e:
            print(f"[{idx}/{len(page_rows)}] {sym} ✗ Notion 업데이트 실패: {e}")
            fail += 1

    print("\n=== 완료 ===")
    print(f"성공: {ok} | 실패: {fail} | 총: {len(page_rows)}")

if __name__ == "__main__":
    main()
