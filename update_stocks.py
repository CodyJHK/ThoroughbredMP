import os
import sys
import time
import random
from datetime import datetime

import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from notion_client import Client

# -----------------------------
# 환경 변수
# -----------------------------
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID = os.environ.get('DATABASE_ID')
if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN and DATABASE_ID environment variables must be set")
    sys.exit(1)

# -----------------------------
# 클라이언트 / 시간대
# -----------------------------
notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone('Asia/Seoul')

# -----------------------------
# HTTP 세션 (재시도 설정)
# -----------------------------
def build_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # 간단한 UA 지정 (일부 환경에서 필요)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; PriceBot/1.0; +https://github.com/)"
    })
    return session

SESSION = build_session()

# -----------------------------
# Yahoo Finance API helpers
# -----------------------------
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

def fetch_from_quote(symbol: str):
    """
    v7 quote 엔드포인트에서 현재가/전일종가/시가총액을 우선적으로 가져옴.
    """
    try:
        r = SESSION.get(YF_QUOTE_URL, params={"symbols": symbol}, timeout=10)
        r.raise_for_status()
        data = r.json()
        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            return None
        q = results[0]
        current_price = q.get("regularMarketPrice")
        previous_close = q.get("regularMarketPreviousClose")
        market_cap = q.get("marketCap", 0) or 0
        return {
            "currentPrice": float(current_price) if current_price is not None else None,
            "previousClose": float(previous_close) if previous_close is not None else None,
            "marketCap": int(market_cap) if market_cap is not None else 0
        }
    except Exception as e:
        print(f"  경고(quote): {symbol} - {e}")
        return None

def fetch_from_chart(symbol: str):
    """
    v8 chart 엔드포인트로 최근 5영업일 일봉 종가를 가져와
    현재가/전일종가를 산출 (시가총액은 제공 안함 → 0 반환).
    """
    try:
        r = SESSION.get(
            YF_CHART_URL.format(symbol=symbol),
            params={"range": "5d", "interval": "1d"},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        result = (data.get("chart", {}) or {}).get("result", [])
        if not result:
            return None
        indicators = result[0].get("indicators", {})
        quotes = indicators.get("quote", [])
        if not quotes:
            return None
        closes = quotes[0].get("close", [])
        # 유효한 종가만 추출
        closes = [c for c in closes if c is not None]
        if not closes:
            return None

        current_price = float(closes[-1])
        previous_close = float(closes[-2]) if len(closes) >= 2 else current_price
        return {
            "currentPrice": current_price,
            "previousClose": previous_close,
            "marketCap": 0  # chart에는 시총 없음
        }
    except Exception as e:
        print(f"  경고(chart): {symbol} - {e}")
        return None

def get_stock_data(symbol: str, retry_count: int = 3):
    """
    1) quote로 시도 → 2) 부족분은 chart로 보완.
    모두 실패 시 None.
    """
    for attempt in range(retry_count):
        try:
            if attempt > 0:
                wait_time = (attempt + 1) * 1.5 + random.uniform(0.5, 1.5)
                print(f"  재시도 {attempt+1}/{retry_count} (대기 {wait_time:.1f}s)")
                time.sleep(wait_time)

            q = fetch_from_quote(symbol)
            if q is None:
                # quote 실패 → chart로 시도
                c = fetch_from_chart(symbol)
                if c is None:
                    continue
                # chart 결과 사용
                data = c
            else:
                # quote 성공. 값 보완 필요하면 chart 병합
                if q.get("currentPrice") is None or q.get("previousClose") is None:
                    c = fetch_from_chart(symbol)
                    if c:
                        q["currentPrice"] = q.get("currentPrice") or c.get("currentPrice")
                        q["previousClose"] = q.get("previousClose") or c.get("previousClose")
                data = q

            # 숫자 정리 및 시총(억 단위 변환)
            curr = float(data.get("currentPrice") or 0)
            prev = float(data.get("previousClose") or 0)
            mcap_raw = int(data.get("marketCap") or 0)
            mcap_eok = round(mcap_raw / 100_000_000) if mcap_raw > 0 else 0

            print(f"  현재가: ${curr:.2f}")
            print(f"  전일종가: ${prev:.2f}")
            if mcap_eok > 0:
                print(f"  시가총액: {mcap_eok}억")

            return {
                "currentPrice": curr,
                "previousClose": prev if prev > 0 else curr,
                "marketCap": mcap_eok
            }

        except Exception as e:
            if attempt == retry_count - 1:
                print(f"  오류: {symbol} 데이터 가져오기 실패 - {e}")
                return None
            # 다음 루프에서 재시도
            continue

    return None

# -----------------------------
# Notion helpers
# -----------------------------
def update_notion_page(page_id: str, stock_data: dict) -> bool:
    try:
        # 변동률 계산(로깅용)
        prev = stock_data.get("previousClose", 0)
        curr = stock_data.get("currentPrice", 0)
        change_percent = round(((curr - prev) / prev) * 100, 2) if prev > 0 else 0.0

        properties = {
            "현재가": {"number": curr},
            "전일종가": {"number": prev},
            "시가총액": {"number": stock_data.get("marketCap", 0)},
            "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}},
        }

        notion.pages.update(page_id=page_id, properties=properties)
        print(f"  ✓ 업데이트 완료 (변동률: {change_percent:+.2f}%)")
        return True
    except Exception as e:
        print(f"Error updating page {page_id}: {e}")
        return False

def fetch_all_pages(database_id: str):
    """
    Notion DB 전체 페이지 페이징 수집
    """
    pages = []
    start_cursor = None
    while True:
        payload = {"database_id": database_id}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        resp = notion.databases.query(**payload)
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return pages

# -----------------------------
# 메인
# -----------------------------
def main():
    print("=== 주식 가격 업데이트 시작 ===")
    print(f"시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}\n")

    try:
        pages = fetch_all_pages(DATABASE_ID)
        if not pages:
            print("데이터베이스에 항목이 없습니다.")
            return

        print(f"총 {len(pages)}개 종목 발견\n")

        updated_count = 0
        error_count = 0
        skipped_count = 0

        for idx, page in enumerate(pages, start=1):
            page_id = page["id"]
            props = page.get("properties", {})

            # '티커' title 속성에서 티커 추출
            title_prop = props.get("티커", {})
            ticker = None
            if title_prop.get("type") == "title":
                title_items = title_prop.get("title", [])
                if title_items:
                    ticker = (title_items[0].get("text", {}) or {}).get("content", "")
                    ticker = (ticker or "").strip().upper()

            if not ticker:
                print(f"[{idx}/{len(pages)}] 티커 미존재 → 건너뜀 (페이지 ID: {page_id})")
                skipped_count += 1
                continue

            print(f"[{idx}/{len(pages)}] 처리 중: {ticker}")

            stock_data = get_stock_data(ticker)
            if stock_data and stock_data["currentPrice"] > 0:
                if update_notion_page(page_id, stock_data):
                    updated_count += 1
                else:
                    error_count += 1
            else:
                print("  ✗ 데이터를 가져올 수 없습니다")
                error_count += 1

            # API 과호출 방지 대기(마지막 항목 제외)
            if idx < len(pages):
                wait = random.uniform(1.0, 2.5)
                print(f"  다음 종목 대기: {wait:.1f}초\n")
                time.sleep(wait)
            else:
                print()

        print("=== 업데이트 완료 ===")
        print(f"성공: {updated_count}개")
        print(f"실패: {error_count}개")
        print(f"건너뜀: {skipped_count}개")
        print(f"총 처리: {len(pages)}개")

    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
