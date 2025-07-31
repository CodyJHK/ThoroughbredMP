import os
import sys
from datetime import datetime
import pytz
import requests
from notion_client import Client

# ── 환경 변수 ─────────────────────────────────────────────
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID  = os.environ.get('DATABASE_ID')
if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN and DATABASE_ID must be set")
    sys.exit(1)

# ── 클라이언트/타임존 ─────────────────────────────────────
notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone("Asia/Seoul")

# ── Yahoo Finance 엔드포인트 ─────────────────────────────
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

def fetch_from_quote(symbol: str):
    """quote로 현재가/전일가/시총/종목명 획득"""
    r = requests.get(YF_QUOTE_URL, params={"symbols": symbol}, timeout=10)
    r.raise_for_status()
    res = r.json().get("quoteResponse", {}).get("result", [])
    if not res:
        return None
    q = res[0]
    return {
        "currentPrice": q.get("regularMarketPrice"),
        "previousClose": q.get("regularMarketPreviousClose"),
        "marketCap": q.get("marketCap") or 0,
        "name": q.get("longName") or q.get("shortName") or q.get("displayName") or q.get("symbol")
    }

def fetch_from_chart(symbol: str):
    """chart로 최근 일봉 종가 기반 현재가/전일가 보완 (시총/이름 없음)"""
    r = requests.get(YF_CHART_URL.format(symbol=symbol),
                     params={"range": "5d", "interval": "1d"}, timeout=10)
    r.raise_for_status()
    result = r.json().get("chart", {}).get("result", [])
    if not result:
        return None
    closes = [c for c in result[0]["indicators"]["quote"][0]["close"] if c is not None]
    if not closes:
        return None
    curr = float(closes[-1])
    prev = float(closes[-2]) if len(closes) >= 2 else curr
    return {"currentPrice": curr, "previousClose": prev}

def get_stock_data(symbol: str):
    """quote 우선, 부족하면 chart로 보완"""
    try:
        q = fetch_from_quote(symbol)
    except Exception as e:
        print(f"  quote 오류: {symbol} - {e}")
        q = None

    if not q:
        try:
            c = fetch_from_chart(symbol)
        except Exception as e:
            print(f"  chart 오류: {symbol} - {e}")
            c = None
        if not c:
            return None
        data = {
            "currentPrice": float(c["currentPrice"]),
            "previousClose": float(c["previousClose"]),
            "marketCap": 0,
            "name": None
        }
    else:
        # quote 결과에서 비는 값이 있으면 chart로 보완
        curr = q.get("currentPrice")
        prev = q.get("previousClose")
        if curr is None or prev is None:
            try:
                c = fetch_from_chart(symbol)
            except Exception:
                c = None
            if c:
                curr = curr if curr is not None else c["currentPrice"]
                prev = prev if prev is not None else c["previousClose"]
        data = {
            "currentPrice": float(curr or 0),
            "previousClose": float(prev or 0),
            "marketCap": int(q.get("marketCap") or 0),
            "name": q.get("name")
        }

    # 시가총액: 억 단위 변환
    mcap_eok = round(data["marketCap"] / 100_000_000) if data["marketCap"] > 0 else 0
    return {
        "currentPrice": data["currentPrice"],
        "previousClose": data["previousClose"] if data["previousClose"] > 0 else data["currentPrice"],
        "marketCap": mcap_eok,
        "name": data.get("name")
    }

def update_notion_page(page_id: str, stock: dict):
    """Notion 속성 업데이트 (종목명은 rich_text 고정)"""
    props = {
        "현재가": {"number": stock["currentPrice"]},
        "전일종가": {"number": stock["previousClose"]},
        "시가총액": {"number": stock["marketCap"]},
        "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}}
    }
    if stock.get("name"):
        props["종목명"] = {"rich_text": [{"text": {"content": stock["name"]}}]}

    notion.pages.update(page_id=page_id, properties=props)

def fetch_all_pages(database_id: str):
    """DB 전체 페이지 조회(간단 페이징)"""
    pages, start_cursor = [], None
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

def main():
    print("=== 주식 가격 업데이트 시작 ===")
    print(f"시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}\n")

    pages = fetch_all_pages(DATABASE_ID)
    if not pages:
        print("데이터베이스에 항목이 없습니다.")
        return

    print(f"총 {len(pages)}개 종목 발견\n")
    ok = fail = skip = 0

    for i, page in enumerate(pages, start=1):
        page_id = page["id"]
        tprop = page.get("properties", {}).get("티커", {})
        ticker = ""
        if tprop.get("type") == "title":
            items = tprop.get("title", [])
            if items:
                ticker = (items[0].get("text", {}) or {}).get("content", "") or ""
        ticker = ticker.strip().upper()

        if not ticker:
            print(f"[{i}/{len(pages)}] 티커 없음 → 건너뜀 ({page_id})")
            skip += 1
            continue

        print(f"[{i}/{len(pages)}] {ticker} 조회")
        data = get_stock_data(ticker)
        if not data or data["currentPrice"] <= 0:
            print("  ✗ 데이터 없음/오류")
            fail += 1
            continue

        try:
            update_notion_page(page_id, data)
            chg = 0.0
            if data["previousClose"] > 0:
                chg = round((data["currentPrice"] - data["previousClose"]) / data["previousClose"] * 100, 2)
            name_log = f" | {data.get('name')}" if data.get("name") else ""
            mcap_log = f" | 시총 {data['marketCap']}억" if data["marketCap"] > 0 else ""
            print(f"  ✓ {data['currentPrice']:.2f} ({chg:+.2f}%){mcap_log}{name_log}")
            ok += 1
        except Exception as e:
            print(f"  ✗ Notion 업데이트 실패: {e}")
            fail += 1

    print("\n=== 완료 ===")
    print(f"성공: {ok} | 실패: {fail} | 건너뜀: {skip} | 총: {len(pages)}")

if __name__ == "__main__":
    main()
