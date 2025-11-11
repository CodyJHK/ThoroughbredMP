import os
import sys
from datetime import datetime
import pytz
import yfinance as yf
from notion_client import Client

# ── 환경 변수 ─────────────────────────────────────────────
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID  = os.environ.get('DATABASE_ID')
if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN, DATABASE_ID must be set")
    sys.exit(1)

# ── Notion / TZ ───────────────────────────────────────────
notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone("Asia/Seoul")

def fetch_all_pages(database_id: str):
    pages, start = [], None
    while True:
        kwargs = {"database_id": database_id}
        if start:
            kwargs["start_cursor"] = start
            
        resp = notion.databases.query(**kwargs)
        
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start = resp.get("next_cursor")
    return pages

def parse_ticker_from_page(page: dict) -> str:
    tp = page.get("properties", {}).get("티커", {})
    if tp.get("type") != "title":
        return ""
    items = tp.get("title", [])
    if not items:
        return ""
    t = (items[0].get("text", {}) or {}).get("content", "") or ""
    return t.strip().upper()

def fetch_yahoo_quotes(symbols):
    """Yahoo Finance를 사용하여 주식 가격 조회"""
    if not symbols:
        return {}

    out = {}
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            info = ticker.info

            # 현재가
            curr = info.get("currentPrice") or info.get("regularMarketPrice") or 0.0
            # 전일종가
            prev = info.get("previousClose") or info.get("regularMarketPreviousClose") or curr
            # 시가총액 (억 단위로 변환)
            mcap = info.get("marketCap") or 0
            mcap_eok = round(mcap / 100_000_000) if mcap and mcap > 0 else 0
            # 종목명
            name = info.get("longName") or info.get("shortName") or sym

            if curr > 0:
                out[sym] = {
                    "currentPrice": float(curr),
                    "previousClose": float(prev),
                    "marketCap": mcap_eok,
                    "name": name,
                }
        except Exception as e:
            print(f"  {sym}: 조회 실패 - {e}")
            continue

    return out

def fetch_usdkrw():
    """Yahoo Finance를 사용하여 USDKRW 환율 조회"""
    try:
        ticker = yf.Ticker("USDKRW=X")
        info = ticker.info
        # 현재 환율
        rate = info.get("regularMarketPrice") or info.get("currentPrice")
        if rate:
            return float(rate)
    except Exception:
        pass
    return None

def update_notion_page(page_id: str, stock: dict, usdkrw: float | None):
    props = {
        "현재가": {"number": stock["currentPrice"]},
        "전일종가": {"number": stock["previousClose"] if stock["previousClose"] > 0 else stock["currentPrice"]},
        "시가총액": {"number": stock["marketCap"]},
        "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}},
        "종목명": {"rich_text": [{"text": {"content": stock.get("name","")}}]},
    }
    if isinstance(usdkrw, (int, float)) and usdkrw > 0:
        props["USDKRW"] = {"number": float(usdkrw)}
    notion.pages.update(page_id=page_id, properties=props)

def main():
    print("=== 주식 가격 업데이트 시작 ===")
    print(f"시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}\n")

    pages = fetch_all_pages(DATABASE_ID)
    if not pages:
        print("데이터베이스에 항목이 없습니다.")
        return
    print(f"총 {len(pages)}개 종목 발견\n")

    # 환율 1회 조회
    usdkrw = fetch_usdkrw()
    if usdkrw:
        print(f"USDKRW: {usdkrw:.2f}")

    # 티커 수집
    rows, symbols = [], []
    for p in pages:
        t = parse_ticker_from_page(p)
        if not t:
            print(f"티커 없음 → 건너뜀 ({p['id']})")
            continue
        rows.append((p["id"], t))
        symbols.append(t)

    if not rows:
        print("유효한 티커가 없습니다.")
        return

    # Yahoo Finance 조회
    uniq = sorted(set(symbols))
    print(f"\nYahoo Finance 조회 시작: {len(uniq)}개 티커")
    try:
        data_map = fetch_yahoo_quotes(uniq)
        print(f"조회 완료: {len(data_map)}개 성공\n")
    except Exception as e:
        print(f"Yahoo Finance 조회 실패: {e}")
        sys.exit(1)

    # 페이지별 업데이트
    ok = fail = 0
    for i, (pid, sym) in enumerate(rows, start=1):
        info = data_map.get(sym)
        if not info or info["currentPrice"] <= 0:
            print(f"[{i}/{len(rows)}] {sym} ✗ 데이터 없음/오류")
            fail += 1
            continue
        try:
            update_notion_page(pid, info, usdkrw)
            chg = 0.0
            if info["previousClose"] > 0:
                chg = round((info["currentPrice"] - info["previousClose"]) / info["previousClose"] * 100, 2)
            mcap_log = f" | 시총 {info['marketCap']}억" if info["marketCap"] > 0 else ""
            fx_log = f" | USDKRW {usdkrw:.2f}" if isinstance(usdkrw, (int, float)) else ""
            print(f"[{i}/{len(rows)}] {sym} ✓ {info['currentPrice']:.2f} ({chg:+.2f}%){mcap_log}{fx_log} | {info.get('name','')}")
            ok += 1
        except Exception as e:
            print(f"[{i}/{len(rows)}] {sym} ✗ Notion 업데이트 실패: {e}")
            fail += 1

    print("\n=== 완료 ===")
    print(f"성공: {ok} | 실패: {fail} | 총: {len(rows)}")

if __name__ == "__main__":
    main()

