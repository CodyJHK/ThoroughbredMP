import os
import sys
from datetime import datetime
import pytz
import requests
from notion_client import Client

# ── 환경 변수 ─────────────────────────────────────────────
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID  = os.environ.get('DATABASE_ID')
FMP_API_KEY  = os.environ.get('FMP_API_KEY')
if not NOTION_TOKEN or not DATABASE_ID or not FMP_API_KEY:
    print("Error: NOTION_TOKEN, DATABASE_ID, FMP_API_KEY must be set")
    sys.exit(1)

# ── Notion / TZ ───────────────────────────────────────────
notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone("Asia/Seoul")

# ── FMP endpoints ─────────────────────────────────────────
FMP_QUOTE_URL = "https://financialmodelingprep.com/api/v3/quote/{}"     # stocks (batch)
FMP_FX_LAST   = "https://financialmodelingprep.com/api/v4/forex/last/{}"  # e.g., USDKRW
FMP_FX_PRICE  = "https://financialmodelingprep.com/api/v3/forex/{}"       # fallback

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

def fetch_fmp_quotes(symbols):
    if not symbols:
        return {}

    # 일괄 조회 시도
    try:
        url = FMP_QUOTE_URL.format(",".join(symbols))
        r = requests.get(url, params={"apikey": FMP_API_KEY}, timeout=20)
        r.raise_for_status()
        data = r.json() or []
        out = {}
        for it in data:
            sym  = (it.get("symbol") or "").upper()
            curr = it.get("price")
            prev = it.get("previousClose")
            mcap = it.get("marketCap") or 0
            name = it.get("name") or sym
            curr = float(curr) if curr is not None else 0.0
            prev = float(prev) if prev is not None else curr
            mcap_eok = round(mcap / 100_000_000) if mcap and mcap > 0 else 0
            out[sym] = {
                "currentPrice": curr,
                "previousClose": prev,
                "marketCap": mcap_eok,
                "name": name,
            }
        return out
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print("일괄 조회 실패 (403). 개별 조회로 전환합니다...")
            # 개별 조회로 폴백
            return fetch_fmp_quotes_individually(symbols)
        else:
            raise

def fetch_fmp_quotes_individually(symbols):
    """티커를 개별적으로 조회 (403 에러 시 폴백)"""
    import time
    out = {}
    total = len(symbols)

    for idx, sym in enumerate(symbols, 1):
        try:
            url = FMP_QUOTE_URL.format(sym)
            r = requests.get(url, params={"apikey": FMP_API_KEY}, timeout=20)
            r.raise_for_status()
            data = r.json() or []

            if data and isinstance(data, list) and len(data) > 0:
                it = data[0]
                curr = it.get("price")
                prev = it.get("previousClose")
                mcap = it.get("marketCap") or 0
                name = it.get("name") or sym
                curr = float(curr) if curr is not None else 0.0
                prev = float(prev) if prev is not None else curr
                mcap_eok = round(mcap / 100_000_000) if mcap and mcap > 0 else 0
                out[sym] = {
                    "currentPrice": curr,
                    "previousClose": prev,
                    "marketCap": mcap_eok,
                    "name": name,
                }
                print(f"  [{idx}/{total}] {sym} 조회 완료")
            else:
                print(f"  [{idx}/{total}] {sym} 데이터 없음")

            # Rate limit 방지: 0.3초 대기
            if idx < total:
                time.sleep(0.3)

        except Exception as e:
            print(f"  [{idx}/{total}] {sym} 조회 실패: {e}")
            continue

    return out

def _extract_fx_price(obj):
    # 가능한 키 우선순위: price > rate/exchangeRate > (bid+ask)/2 > bid > ask
    for k in ("price", "rate", "exchangeRate"):
        v = obj.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    bid, ask = obj.get("bid"), obj.get("ask")
    if isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
        return (float(bid) + float(ask)) / 2.0
    if isinstance(bid, (int, float)):
        return float(bid)
    if isinstance(ask, (int, float)):
        return float(ask)
    return None

def fetch_usdkrw():
    # 1) v4 last 우선
    try:
        r = requests.get(FMP_FX_LAST.format("USDKRW"),
                         params={"apikey": FMP_API_KEY}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            p = _extract_fx_price(data)
        elif isinstance(data, list) and data:
            p = _extract_fx_price(data[0])
        else:
            p = None
        if p:
            return float(p)
    except Exception:
        pass
    # 2) v3 forex 폴백
    try:
        r = requests.get(FMP_FX_PRICE.format("USDKRW"),
                         params={"apikey": FMP_API_KEY}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            p = _extract_fx_price(data)
        elif isinstance(data, list) and data:
            p = _extract_fx_price(data[0])
        else:
            p = None
        if p:
            return float(p)
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

    # 배치 조회
    uniq = sorted(set(symbols))
    print(f"일괄 조회 대상: {len(uniq)}개 티커")
    try:
        data_map = fetch_fmp_quotes(uniq)
    except Exception as e:
        print(f"FMP 조회 실패: {e}")
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

