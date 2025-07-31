import os
import sys
from notion_client import Client
import requests
from datetime import datetime
import pytz
import time

# 환경 변수
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID = os.environ.get('DATABASE_ID')
ALPHA_VANTAGE_KEY = os.environ.get('ALPHA_VANTAGE_KEY', 'demo')  # 무료 키 사용 가능

if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN and DATABASE_ID environment variables must be set")
    sys.exit(1)

notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone('Asia/Seoul')

def get_stock_data_alpha(ticker):
    """Alpha Vantage API 사용"""
    try:
        url = f"https://www.alphavantage.co/query"
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': ticker,
            'apikey': ALPHA_VANTAGE_KEY
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'Global Quote' in data:
            quote = data['Global Quote']
            return {
                'currentPrice': float(quote.get('05. price', 0)),
                'previousClose': float(quote.get('08. previous close', 0)),
                'marketCap': 0  # Alpha Vantage 무료 버전에서는 미제공
            }
        else:
            print(f"  경고: {ticker} 데이터 없음")
            return None
            
    except Exception as e:
        print(f"  오류: {str(e)}")
        return None

def get_stock_data_yfinance_simple(ticker):
    """yfinance 간단한 방법"""
    try:
        import yfinance as yf
        
        # download 함수 사용 (더 안정적)
        data = yf.download(ticker, period="5d", progress=False)
        
        if data.empty:
            return None
            
        current_price = float(data['Close'].iloc[-1])
        
        # 전일 종가
        if len(data) >= 2:
            previous_close = float(data['Close'].iloc[-2])
        else:
            previous_close = current_price
            
        return {
            'currentPrice': current_price,
            'previousClose': previous_close,
            'marketCap': 0
        }
    except:
        return None

def update_notion_page(page_id, stock_data):
    """Notion 페이지 업데이트"""
    try:
        properties = {
            "현재가": {"number": stock_data['currentPrice']},
            "전일종가": {"number": stock_data['previousClose']},
            "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}}
        }
        
        if stock_data.get('marketCap', 0) > 0:
            properties["시가총액"] = {"number": stock_data['marketCap']}
        
        notion.pages.update(
            page_id=page_id,
            properties=properties
        )
        return True
    except Exception as e:
        print(f"  Notion 업데이트 오류: {str(e)}")
        return False

def main():
    print(f"=== 주식 가격 업데이트 시작 ===")
    print(f"시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}\n")
    
    try:
        response = notion.databases.query(database_id=DATABASE_ID)
        pages = response.get('results', [])
        
        if not pages:
            print("데이터베이스에 항목이 없습니다.")
            return
        
        print(f"총 {len(pages)}개 종목 발견\n")
        
        updated_count = 0
        error_count = 0
        
        for i, page in enumerate(pages):
            page_id = page['id']
            properties = page.get('properties', {})
            
            # 티커 가져오기
            ticker_prop = properties.get('티커', {})
            if ticker_prop.get('type') == 'title' and ticker_prop.get('title'):
                ticker = ticker_prop['title'][0]['text']['content'].strip().upper()
            else:
                continue
            
            print(f"[{i+1}/{len(pages)}] {ticker}")
            
            # yfinance 시도
            stock_data = get_stock_data_yfinance_simple(ticker)
            
            # 실패 시 Alpha Vantage 시도
            if not stock_data and ALPHA_VANTAGE_KEY != 'demo':
                print("  YFinance 실패, Alpha Vantage 시도...")
                stock_data = get_stock_data_alpha(ticker)
                time.sleep(12)  # Alpha Vantage 무료: 분당 5회 제한
            
            if stock_data:
                print(f"  현재가: ${stock_data['currentPrice']:.2f}")
                print(f"  전일종가: ${stock_data['previousClose']:.2f}")
                
                if update_notion_page(page_id, stock_data):
                    print("  ✓ 업데이트 완료")
                    updated_count += 1
                else:
                    error_count += 1
            else:
                print("  ✗ 데이터 가져오기 실패")
                error_count += 1
            
            # 대기 시간
            if i < len(pages) - 1:
                time.sleep(2)
            print()
        
        print("=== 업데이트 완료 ===")
        print(f"성공: {updated_count}개")
        print(f"실패: {error_count}개")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
