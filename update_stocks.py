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

if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN and DATABASE_ID environment variables must be set")
    sys.exit(1)

notion = Client(auth=NOTION_TOKEN)
KST = pytz.timezone('Asia/Seoul')

def get_stock_data_finnhub(ticker):
    """Finnhub API 사용 (무료)"""
    try:
        # Finnhub 무료 API 키
        api_key = "cqqh9k9r01qvs3jmang0cqqh9k9r01qvs3jmangd"  # 공개 테스트 키
        
        url = f"https://finnhub.io/api/v1/quote"
        params = {
            'symbol': ticker,
            'token': api_key
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'c' in data and data['c'] > 0:
            return {
                'currentPrice': float(data['c']),  # 현재가
                'previousClose': float(data['pc']),  # 전일 종가
                'marketCap': 0
            }
        return None
    except Exception as e:
        print(f"  Finnhub 오류: {str(e)}")
        return None

def get_stock_data_twelve(ticker):
    """Twelve Data API 사용 (무료)"""
    try:
        # Twelve Data 무료 키 (분당 8회)
        api_key = "demo"  # 또는 무료 가입 후 받은 키
        
        url = f"https://api.twelvedata.com/quote"
        params = {
            'symbol': ticker,
            'apikey': api_key
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'close' in data:
            return {
                'currentPrice': float(data['close']),
                'previousClose': float(data.get('previous_close', data['close'])),
                'marketCap': 0
            }
        return None
    except:
        return None

def get_stock_data_polygon(ticker):
    """Polygon.io API 사용"""
    try:
        # 어제 날짜 계산
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        api_key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"  # 무료 키
        
        # 이전 종가
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{yesterday}"
        params = {'apiKey': api_key}
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'close' in data:
            return {
                'currentPrice': float(data['close']),
                'previousClose': float(data.get('open', data['close'])),
                'marketCap': 0
            }
        return None
    except:
        return None

def update_notion_page(page_id, stock_data):
    """Notion 페이지 업데이트"""
    try:
        # 변동률 계산
        if stock_data['previousClose'] > 0:
            change_percent = round((stock_data['currentPrice'] - stock_data['previousClose']) / stock_data['previousClose'] * 100, 2)
        else:
            change_percent = 0
            
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
        
        print(f"  ✓ 업데이트 완료 (변동: {change_percent:+.2f}%)")
        return True
    except Exception as e:
        print(f"  Notion 오류: {str(e)}")
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
            
            # 여러 API 순차적으로 시도
            stock_data = None
            
            # 1. Finnhub 시도
            print("  Finnhub API 시도...")
            stock_data = get_stock_data_finnhub(ticker)
            
            # 2. 실패시 Twelve Data
            if not stock_data:
                print("  Twelve Data API 시도...")
                stock_data = get_stock_data_twelve(ticker)
                time.sleep(1)
            
            # 3. 실패시 Polygon
            if not stock_data:
                print("  Polygon API 시도...")
                stock_data = get_stock_data_polygon(ticker)
            
            if stock_data:
                print(f"  현재가: ${stock_data['currentPrice']:.2f}")
                print(f"  전일종가: ${stock_data['previousClose']:.2f}")
                
                if update_notion_page(page_id, stock_data):
                    updated_count += 1
                else:
                    error_count += 1
            else:
                print("  ✗ 모든 API 실패")
                error_count += 1
            
            # API 제한 회피
            if i < len(pages) - 1:
                time.sleep(2)
            print()
        
        print("=== 업데이트 완료 ===")
        print(f"성공: {updated_count}개")
        print(f"실패: {error_count}개")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
