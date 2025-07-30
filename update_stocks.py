import os
import sys
from notion_client import Client
import yfinance as yf
from datetime import datetime
import pytz

# 환경 변수에서 설정 가져오기
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
DATABASE_ID = os.environ.get('DATABASE_ID')

if not NOTION_TOKEN or not DATABASE_ID:
    print("Error: NOTION_TOKEN and DATABASE_ID environment variables must be set")
    sys.exit(1)

# Notion 클라이언트 초기화
notion = Client(auth=NOTION_TOKEN)

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

def get_stock_data(ticker):
    """yfinance를 사용해 주식 데이터 가져오기"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # 기본값 설정
        data = {
            'currentPrice': info.get('currentPrice', info.get('regularMarketPrice', 0)),
            'previousClose': info.get('previousClose', info.get('regularMarketPreviousClose', 0)),
            'fiftyTwoWeekHigh': info.get('fiftyTwoWeekHigh', 0),
            'fiftyTwoWeekLow': info.get('fiftyTwoWeekLow', 0),
            'marketCap': info.get('marketCap', 0)
        }
        
        # 현재가가 없으면 최근 종가 사용
        if data['currentPrice'] == 0:
            hist = stock.history(period="1d")
            if not hist.empty:
                data['currentPrice'] = hist['Close'].iloc[-1]
        
        return data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {str(e)}")
        return None

def update_notion_page(page_id, stock_data):
    """Notion 페이지 업데이트"""
    try:
        properties = {
            "현재가": {"number": stock_data['currentPrice']},
            "전일종가": {"number": stock_data['previousClose']},
            "52주최고": {"number": stock_data['fiftyTwoWeekHigh']},
            "52주최저": {"number": stock_data['fiftyTwoWeekLow']},
            "시가총액": {"number": stock_data['marketCap']},
            "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}}
        }
        
        notion.pages.update(
            page_id=page_id,
            properties=properties
        )
        return True
    except Exception as e:
        print(f"Error updating page {page_id}: {str(e)}")
        return False

def main():
    """메인 실행 함수"""
    print(f"Starting stock price update at {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}")
    
    try:
        # 데이터베이스에서 모든 페이지 가져오기
        response = notion.databases.query(database_id=DATABASE_ID)
        pages = response.get('results', [])
        
        if not pages:
            print("No pages found in the database")
            return
        
        updated_count = 0
        error_count = 0
        
        for page in pages:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # 티커 심볼 가져오기
            ticker_prop = properties.get('티커', {})
            if ticker_prop.get('type') == 'rich_text' and ticker_prop.get('rich_text'):
                ticker = ticker_prop['rich_text'][0]['text']['content']
            else:
                # 티커가 없으면 종목명에서 추출 시도
                title_prop = properties.get('종목명', {})
                if title_prop.get('type') == 'title' and title_prop.get('title'):
                    title = title_prop['title'][0]['text']['content']
                    print(f"No ticker found for {title}, skipping...")
                    continue
                else:
                    print(f"No ticker or title found for page {page_id}, skipping...")
                    continue
            
            print(f"Processing {ticker}...")
            
            # 주식 데이터 가져오기
            stock_data = get_stock_data(ticker)
            
            if stock_data:
                # Notion 페이지 업데이트
                if update_notion_page(page_id, stock_data):
                    print(f"✓ Successfully updated {ticker}")
                    updated_count += 1
                else:
                    print(f"✗ Failed to update {ticker}")
                    error_count += 1
            else:
                print(f"✗ Failed to fetch data for {ticker}")
                error_count += 1
        
        print(f"\nUpdate complete!")
        print(f"Updated: {updated_count} pages")
        print(f"Errors: {error_count} pages")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()