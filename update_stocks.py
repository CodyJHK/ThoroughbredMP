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
        
        # 현재가 가져오기 - 여러 필드 시도
        current_price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('price', 0)
        
        # 현재가가 없으면 최근 종가 사용
        if current_price == 0:
            hist = stock.history(period="1d")
            if not hist.empty:
                current_price = float(hist['Close'].iloc[-1])
        
        # 전일 종가
        previous_close = info.get('previousClose') or info.get('regularMarketPreviousClose', 0)
        
        # 시가총액 (억 단위로 변환)
        market_cap = info.get('marketCap', 0)
        if market_cap > 0:
            market_cap = round(market_cap / 100000000)  # 억 단위
        
        data = {
            'currentPrice': current_price,
            'previousClose': previous_close,
            'marketCap': market_cap
        }
        
        print(f"  현재가: ${current_price:.2f}")
        print(f"  전일종가: ${previous_close:.2f}")
        print(f"  시가총액: {market_cap}억")
        
        return data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {str(e)}")
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
            "시가총액": {"number": stock_data['marketCap']},
            "업데이트시간": {"date": {"start": datetime.now(KST).isoformat()}}
        }
        
        notion.pages.update(
            page_id=page_id,
            properties=properties
        )
        
        print(f"  ✓ 업데이트 완료 (변동률: {change_percent:+.2f}%)")
        return True
    except Exception as e:
        print(f"Error updating page {page_id}: {str(e)}")
        return False

def main():
    """메인 실행 함수"""
    print(f"=== 주식 가격 업데이트 시작 ===")
    print(f"시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}\n")
    
    try:
        # 데이터베이스에서 모든 페이지 가져오기
        response = notion.databases.query(database_id=DATABASE_ID)
        pages = response.get('results', [])
        
        if not pages:
            print("데이터베이스에 항목이 없습니다.")
            return
        
        print(f"총 {len(pages)}개 종목 발견\n")
        
        updated_count = 0
        error_count = 0
        skipped_count = 0
        
        for page in pages:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # 종목명 가져오기 (티커 대신)
            title_prop = properties.get('티커', {})
            if title_prop.get('type') == 'title' and title_prop.get('title'):
                ticker = title_prop['title'][0]['text']['content'].strip().upper()
                print(f"처리 중: {ticker}")
            else:
                print(f"티커를 찾을 수 없습니다 (페이지 ID: {page_id})")
                skipped_count += 1
                continue
            
            # 주식 데이터 가져오기
            stock_data = get_stock_data(ticker)
            
            if stock_data and stock_data['currentPrice'] > 0:
                # Notion 페이지 업데이트
                if update_notion_page(page_id, stock_data):
                    updated_count += 1
                else:
                    error_count += 1
            else:
                print(f"  ✗ 데이터를 가져올 수 없습니다")
                error_count += 1
            
            print()  # 줄바꿈
        
        # 결과 요약
        print("=== 업데이트 완료 ===")
        print(f"성공: {updated_count}개")
        print(f"실패: {error_count}개")
        print(f"건너뜀: {skipped_count}개")
        print(f"총 처리: {len(pages)}개")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
