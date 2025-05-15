import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import time
import os # 파일 존재 여부 확인을 위해 import

# -----------------------------------------------------------------------------
# ★★★★★ 사용자가 반드시 확인하고 정확하게 수정해야 하는 부분 ★★★★★
# 이 부분을 수정하지 않으면 코드가 제대로 작동하지 않습니다.
# -----------------------------------------------------------------------------
# 1. 발급받은 API 인증키
SERVICE_KEY = "ccb968f0331e32757e63b288a8e3c1a84a65b58907184c906dc94e97abb90537"
BASE_URL = "http://211.237.50.150:7080/openapi/sample/xml/Grid_20240625000000000653_1"

TARGET_ITEM_MAPPING = {
    "배추": {"LARGE_CODE": "10", "MID_CODE": "01", "LARGENAME": "엽경채류", "MIDNAME": "배추"},
    "무": {"LARGE_CODE": "11", "MID_CODE": "01", "LARGENAME": "근채류", "MIDNAME": "무"}, 
    "마늘": {"LARGE_CODE": "12", "MID_CODE": "09", "LARGENAME": "조미채소류", "MIDNAME": "마늘"}, 
    "양파": {"LARGE_CODE": "12", "MID_CODE": "01", "LARGENAME": "조미채소류", "MIDNAME": "양파"}, 
    "대파": {"LARGE_CODE": "12", "MID_CODE": "02", "LARGENAME": "조미채소류", "MIDNAME": "대파"}, 
    "홍고추": {"LARGE_CODE": "12", "MID_CODE": "08", "LARGENAME": "조미채소류", "MIDNAME": "홍고추"},
    "깻잎": {"LARGE_CODE": "10", "MID_CODE": "11", "LARGENAME": "엽경채류", "MIDNAME": "깻잎"},
}
START_YEAR = 2021
END_YEAR = 2024
WHSAL_CODES_TO_USE = [
    '340101', # 천안
    '220001', # 대구 
    '240001', # 광주각화
    '320101', # 춘천
    '320201', # 원주
    '350101', # 전주
    '110001', # 서울가락
    '210009', # 부산반여
    '230003', # 인천삼산
    '250001', # 대전오정
    '310101', # 수원
    ] 

PARAM_SERVICE_KEY = 'serviceKey'
PARAM_START_INDEX = 'START_INDEX'
PARAM_END_INDEX = 'END_INDEX'
PARAM_RESPONSE_TYPE = 'TYPE'
PARAM_SALE_DATE = 'SALEDATE'
PARAM_MARKET_CODE = 'WHSALCD'
PARAM_LARGE_CLASS_CODE = 'LARGE'
PARAM_MID_CLASS_CODE = 'MID'

PARENT_OF_ITEMS_TAG = 'result'
ITEM_TAG = 'row'
TAG_SALE_DATE = 'SALEDATE'
TAG_MARKET_NAME = 'WHSALNAME'
TAG_CORP_NAME = 'CMPNAME'
TAG_LARGE_CAT_NAME = 'LARGENAME'
TAG_MID_CAT_NAME = 'MIDNAME'
TAG_SMALL_CAT_NAME = 'SMALLNAME'
TAG_AVG_PRICE = 'AVGAMT'
TAG_STD_UNIT = 'STD'
TAG_TOTAL_QTY = 'TOTQTY'
# -----------------------------------------------------------------------------

# 전체 데이터를 저장할 파일명
OUTPUT_CSV_FILENAME = f"collected_prices_{START_YEAR}_to_{END_YEAR}_all_items_markets_cumulative.csv"

# 이전에 저장된 파일이 있다면, 마지막으로 처리된 품목/시장/날짜를 확인하는 로직 (선택적 고급 기능)
# last_processed_info = load_last_progress_from_csv(OUTPUT_CSV_FILENAME) # 이런 함수를 만들어야 함

def get_total_count_from_xml(xml_root):
    total_count_node = xml_root.find('.//totalCnt')
    if total_count_node is not None and total_count_node.text is not None:
        try: return int(total_count_node.text)
        except ValueError: return 0
    return 0

def fetch_single_page_data(base_url, params_dict, item_key_for_log, date_for_log, market_for_log):
    page_items_data = []
    total_items_on_server = 0
    try:
        response = requests.get(base_url, params=params_dict, timeout=30)
        response.raise_for_status()
        if not response.content.strip():
            return page_items_data, total_items_on_server
        xml_string = response.content.decode('utf-8', errors='ignore')
        root = ET.fromstring(xml_string)
        api_status_code_node = root.find(f'.//{PARENT_OF_ITEMS_TAG}/code') if PARENT_OF_ITEMS_TAG else root.find('.//code')
        if api_status_code_node is not None and api_status_code_node.text not in ['00', 'INFO-000', 'INFO-00']:
            api_message_node = root.find(f'.//{PARENT_OF_ITEMS_TAG}/message') if PARENT_OF_ITEMS_TAG else root.find('.//message')
            error_message = api_message_node.text if api_message_node is not None else "Unknown API Error"
            print(f"    -> API Error for {item_key_for_log}, {date_for_log}, {market_for_log}: Code {api_status_code_node.text} - {error_message}")
            return page_items_data, total_items_on_server
        total_items_on_server = get_total_count_from_xml(root)
        items_container = root
        if PARENT_OF_ITEMS_TAG:
            items_container_node = root.find(PARENT_OF_ITEMS_TAG)
            if items_container_node is not None: items_container = items_container_node
        data_rows = items_container.findall(ITEM_TAG)
        if not data_rows and root.findall(f".//{ITEM_TAG}"): data_rows = root.findall(f".//{ITEM_TAG}")
        for item_node in data_rows:
            row = {
                'SALEDATE': item_node.findtext(TAG_SALE_DATE),
                'WHSALNAME': item_node.findtext(TAG_MARKET_NAME),
                'CMPNAME': item_node.findtext(TAG_CORP_NAME),
                'LARGENAME_API': item_node.findtext(TAG_LARGE_CAT_NAME),
                'MIDNAME_API': item_node.findtext(TAG_MID_CAT_NAME),
                'SMALLNAME': item_node.findtext(TAG_SMALL_CAT_NAME),
                'AVGAMT': item_node.findtext(TAG_AVG_PRICE),
                'STD': item_node.findtext(TAG_STD_UNIT),
                'TOTQTY': item_node.findtext(TAG_TOTAL_QTY),
            }
            if row.get('MIDNAME_API') and row.get('AVGAMT'):
                page_items_data.append(row)
    except requests.exceptions.Timeout:
        print(f"    -> Timeout for {item_key_for_log}, {date_for_log}, {market_for_log}")
    except requests.exceptions.HTTPError as errh:
        print(f"    -> Http Error for {item_key_for_log}, {date_for_log}, {market_for_log}: Code {errh.response.status_code}")
    except ET.ParseError as e:
        print(f"    -> XML Parse Error for {item_key_for_log}, {date_for_log}, {market_for_log}: {e}")
    except Exception as e:
        print(f"    -> Other error during page fetch for {item_key_for_log}, {date_for_log}, {market_for_log}: {e}")
    return page_items_data, total_items_on_server
# ---------------------------------------------------------------------------


def get_last_processed_info(filename):
    """CSV 파일에서 마지막으로 처리된 정보를 읽어옴"""
    if not os.path.exists(filename):
        return None, None, None # 파일 없으면 처음부터
    try:
        # CSV 파일의 마지막 줄만 효율적으로 읽기 위해 노력 (파일이 매우 클 경우 대비)
        # 실제로는 판다스로 전체를 읽고 마지막 행을 가져오는 것이 더 간단할 수 있음
        # 여기서는 마지막 줄을 직접 읽는 예시 (더 효율적인 방법도 있음)
        with open(filename, 'rb') as f: # 바이너리 모드로 열어서 seek 효율화
            try:  # 파일이 비어있을 경우를 대비
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
            except OSError: # 파일 시작 부분에 도달
                f.seek(0)
            last_line = f.readline().decode('utf-8-sig').strip()

        if last_line:
            # CSV 헤더와 데이터 순서가 일치한다고 가정하고 파싱
            # 예시: MY_TARGET_ITEM_NAME,SALEDATE,WHSALCD 순으로 저장했다고 가정하고,
            #       이 값들이 각각 어떤 컬럼에 있는지 확인 필요
            #       여기서는 MY_TARGET_ITEM_NAME, SALEDATE, WHSALCD가 있다고 가정
            #       그리고 DataFrame 저장 시 이 컬럼들이 있다고 가정
            #       가장 안전한 방법은 판다스로 읽어서 마지막 행을 가져오는 것
            df_check = pd.read_csv(filename, encoding='utf-8-sig')
            if not df_check.empty:
                last_row = df_check.iloc[-1]
                # 아래 컬럼명은 실제 CSV 파일의 컬럼명과 일치해야 함!
                last_item = last_row.get('MY_TARGET_ITEM_NAME')
                last_date_str = str(last_row.get(TAG_SALE_DATE)) # 날짜는 문자열로 가져와서 파싱
                last_market = str(last_row.get(PARAM_MARKET_CODE)) # CSV에 WHSALCD가 저장되어 있다고 가정

                if last_item and last_date_str and last_market:
                    # 날짜 형식 변환 시도 (YYYYMMDD 또는 YYYY-MM-DD HH:MM:SS 등 다양한 형식 고려)
                    try:
                        last_date_obj = datetime.strptime(last_date_str.split(' ')[0], '%Y-%m-%d') # 만약 YYYY-MM-DD 형식이라면
                    except ValueError:
                        try:
                            last_date_obj = datetime.strptime(last_date_str, '%Y%m%d') # YYYYMMDD 형식이라면
                        except ValueError:
                             print(f"Warning: Could not parse last date '{last_date_str}' from CSV. Starting fresh for this item/market.")
                             return None, None, None # 날짜 파싱 실패 시 처음부터
                    print(f"Resuming from: Item '{last_item}', Market '{last_market}', After Date '{last_date_obj.strftime('%Y%m%d')}'")
                    return last_item, last_date_obj, last_market
    except Exception as e:
        print(f"Error reading progress from {filename}: {e}. Starting fresh.")
    return None, None, None


# --- 메인 데이터 수집 로직 ---
MAX_ROWS_PER_REQUEST = 1000
processed_items_markets_dates = set() # (품목, 시장, 날짜) 조합으로 이미 저장된 데이터인지 확인용

# 이어하기 위한 시작 지점 결정
resume_item_key, resume_date_obj, resume_market_code = get_last_processed_info(OUTPUT_CSV_FILENAME)
start_processing_from_here = False
if resume_item_key is None: # 처음 시작이거나, 이어하기 정보 로드 실패
    start_processing_from_here = True

# 파일이 처음 생성되는 경우 헤더를 쓰기 위한 플래그
# 이어하기 시에는 CSV가 이미 존재하므로 header_written은 True로 시작할 가능성이 높음
header_written = os.path.exists(OUTPUT_CSV_FILENAME)


for target_item_key, item_codes in TARGET_ITEM_MAPPING.items():
    if not start_processing_from_here and target_item_key != resume_item_key:
        print(f"Skipping item: {target_item_key} (Resuming later)")
        continue

    large_code_val = item_codes['LARGE_CODE']
    mid_code_val = item_codes['MID_CODE']
    print(f"\n\n===== Processing Item: {target_item_key} (L:{large_code_val}, M:{mid_code_val}) =====")
    
    current_item_all_data_for_csv = [] # 현재 품목에 대한 데이터를 모아 한 번에 저장하기 위함

    for whsal_code_val in WHSAL_CODES_TO_USE:
        if not start_processing_from_here and target_item_key == resume_item_key and whsal_code_val != resume_market_code:
            print(f"Skipping market: {whsal_code_val} for item {target_item_key} (Resuming later)")
            continue
        
        print(f"\n  -- Market Code: {whsal_code_val} --")
        
        current_date_obj_loop_start = datetime(START_YEAR, 1, 1)
        if not start_processing_from_here and target_item_key == resume_item_key and whsal_code_val == resume_market_code and resume_date_obj:
            current_date_obj_loop_start = resume_date_obj + timedelta(days=1) # 마지막 성공 다음날부터
            print(f"  Resuming for market {whsal_code_val}, item {target_item_key} from date {current_date_obj_loop_start.strftime('%Y%m%d')}")
        
        start_processing_from_here = True # 이 지점부터는 모든 것을 새로 처리

        current_date_obj = current_date_obj_loop_start
        end_date_obj_loop = datetime(END_YEAR, 12, 31)

        while current_date_obj <= end_date_obj_loop:
            date_str = current_date_obj.strftime("%Y%m%d")
            
            # (품목, 시장, 날짜) 조합이 이미 처리되었는지 확인 (더 정교한 중복 방지용 - 선택적)
            # if (target_item_key, whsal_code_val, date_str) in processed_items_markets_dates:
            #     current_date_obj += timedelta(days=1)
            #     continue

            # 진행 상황 간략히 표시
            if current_date_obj.day == 1: # 매월 1일이 될 때마다
                 print(f"    Processing date: {date_str} for {target_item_key} at market {whsal_code_val}...")

            current_page_start = 1
            more_data_exists = True
            
            while more_data_exists:
                params = {
                    PARAM_SERVICE_KEY: SERVICE_KEY,
                    PARAM_START_INDEX: str(current_page_start),
                    PARAM_END_INDEX: str(current_page_start + MAX_ROWS_PER_REQUEST - 1),
                    PARAM_RESPONSE_TYPE: 'xml',
                    PARAM_SALE_DATE: date_str,
                    PARAM_MARKET_CODE: whsal_code_val,
                    PARAM_LARGE_CLASS_CODE: large_code_val,
                    PARAM_MID_CLASS_CODE: mid_code_val,
                }

                page_data, total_count = fetch_single_page_data(BASE_URL, params, target_item_key, date_str, whsal_code_val)
                
                if page_data:
                    for record in page_data:
                        record['MY_TARGET_ITEM_NAME'] = target_item_key
                        record['MY_TARGET_LARGENAME'] = item_codes['LARGENAME']
                        record['MY_TARGET_MIDNAME'] = item_codes['MIDNAME']
                        # WHSALCD도 추가해서 어떤 시장 데이터인지 명시
                        record[PARAM_MARKET_CODE] = whsal_code_val 
                    current_item_all_data_for_csv.extend(page_data)
                
                if total_count == 0 or not page_data or current_page_start + len(page_data) >= total_count :
                    more_data_exists = False
                else:
                    current_page_start += MAX_ROWS_PER_REQUEST
                
                if more_data_exists: time.sleep(0.1)
            
            # processed_items_markets_dates.add((target_item_key, whsal_code_val, date_str)) # 처리된 조합 기록 (선택적)
            current_date_obj += timedelta(days=1)
        
        time.sleep(0.2) # 시장 변경 시 딜레이

    # --- 한 품목에 대한 모든 데이터 수집 후 CSV 파일에 저장 ---
    if current_item_all_data_for_csv:
        item_df = pd.DataFrame(current_item_all_data_for_csv)
        print(f"\n  Saving data for item: {target_item_key}. Records: {len(item_df)}")
        
        # CSV 파일에 저장 (이어쓰기 모드, 헤더는 처음 한 번만)
        if not header_written:
            item_df.to_csv(OUTPUT_CSV_FILENAME, index=False, mode='w', encoding='utf-8-sig')
            header_written = True
        else:
            item_df.to_csv(OUTPUT_CSV_FILENAME, index=False, mode='a', header=False, encoding='utf-8-sig')
        
        print(f"  Data for {target_item_key} appended to {OUTPUT_CSV_FILENAME}")
        current_item_all_data_for_csv = [] # 다음 품목을 위해 리스트 비우기
    else:
        print(f"  No new data collected for item: {target_item_key} in this run.")


print("\n\n===== All Data Collection Finished or Resumed! =====")

# 최종 데이터 확인 및 후처리 (필요시 주석 해제)
# try:
#     final_df_check = pd.read_csv(OUTPUT_CSV_FILENAME, encoding='utf-8-sig')
#     print("\n--- Final CSV Content Sample (First 5 rows) ---")
#     print(final_df_check.head())
#     print(f"Total records in CSV: {len(final_df_check)}")
    
#     # 데이터 타입 변환
#     numeric_cols = [TAG_AVG_PRICE, TAG_TOTAL_QTY]
#     for col in numeric_cols:
#         if col in final_df_check.columns:
#             final_df_check[col] = pd.to_numeric(final_df_check[col], errors='coerce')
#     if TAG_SALE_DATE in final_df_check.columns:
#         final_df_check[TAG_SALE_DATE] = pd.to_datetime(final_df_check[TAG_SALE_DATE], format='%Y%m%d', errors='coerce')
    
#     final_df_check.info()
# except FileNotFoundError:
#     print(f"\nOutput file {OUTPUT_CSV_FILENAME} not found.")
# except Exception as e:
#     print(f"\nError reading final CSV: {e}")