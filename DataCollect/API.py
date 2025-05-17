import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# -----------------------------------------------------------------------------
# API KEY 값과 CERT ID 값은 직접 KAMIS 홈페이지에서 요청해서 받아야함 !
# -----------------------------------------------------------------------------
KAMIS_API_KEY = "####"
KAMIS_CERT_ID = "####"
KAMIS_BASE_URL = "http://www.kamis.or.kr/service/price/xml.do"
KAMIS_ACTION = "periodProductList"

TARGET_ITEMS_KAMIS = {
    "배추": {
        "부류코드": "200", "품목코드": "211",
        "품종들": [
            {"코드": "06", "이름": "월동배추"},
        ], "우리가_쓸_대표명": "배추"
    },
    "무": {
        "부류코드": "200", "품목코드": "231",
        "품종들": [
            {"코드": "06", "이름": "월동무"},
        ], "우리가_쓸_대표명": "무"
    },
    "양파": {
        "부류코드": "200", "품목코드": "245",
        "품종들": [{"코드": "00", "이름": "양파(일반)"}],
    },
    "깐마늘": {
        "부류코드": "200", "품목코드": "258",
        "품종들": [
            {"코드": "01", "이름": "깐마늘(국산)"},
        ], "우리가_쓸_대표명": "마늘"
    },
    "대파": {
        "부류코드": "200", "품목코드": "246",
        "품종들": [{"코드": "00", "이름": "대파(일반)"}],
    },
    "홍고추": {
        "부류코드": "200", "품목코드": "243",
        "품종들": [{"코드": "00", "이름": "홍고추(일반)"}],
    },
    "깻잎": {
        "부류코드": "200", "품목코드": "253",
        "품종들": [{"코드": "00", "이름": "깻잎(일반)"}],
    },
}
START_DATE_REQ = "2025-01-01"
END_DATE_REQ = "2025-04-30"

PRODUCT_CLS_CODE = '01'
PRODUCT_RANK_CODE = '04'
COUNTRY_CODES_TO_USE = ['1101']
CONVERT_KG_YN = 'Y'
OUTPUT_CSV_FILENAME_KAMIS = "kamis_prices_2025_verification_no_duplicates.csv"

# API 요청 시 사용할 파라미터 이름들
PARAM_API_KEY = 'p_cert_key'
PARAM_API_ID = 'p_cert_id'  
PARAM_RETURN_TYPE = 'p_returntype'
PARAM_START_DAY = 'p_startday'
PARAM_END_DAY = 'p_endday'
PARAM_ITEM_CATEGORY_CODE = 'p_itemcategorycode'
PARAM_ITEM_CODE = 'p_itemcode'
PARAM_KIND_CODE = 'p_kindcode'
PARAM_PRODUCT_RANK_CODE = 'p_productrankcode'
PARAM_COUNTY_CODE = 'p_countycode'
PARAM_CONVERT_KG_YN = 'p_convert_kg_yn'
PARAM_PRODUCT_CLS_CODE = 'p_productclscode'
ACTION_PARAM = 'action'

# API 응답 XML의 태그명
TAG_DATA_PARENT = 'data'
TAG_ERROR_CODE = 'error_code'
TAG_DATA_ITEM = 'item'
TAG_ITEM_NAME_API = 'itemname'
TAG_KIND_NAME_API = 'kindname'
TAG_COUNTY_NAME_API = 'countyname'
TAG_MARKET_NAME_API = 'marketname'
TAG_YYYY_API = 'yyyy'
TAG_REGDAY_API = 'regday'
TAG_PRICE_API = 'price'
# -----------------------------------------------------------------------------

def parse_kamis_xml_for_collection(xml_string, target_item_name_main_log, kind_name_req_log):
    parsed_data_list = []
    api_error_code_from_xml = None
    try:
        if not xml_string or xml_string.startswith("<!DOCTYPE html"):
            api_error_code_from_xml = "HTML_ERROR_OR_EMPTY"
            return pd.DataFrame(parsed_data_list), api_error_code_from_xml

        root = ET.fromstring(xml_string)
        data_node = root.find(TAG_DATA_PARENT)

        if data_node is not None:
            error_code_node = data_node.find(TAG_ERROR_CODE)
            if error_code_node is not None and error_code_node.text:
                api_error_code_from_xml = error_code_node.text.strip()
                if api_error_code_from_xml != '000':
                    return pd.DataFrame(parsed_data_list), api_error_code_from_xml

            price_items = data_node.findall(TAG_DATA_ITEM)
            if not price_items and api_error_code_from_xml == '000':
                return pd.DataFrame(parsed_data_list), api_error_code_from_xml

            for item_data_node in price_items:
                date_yyyy = item_data_node.findtext(TAG_YYYY_API)
                date_regday = item_data_node.findtext(TAG_REGDAY_API)
                full_date_str = f"{date_yyyy}-{date_regday.replace('/', '-')}" if date_yyyy and date_regday else None
                price_val = item_data_node.findtext(TAG_PRICE_API)
                current_county_name = item_data_node.findtext(TAG_COUNTY_NAME_API)

                if current_county_name in ['평균', '평년']:
                    continue

                row = {
                    'MY_TARGET_ITEM_NAME': target_item_name_main_log,
                    'itemname_api': item_data_node.findtext(TAG_ITEM_NAME_API),
                    'kindname_api': item_data_node.findtext(TAG_KIND_NAME_API),
                    'countyname_api': current_county_name,
                    'marketname_api': item_data_node.findtext(TAG_MARKET_NAME_API),
                    'date': full_date_str,
                    'price_api': str(price_val).replace(',', '') if price_val and str(price_val) != '-' else None,
                    'unit_api': "1kg" if CONVERT_KG_YN == 'Y' else "조사단위",
                    'REQUESTED_KINDNAME': kind_name_req_log
                }
                if row.get('price_api'):
                    parsed_data_list.append(row)
        else:
            api_error_code_from_xml = "NO_DATA_TAG"

    except ET.ParseError: api_error_code_from_xml = "PARSE_ERROR"
    except Exception: api_error_code_from_xml = "UNKNOWN_PARSING_ERROR"
    
    return pd.DataFrame(parsed_data_list), api_error_code_from_xml

def load_already_collected_keys(filename):
    """CSV에서 (대표품목명, 요청품종명, 요청시장코드, 날짜) 조합 set 로드"""
    keys_set = set()
    if not os.path.exists(filename):
        print(f"'{filename}' not found. Starting fresh collection.")
        return keys_set
    try:
        # CSV 파일에 실제 저장된 컬럼명으로 usecols 지정!
        # MY_TARGET_ITEM_NAME, REQUESTED_KINDNAME, REQ_COUNTRYCODE, date 컬럼이 있다고 가정
        df = pd.read_csv(filename, encoding='utf-8-sig', dtype=str,
                         usecols=['MY_TARGET_ITEM_NAME', 'REQUESTED_KINDNAME', 'REQ_COUNTRYCODE', 'date'])
        for _, row in df.iterrows():
            # DataFrame에서 값을 가져올 때 NaN이 아닌지 확인
            if pd.notna(row['MY_TARGET_ITEM_NAME']) and \
               pd.notna(row['REQUESTED_KINDNAME']) and \
               pd.notna(row['REQ_COUNTRYCODE']) and \
               pd.notna(row['date']):
                key = (row['MY_TARGET_ITEM_NAME'], row['REQUESTED_KINDNAME'], row['REQ_COUNTRYCODE'], row['date'])
                keys_set.add(key)
        if keys_set:
             print(f"Loaded {len(keys_set)} existing unique data keys from '{filename}'.")
    except FileNotFoundError: # 이 부분은 os.path.exists로 이미 처리됨
        print(f"File '{filename}' not found during key loading. Starting fresh.")
    except KeyError as e:
        print(f"KeyError while loading keys from '{filename}': Column {e} not found. CSV format might have changed. Starting fresh.")
    except Exception as e:
        print(f"Error loading existing data keys from '{filename}': {e}. Starting fresh.")
    return keys_set

# --- 메인 데이터 수집 로직 ---
# ★★★ already_collected_keys를 스크립트 시작 시 한 번만 로드! ★★★
already_collected_keys = load_already_collected_keys(OUTPUT_CSV_FILENAME_KAMIS)
header_already_written_kamis = os.path.exists(OUTPUT_CSV_FILENAME_KAMIS) # 헤더 존재 여부도 여기서 결정
total_new_records_this_session = 0

print(f"Starting KAMIS data collection. Target period: {START_DATE_REQ} to {END_DATE_REQ}.")
if not header_already_written_kamis: print(f"Output file '{OUTPUT_CSV_FILENAME_KAMIS}' will be created with headers.")
else: print(f"Output file '{OUTPUT_CSV_FILENAME_KAMIS}' exists. Will append new data, skipping {len(already_collected_keys)} already collected entries.")

# 전체 데이터를 모을 리스트 (메모리에 모든 걸 다 담지 않고, 품목별로 처리 후 파일에 쓰는 방식 유지)
# all_kamis_data_df_accumulator = [] # 이 변수는 이제 품목별로 사용

for main_item_name_key, item_details_value in TARGET_ITEMS_KAMIS.items():
    item_category_code = item_details_value['부류코드']
    item_code_val = item_details_value['품목코드']
    target_item_name_for_df = item_details_value['우리가_쓸_대표명']
    
    new_data_for_this_main_item = [] # 현재 대표 품목에 대한 '새로운' 데이터만 모으는 리스트

    print(f"\n===== Processing Main Item (KAMIS): {target_item_name_for_df} =====")

    for kind_info_item in item_details_value['품종들']:
        kind_code_api = kind_info_item['코드']
        kind_name_log = kind_info_item['이름']

        for country_code_api in COUNTRY_CODES_TO_USE:
            # print(f"  -- Kind: {kind_name_log} (Code: {kind_code_api}), Market: {country_code_api} --") # 로그 간소화
            
            current_date_obj_loop = datetime.strptime(START_DATE_REQ, "%Y-%m-%d")
            end_date_obj_loop_limit = datetime.strptime(END_DATE_REQ, "%Y-%m-%d")

            # 이 (품목-품종-시장) 조합에 대해 월별로 진행 상황 표시
            # print_monthly_log = True 

            while current_date_obj_loop <= end_date_obj_loop_limit:
                date_str_for_api_call = current_date_obj_loop.strftime("%Y-%m-%d")
                
                # ★★★ 중복 체크 키 생성 및 확인 ★★★
                data_key_to_check = (target_item_name_for_df, kind_name_log, country_code_api, date_str_for_api_call)

                if data_key_to_check in already_collected_keys:
                    # print(f"    Skipping {date_str_for_api_call} for {target_item_name_for_df}-{kind_name_log}, market {country_code_api} (already collected).") # 너무 많은 로그
                    current_date_obj_loop += timedelta(days=1)
                    continue # 이미 수집된 데이터면 이 API 호출은 건너뛰기
                
                # 로그는 월 바뀔 때 한 번만 또는 루프 시작 시
                if current_date_obj_loop.day == 1 or current_date_obj_loop == datetime.strptime(START_DATE_REQ, "%Y-%m-%d"):
                     print(f"    Processing date: {date_str_for_api_call} for {target_item_name_for_df}-{kind_name_log} @ {country_code_api} ... (Session new: {total_new_records_this_session})")
                     # print_monthly_log = False


                params = {
                    PARAM_API_KEY: KAMIS_API_KEY, PARAM_API_ID: KAMIS_CERT_ID,
                    PARAM_RETURN_TYPE: 'xml', PARAM_START_DAY: date_str_for_api_call,
                    PARAM_END_DAY: date_str_for_api_call, PARAM_ITEM_CATEGORY_CODE: item_category_code,
                    PARAM_ITEM_CODE: item_code_val, PARAM_KIND_CODE: kind_code_api,
                    PARAM_PRODUCT_RANK_CODE: PRODUCT_RANK_CODE, PARAM_COUNTY_CODE: country_code_api,
                    PARAM_CONVERT_KG_YN: CONVERT_KG_YN, ACTION_PARAM: KAMIS_ACTION,
                    PARAM_PRODUCT_CLS_CODE: PRODUCT_CLS_CODE
                }

                response_text = ""
                page_df = pd.DataFrame()
                api_error_code_returned = None
                try:
                    response = requests.get(KAMIS_BASE_URL, params=params, timeout=30)
                    response.raise_for_status()
                    try: response_text = response.content.decode('utf-8')
                    except UnicodeDecodeError: response_text = response.content.decode('euc-kr', errors='replace')
                    
                    page_df, api_error_code_returned = parse_kamis_xml_for_collection(response_text, target_item_name_for_df, kind_name_log)
                    
                    if not page_df.empty:
                        page_df['REQ_ITEMCATCODE'] = item_category_code
                        page_df['REQ_ITEMCODE'] = item_code_val
                        page_df['REQ_KINDCODE'] = kind_code_api
                        page_df['REQ_RANKCODE'] = PRODUCT_RANK_CODE
                        page_df['REQ_COUNTRYCODE'] = country_code_api # CSV에 저장될 컬럼명 (이어하기 키와 일치)
                        new_data_for_this_main_item.extend(page_df.to_dict('records'))
                        total_new_records_this_session += len(page_df)
                        # ★★★ 새로 수집된 키도 already_collected_keys에 실시간으로 추가 (중요!) ★★★
                        for _, new_row_data in page_df.iterrows():
                            new_data_key = (new_row_data['MY_TARGET_ITEM_NAME'], new_row_data['REQUESTED_KINDNAME'], new_row_data['REQ_COUNTRYCODE'], new_row_data['date'])
                            already_collected_keys.add(new_data_key)
                    elif api_error_code_returned and api_error_code_returned != '000':
                        # print(f"        -> KAMIS API Error {api_error_code_returned} for {date_str_for_api_call}.") # 로그 간소화
                        pass
                        
                except Exception as e: 
                    # print(f"        -> Request/Network Error for {target_item_name_for_df}-{kind_name_log} on {date_str_for_api_call}, market {country_code_api}: {e}") # 로그 간소화
                    pass # 네트워크 에러 등은 일단 건너뛰고 다음 날짜로
                
                time.sleep(0.1) # API 호출 간격 조금 줄임 (테스트용, 실제론 0.2~0.3초 권장)
                current_date_obj_loop += timedelta(days=1)
            
            # time.sleep(0.2) # 로그 간소화
        # time.sleep(0.2) # 로그 간소화

    # --- 한 대표 품목(main_item_name)에 대한 모든 '새로운' 데이터 수집 후 CSV 파일에 저장 ---
    if new_data_for_this_main_item:
        item_df_to_save = pd.DataFrame(new_data_for_this_main_item)
        print(f"\n  Appending {len(item_df_to_save)} new records for Main Item: {target_item_name_for_df} to CSV.")
        
        # mode='a'는 파일이 없으면 새로 만들고, 있으면 추가함. header는 파일 존재 여부로 결정.
        if not header_already_written_kamis:
            item_df_to_save.to_csv(OUTPUT_CSV_FILENAME_KAMIS, index=False, mode='w', encoding='utf-8-sig')
            header_already_written_kamis = True # 다음부터는 헤더 안 쓰도록 플래그 변경
        else:
            item_df_to_save.to_csv(OUTPUT_CSV_FILENAME_KAMIS, index=False, mode='a', header=False, encoding='utf-8-sig')
        
        print(f"  Data for {target_item_name_for_df} (KAMIS) updated in {OUTPUT_CSV_FILENAME_KAMIS}")
    else:
        print(f"\n  No new data collected for Main Item (KAMIS): {target_item_name_for_df} in this run (all data likely already exists or no data found from API).")
    time.sleep(0.3) # 다음 대표 품목 처리 전 딜레이

print(f"\n\n===== All KAMIS Data Collection Finished or Resumed! Total new records added this session: {total_new_records_this_session} =====")
