import pandas as pd

def returnAveragePrice(df):
    # 1. 같은 품목끼리 그룹화
    # 2. 같은 품목 내에서 date 별로 그룹화
    # 3. 같은 품목, date 내에서 지역별로 그룹화
    # 4. 같은 그룹끼리 평균
    avg = df.groupby(
        by=['MY_TARGET_ITEM_NAME','date','countyname_api'],
        sort=False
    )['price_api'].mean().reset_index()
    
    pivot_column_name = 'MY_TARGET_ITEM_NAME'

    try:
        avg = avg.pivot_table(index = ['date','countyname_api'],
                                        columns = pivot_column_name,
                                        values = 'price_api',
                                        aggfunc = 'mean')
    except KeyError as e:
        print(f"Check Column's name")
    except Exception as e:
        print(f"Other error happened")
        
    avg.columns.name = None
    avg = avg.reset_index()
    avg = avg.round(2)
    
    column_order = [
        'date',
        'countyname_api',
        '배추',
        '무',
        '마늘',
        '양파',
        '대파',
        '홍고추',
        '깻잎'
    ]

    avg = avg[column_order]
    
    item_en = {
        'date':'date',
        'countyname_api':'region',
        '배추': 'cabbage',
        '무': 'radish',
        '양파': 'onion',
        '마늘': 'garlic',
        '대파': 'daikon',
        '홍고추': 'cilantror',
        '깻잎': 'artichoke'
    }
    
    avg.columns = avg.columns.map(item_en)
    
    region_en = {
        '서울': 'Seoul',
        '부산': 'Busan',
        '대구': 'Daegu',
        '인천': 'Incheon',
        '광주': 'Gwangju',
        '대전': 'Daejeon',
        '울산': 'Ulsan',
        '세종': 'Sejong',
        '수원': 'Suwon',
        '성남': 'Seongnam',
        '고양': 'Goyang',
        '용인': 'Yongin',
        '춘천': 'Chuncheon',
        '강릉': 'Gangneung',
        '청주': 'Cheongju',
        '천안': 'Cheonan',
        '전주': 'Jeonju',
        '순천': 'Suncheon',
        '포항': 'Pohang',
        '안동': 'Andong',
        '창원': 'Changwon',
        '김해': 'Gimhae',
        '제주': 'Jeju'
    }
    
    avg['region'] = avg['region'].replace(region_en)
    
    return avg
    

if __name__ == "__main__":
    testCSV = "kamis_prices_2025_verification_no_duplicates.csv"

    test_df = pd.read_csv(testCSV)
    
    test_avg_df = returnAveragePrice(test_df)

    test_avg_df.to_csv('average_price_by_dateAndRegion.csv', encoding='utf-8')


