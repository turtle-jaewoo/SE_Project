import pandas as pd

def returnAveragePrice(df):
    # 1. 같은 품목끼리 그룹화
    # 2. 같은 품목 내에서 date 별로 그룹화
    # 3. 같은 품목, date 내에서 지역별로 그룹화
    # 4. 같은 그룹끼리 평균
    avg = df.groupby(
        by=['MY_TARGET_ITEM_NAME','date','countyname_api'],
        sort=False
    )['price_api'].mean().round().astype(int).reset_index()

    avg.rename(
        columns={'price_api': 'avg_price'},
        inplace=True
        )
    
    avg.set_index('date',inplace=True)
    
    return avg

if __name__ == "__main__":
    testCSV = "kamis_prices_2025_verification_no_duplicates.csv"

    test_df = pd.read_csv(testCSV)
    
    
    numOfRegion = len(test_df['countyname_api'].unique())
    
    test_avg_df = returnAveragePrice(test_df)
    
    item_en = {
        '배추': 'Cabbage',
        '무': 'Radish',
        '양파': 'Onion',
        '마늘': 'Garlic',
        '대파': 'Green Onion',
        '홍고추': 'Red Pepper',
        '깻잎': 'Perilla Leaf'
    }
    test_avg_df['MY_TARGET_ITEM_NAME'] = test_avg_df['MY_TARGET_ITEM_NAME'].replace(item_en)
    
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
    
    test_avg_df['countyname_api'] = test_avg_df['countyname_api'].replace(region_en)
    
    #print(test_avg_df.head(len(test_df['countyname_api'].unique()) * 2))
    #print(len(test_avg_df['countyname_api'].unique()) == numOfRegion)
    #print(test_avg_df.index.max(), test_avg_df.index.min())
    
    test_avg_df.to_csv('average_price_by_dateAndRegion.csv', encoding='utf-8')
    
    def showGraph():
        import matplotlib.pyplot as plt
        
        foodList = test_avg_df['MY_TARGET_ITEM_NAME'].unique()[0:4]
        regionList = test_avg_df['countyname_api'].unique()[0:4]
        
        fig, axes = plt.subplots(len(foodList), 1, figsize=(12,12), squeeze=False)
        
        xticks = [test_avg_df.index.min(), test_avg_df.index.max()]
        
        for row ,food in enumerate(foodList):
            ax = axes[row][0]
            for col, region in enumerate(regionList):
                
                set = test_avg_df[(test_avg_df['MY_TARGET_ITEM_NAME'] == food) & (test_avg_df['countyname_api'] == region)]
                
                ax.plot(set.index, set['avg_price'], label=region)
                ax.set_title(f'{food}')
                ax.set_xticks(xticks)
                ax.legend()
                
                
        plt.tight_layout()
        plt.show()
    
    showGraph()

