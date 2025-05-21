# SE_Project

## KAMIS와 data.go.kr에서 API KEY를 받아와 csv 파일로 농산물(배추, 무, 마늘, 양파, 대파, 홍고추, 깻잎)의 데이터를 받아옴

- 각각의 API 데이터를 분석하기 쉬운 형태로 변환 (train_set.csv & test_set.csv)

- SE_Project.ipynb 파일에서 데이터 분석을 진행
  - EDA 과정 진행중이고 항목 별 Nan 값 존재 확인
  - Nan 값을 월 평균 농산물의 가격으로 대체할 예정

- EDA까지 진행 완료

- 데이터 분석
    - 시계열 데이터 분석 모델인 Prophet을 사용한 분석, 시각화까지 완료
    - ML 모델을 위한 Feature engineering 완료, 분석 시작
