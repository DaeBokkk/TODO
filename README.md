# TODO
# 데이터 수집 파트

 bitKinds.py: 빅카인즈에서 경기도권의 부동산 뉴스 정보를 매 23:58분마다 자동 크롤링 
 if __name__ == "__main__": 아래에 2가지 사용 방법 정리해놓음(txt 파일 도출 방식 or Langchain Document 객체로 전달해주는 방식) 자동화를 생각하면 txt 도출 방식이 적절해보임

 apt_list.py: 아파트 매매 실거래가 api 수집 파일
 위와 마찬가지로 메인에 실행 로직 짜여져있음

 law_go.py: 법령 크롤링 모듈

<h2>파일 종류</h2>

/txts: 수집한 데이터들을 종류별로 정리해놓은 디렉토리

/apt_real_estate: 아파트 매매, 전월세 실거래가 정리 파일
(apt_data_*.txt : 아파트 매매, apt_rent_data_*.txt : 아파트 전/월세)

/land_real_estate: 토지 매매 실거래가 정리 파일
(land_data_*.txt : 토지 매매)

/officetel_real_estate: 오피스텔 매매, 전월세 실거래가 정리 파일
(officetel_data_*.txt : 오피스텔 매매, officetel_rent_data_*.txt : 오피스텔 전/월세)

/rh_real_estate: 연립다세대 매매, 전월세 실거래가 정리 파일
(rh_data_*.txt : 연립다세대 매매, rh_rent_data_*.txt : 연립다세대 전/월세)

/sm_real_estate: 단독/다가구 매매, 전월세 실거래가 정리 파일
(sm_data_*.txt : 단독/다가구 매매, sm_rent_data_*.txt : 단독/다가구 전/월세)

/news: 부동산 관련 뉴스 정리 파일
(bitkinds_news_*.txt : 부동산 관련 뉴스(경기도 한정))

/laws: 부동산 관련 법률 정리 파일
 