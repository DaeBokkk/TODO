# TODO
# 데이터 수집 파트

 bitKinds.py: 빅카인즈에서 경기도권의 부동산 뉴스 정보를 매 23:58분마다 자동 크롤링 
 if __name__ == "__main__": 아래에 2가지 사용 방법 정리해놓음(txt 파일 도출 방식 or Langchain Document 객체로 전달해주는 방식) 자동화를 생각하면 txt 도출 방식이 적절해보임

 apt_list.py: 아파트 매매 실거래가 api 수집 파일
 위와 마찬가지로 메인에 실행 로직 짜여져있음

 law_go.py: 법령 크롤링 모듈

/txts: 수집한 데이터들을 종류별로 정리해놓은 디렉토리
(rent가 붙은 파일은 전월세 텍스트 데이터임)
/apt_real_estate: 아파트 매매, 전월세 실거래가 정리 파일
/land_real_estate: 토지 매매 실거래가 정리 파일
/official_real_estate: 오피스텔 매매, 전월세 실거래가 정리 파일
/rh_real_estate: 연립다세대 매매, 전월세 실거래가 정리 파일
/sm_real_estate: 단독/다가구 매매, 전월세 실거래가 정리 파일

/laws: 부동산 관련 법률 정리 파일
/news: 부동산 관련 뉴스 정리 파일
 