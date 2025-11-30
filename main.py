from dataPortal import land_ai, apt, sm, rh, officetel
from crawling_laws import law_go
from crawling_news import bitKinds
import schedule
import time

# 메인 파이프라인 스크립트 함수 구현
def trade_auto_collect():
    apt.save_apt_data_to_txt()
    apt.save_apt_rent_data_to_txt()

    land_ai.save_land_trade_data_to_txt()

    officetel.save_officetel_trade_data_to_txt()
    officetel.save_officetel_rent_data_to_txt()
    
    sm.save_sm_trade_data_to_txt()
    sm.save_sm_rent_data_to_txt()

    rh.save_rh_trade_data_to_txt()
    rh.save_rh_rent_data_to_txt()

def crawl_law_texts():
    bitKinds.main()

schedule.every().day.at("01:00").do(trade_auto_collect)
schedule.every().day.at("23:58").do(crawl_law_texts)

if __name__ == "__main__":
    while True:
        print("스케줄러 대기 중...")
        schedule.run_pending()
        time.sleep(30) # 30초 대기

