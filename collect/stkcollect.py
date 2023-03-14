from tqdm import tqdm
import time
import json
import pymysql
import numpy as np
import pandas as pd
from io import BytesIO
import re
import requests as rq
from bs4 import BeautifulSoup

url = 'https://finance.naver.com/sise/sise_deposit.nhn'
data = rq.get(url)
data_html = BeautifulSoup(data.content)

parse_day = data_html.select_one(
    'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah'
).text


biz_day = re.findall('[0-9]+', parse_day)
biz_day = ''.join(biz_day)


print(biz_day)

gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_top_stk = {
    'mktId': 'STK',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}
headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp_stk = rq.post(gen_otp_url, gen_top_stk, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)

sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
gen_top_ksq = {
    'mktId': 'KSQ',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}

otp_ksq = rq.post(gen_otp_url, gen_top_ksq, headers=headers).text
down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR')

krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)


krx_sector['종목명'] = krx_sector['종목명'].str.strip()
krx_sector['기준일'] = biz_day

gen_top_ind = {
    'searchType': '1',
    'mktId': 'ALL',
    'trdDd': biz_day,
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
}

otp_ind = rq.post(gen_otp_url, gen_top_ind, headers=headers).text

krx_ind = rq.post(down_url, {'code': otp_ind}, headers=headers)
krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
krx_ind['종목명'] = krx_ind['종목명'].str.strip()
krx_ind['기준일'] = biz_day


set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명']))

kor_ticker = pd.merge(krx_sector,
                      krx_ind,
                      on=krx_sector.columns.intersection(
                          krx_ind.columns).to_list(),
                      how='outer')


kor_ticker[kor_ticker['종목명'].str.contains('스팩|제[0-9]+호')]['종목명']


kor_ticker[kor_ticker['종목코드'].str[-1:] != '0']['종목명']

kor_ticker[kor_ticker['종목명'].str.endswith('리츠')]


diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))


kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                              np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                                       np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                                                np.where(kor_ticker['종목명'].isin(diff), '기타',
                                                         '보통주'))))

kor_ticker = kor_ticker.reset_index(drop=True)
kor_ticker.columns = kor_ticker.columns.str.replace(' ', '')
kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '종가',
                         '시가총액', '기준일', 'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']]

kor_ticker = kor_ticker.replace({np.nan: None})
kor_ticker['기준일'] = pd.to_datetime(kor_ticker['기준일'])

kor_ticker.head()




mycursor = con.cursor()

query = f"""
insert into kor_ticker (종목코드,종목명,시장구분,종가,시가총액,기준일,EPS,선행EPS,BPS,주당배당금,종목구분) 
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
on duplicate key update 
종목명 = VALUES(종목명),
시장구분 = VALUES(시장구분), 
종가 = VALUES(종가),  
시가총액 = VALUES(시가총액), 
EPS = VALUES(EPS), 
선행EPS = VALUES(선행EPS),  
BPS = VALUES(BPS), 
주당배당금 = VALUES(주당배당금), 
종목구분 = VALUES(종목구분);
"""

args = kor_ticker.values.tolist()

mycursor.executemany(query, args)
con.commit()

con.close()


# import requests as rq
# import pandas as pd


print(data)

sector_code = ['G25', 'G35', 'G50', 'G40',
               'G10', 'G20', 'G55', 'G30', 'G15', 'G45']


data_sector = []

for i in tqdm(sector_code):
    url_sector = f'''https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd={i}'''
    data = rq.get(url_sector).json()
    data_pd = pd.json_normalize(data['list'])

    data_sector.append(data_pd)

    time.sleep(2)


print(data_sector)

kor_sector = pd.concat(data_sector, axis=0)
kor_sector = kor_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']]
kor_sector['기준일'] = biz_day
kor_sector['기준일'] = pd.to_datetime(kor_sector['기준일'])



mycursor = con.cursor()

query = f"""
insert into kor_sector (IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
values (%s,%s,%s,%s,%s)
on duplicate key update
IDX_CD = VALUES(IDX_CD), 
CMP_KOR = VALUES(CMP_KOR), 
SEC_NM_KOR = VALUES(SEC_NM_KOR)
"""

args = kor_sector.values.tolist()

mycursor.executemany(query, args)
con.commit()
con.close()
