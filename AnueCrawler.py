from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import pandas as pd
import json
from .base.invoker import SimpleInvoker



category_url = {
    'headline': 'https://api.cnyes.com/media/api/v1/newslist/category/headline?',
    'twstock': 'https://api.cnyes.com/media/api/v1/newslist/category/tw_stock?',
                '美股': '美股api網址'
}



class BaseCNYESInvoker(SimpleInvoker):
    def __init__(self):
        SimpleInvoker.__init__(self, 'api.cnyes.com', 'https', 443)

# API 鉅亨網呼叫工具
# 準備打鉅亨網API必要的Header與參數
# 
# 將收到的資料轉成Python標準物件(List, Dictionary等)
class BaseInvoker:
    # TODO: 先不要轉成DataFrame，而是Python原生的物件
    # TODO: 先不要在這裡指定API所需要的參數，跟API相關的邏輯可以搬移到ApiCrawler，或是更下層的子類別
    # TODO: 在這裏做流量控制

    # 執行抓取資料的動作，並將取得的資料轉換成DataFrame
    def execute(self, url: str, startdate: str, enddate: str) -> pd.DataFrame:
        __startdate = datetime.strptime(startdate, "%Y-%m-%d")
        __targetdate = __startdate + timedelta(days=50)
        __enddate = datetime.strptime(enddate, "%Y-%m-%d")

        # 鉅亨網API不予允查詢超過２個月，所以先用50天
        __startstamp = int(__startdate.timestamp())
        if __targetdate < __enddate:
            __endstamp = int(__targetdate.timestamp())
        else:
            __endstamp = int(__enddate.timestamp())

        first_rquest_json = self._request(
            url, __startstamp, __endstamp, page=1)

        __count = int(first_rquest_json['items']['total'])
        __last_page = int(first_rquest_json['items']['last_page'])
        # TODO: 將資料轉成DataFrame的邏輯搬移到其他class
        # TODO: 將過濾資料欄位與變更欄位名稱的邏輯搬移到其他class
        # dataframe = pd.DataFrame(columns=['時間戳記', '標題'])
        # TODO: 將翻頁的邏輯搬移到PagedInvoker
        DataList = []
        while True:
            for p in range(1, __last_page + 1):
                page_request = self._request(
                    url, __startstamp, __endstamp, page=p)
                for data in page_request['items']['data']:
                    newdata = {'timestamp': data['publishAt'], 'title': data['title'], 'stocks': data['market']}
                    DataList.append(newdata)

            if (__targetdate < __enddate):
                __startdate = __targetdate + timedelta(days=1)
                __targetdate = __startdate + timedelta(days=50)
                __startstamp = int(__startdate.timestamp())
                __endstamp = int(__targetdate.timestamp())
            else:
                break
        return DataList

    def _request(self, url, startstamp: int, endstamp: int, page: int = 1) -> dict:
        # 鉅亨網API網址
        r = re.get(url,
                   headers={
                       'Accept': 'text/html,application/xhtml+xml,application/xml',
                       'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15'
                   },
                   params={'startAt': str(startstamp), 'endAt': str(endstamp), 'limit': '30', 'page': str(page)})
        return json.loads(r.text)



# 準備特定的參數API，分析下行資料並轉成High Level物件（如 DataFrame)


class ApiCrawler:
    def __init__(self, url: str, startdate: str, enddate: str):
        self.__invoker = BaseInvoker()
        self.__url = url
        self.__startdate = startdate
        self.__enddate = enddate

    def execute(self) -> pd.DataFrame:
        return self.__invoker.execute(self.__url, self.__startdate, self.__enddate)

    def to_csv(self):
        self.execute().to_csv()


def headline(startdate: str, enddate: str):
    return ApiCrawler(category_url['headline'], startdate, enddate).execute()


def twstock(startdate: str, enddate: str, stockID=""):
    if stockID == "":
        return ApiCrawler(category_url['twstock'], startdate, enddate).execute()
    else:
        AllData = ApiCrawler(category_url['twstock'], startdate, enddate).execute()
        TargetLsit=[]
        for data in AllData:
            for stock in data['stocks']:
                if stock['code'] == stockID:
                    TargetLsit.append[{'timestamp': data['timestamp'], 'title': data['title']}]
        
        return TargetLsit
