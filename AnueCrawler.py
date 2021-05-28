from abc import abstractmethod
from typing import Any, List, final
from base.api_client import ApiClient, BaseApiClient, BaseApiClientRequest, BaseApiClientResponse, InvokerRq, InvokerRs
from base.invoker import Invoker, LoggingInvokerAdapter, Request, Response, SimpleInvoker
from datetime import datetime, time, timedelta
import pandas as pd
import json, copy
import logging
import logging.config
import yaml
from dataclasses import dataclass
import datetime


@dataclass
class HeadlineRequest(BaseApiClientRequest):
    startAt: datetime.datetime
    endAt: datetime.datetime
    limit: int
    page: int

    def generateRequest(self) -> Request:
        request = Request(None, None, None, None, None)
        request.params = {
            'startAt': str(int(self.startAt.timestamp())),
            'endAt': str(int(self.endAt.timestamp())), 'limit': str(self.limit), 'page': str(self.page)
        }
        return request


@dataclass
class HeadlineResponse(BaseApiClientResponse):
    data: Any

    def fit(self, response: Response) -> Request:
        self.data = json.loads(response.text)

class PagedApiClient(ApiClient[InvokerRq, InvokerRs]):
    logger = logging.getLogger('PagedApiClient')
    def __init__(self, client: ApiClient[InvokerRq, InvokerRs]):
        self.__client = client
        self.__responses: List[InvokerRs] = []
        
    @final
    def sendAndReceive(self, rq: InvokerRq) -> InvokerRs:
        self.originRequest = copy.deepcopy(rq)
        rs: InvokerRs = None
        while True:
            rq = self.newRequest(rq, rs)
            logger.info(rq)
            rs = self.__client.sendAndReceive(rq)
            self.__responses.append(rs)
            if self.isEnd(rq, rs):
                break
        return self.aggregrateResponse(self.__responses)


    @abstractmethod
    def isEnd(self, rq: InvokerRq, rs: InvokerRs) -> bool:
        pass
    @abstractmethod
    def newRequest(self, rq: InvokerRq, rs: InvokerRs) -> InvokerRq:
        pass
    @abstractmethod
    def aggregrateResponse(self, responses: List[InvokerRs]) -> InvokerRs:
        pass

class HeadlineApiClient(BaseApiClient[HeadlineRequest, HeadlineResponse]):
    def __init__(self, invoker: Invoker):
        BaseApiClient.__init__(
            self, invoker, 'https://api.cnyes.com/media/api/v1/newslist/category/headline', 'GET')

    def prepareRequest(self, rq: HeadlineRequest) -> Request:
        return rq.generateRequest()

    def parseResponse(self, response: Response) -> HeadlineResponse:
        result = HeadlineResponse(None)
        result.fit(response)
        return result

class PagedHeadlineApiClient(PagedApiClient[HeadlineRequest, HeadlineResponse]):
    def __init__(self, client: HeadlineApiClient):
        PagedApiClient.__init__(self, client)

    def isEnd(self, rq: HeadlineRequest, rs: HeadlineResponse) -> bool:
        return rq.endAt >= self.__targetDate


    def newRequest(self, rq: HeadlineRequest, rs: HeadlineResponse) -> HeadlineRequest:
        newRequest = HeadlineRequest(rq.startAt, rq.startAt + timedelta(days=rq.limit), rq.limit, rq.page)
        if (rs is None):
            self.__targetDate = rq.endAt
            if (newRequest.endAt > self.__targetDate):
                newRequest.endAt = self.__targetDate
            self.__previousEndAt = newRequest.endAt
            return newRequest
        else:
            __last_page = int(rs.data['items']['last_page'])
            __current_page = int(rs.data['items']['current_page'])
            if newRequest.page < __last_page:
                newRequest.page = __current_page + 1
            else:
                newRequest.page = 1
                newRequest.startAt = self.__previousEndAt + timedelta(days = 1)
                newRequest.endAt = newRequest.startAt + timedelta(days = rq.limit)
                if (newRequest.endAt > self.__targetDate):
                    newRequest.endAt = self.__targetDate
                self.__previousEndAt = newRequest.endAt
            return newRequest

    def aggregrateResponse(self, responses: List[HeadlineResponse]) -> HeadlineResponse:
        newResponse = HeadlineResponse(None)
        newResponse.data = [item for list in list(map(lambda resp: resp.data['items']['data'], responses)) for item in list]
        return newResponse

category_url = {
    'headline': 'https://api.cnyes.com/media/api/v1/newslist/category/headline?',
    'twstock': 'https://api.cnyes.com/media/api/v1/newslist/category/tw_stock?',
                '美股': '美股api網址'
}

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
                    newdata = {
                        'timestamp': data['publishAt'], 'title': data['title'], 'stocks': data['market']}
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
        self.url = url
        self.__startdate = startdate
        self.__enddate = enddate

    def execute(self) -> pd.DataFrame:
        return self.__invoker.execute(self.url, self.__startdate, self.__enddate)

    def to_csv(self):
        self.execute().to_csv()


def headline(startdate: str, enddate: str):
    return ApiCrawler(category_url['headline'], startdate, enddate).execute()


def twstock(startdate: str, enddate: str, stockID=""):
    if stockID == "":
        return ApiCrawler(category_url['twstock'], startdate, enddate).execute()
    else:
        AllData = ApiCrawler(
            category_url['twstock'], startdate, enddate).execute()
        TargetLsit = []
        for data in AllData:
            for stock in data['stocks']:
                if stock['code'] == stockID:
                    TargetLsit.append[{
                        'timestamp': data['timestamp'], 'title': data['title']}]

        return TargetLsit


if __name__ == '__main__':
    config = yaml.SafeLoader(open('config/logging.yaml', 'r')).get_data()
    logging.config.dictConfig(config)
    logger = logging.getLogger()
    invoker = PagedHeadlineApiClient(HeadlineApiClient(LoggingInvokerAdapter(
        SimpleInvoker(), logger)))
    request = HeadlineRequest(datetime.datetime(year=2021, month=5, day=1), datetime.datetime(year=2021, month=6, day=30), 30, 1)
    data = invoker.sendAndReceive(request).data
    logger.info(data)
