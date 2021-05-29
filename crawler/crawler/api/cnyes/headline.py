from crawler.blueprint.api_client import BaseApiClient
from crawler.blueprint.invoker import Invoker, Request, Response
from crawler.crawler.api.cnyes.base import CNYESPageRq, CNYESPageRs, CNYESRsDataItemsVo, CNYESRsDataVo, DataVo
import json
from typing import Any
import datetime
from dataclasses import dataclass
from urllib.error import HTTPError


@dataclass
class HeadlineRequest(CNYESPageRq):
    startAt: datetime.datetime
    endAt: datetime.datetime

    def generateRequest(self) -> Request:
        request = Request('', '', {}, {}, None)
        request.params = {
            'startAt': str(int(self.startAt.timestamp())),
            'endAt': str(int(self.endAt.timestamp())), 'limit': str(self.limit), 'page': str(self.page)
        }
        return request

@dataclass
class HeadlineResponse(CNYESPageRs[Any]):
    pass


class HeadlineApiClient(BaseApiClient[HeadlineRequest, HeadlineResponse]):
    def __init__(self, invoker: Invoker):
        BaseApiClient.__init__(
            self, invoker, 'https://api.cnyes.com/media/api/v1/newslist/category/headline', 'GET')

    def prepareRequest(self, rq: HeadlineRequest) -> Request:
        return rq.generateRequest()

    def parseResponse(self, response: Response) -> HeadlineResponse:
        result = HeadlineResponse(CNYESRsDataVo.from_json(json.loads(response.text)))
        return result