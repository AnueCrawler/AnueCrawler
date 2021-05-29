from abc import ABCMeta, abstractmethod
import copy
from dataclasses import dataclass
from base.invoker import Request
from base.api_client import ApiClient, BaseApiClientRequest, BaseApiClientResponse, PagedApiClient
from typing import Generic, List, TypeVar

DataVo = TypeVar('DataVo')

@dataclass
class CNYESPageRq(BaseApiClientRequest):
    limit: int
    page: int
    def generateRequest(self) -> Request:
        request = Request()
        return request

@dataclass
class CNYESRsDataItemsVo(Generic[DataVo]):
    last_page: int
    current_page:int
    data: List[DataVo]
    total: int
    per_page: int
    next_page_url: str
    prev_page_url: str
    fromIndex: int
    toIndex: int


    @classmethod
    def from_json(cls, data):
        return cls(data['last_page'], data['current_page'], data['data'], data['total'], data['per_page'], data['next_page_url'], data['prev_page_url'], data['from'], data['to'])

@dataclass
class CNYESRsDataVo(Generic[DataVo]):
    items: CNYESRsDataItemsVo[DataVo]
    message: str
    statusCode: str
    @classmethod
    def from_json(cls, data):
        return cls(CNYESRsDataItemsVo.from_json(data['items']), data['message'], data['statusCode'])


@dataclass
class CNYESPageRs(BaseApiClientResponse, Generic[DataVo]):
    data: CNYESRsDataVo[DataVo]

    @classmethod
    def from_json(cls, data):
        return cls(**data)


class CNYESPagedApiClient(PagedApiClient[CNYESPageRq, CNYESPageRs[DataVo]], metaclass=ABCMeta):
    def __init__(self, client: ApiClient):
        PagedApiClient.__init__(self, client)

    def newRequest(self, rq: CNYESPageRq, rs: CNYESPageRs[DataVo]) -> CNYESPageRq:
        newRequest = copy.deepcopy(self.originRequest)
        newRequest.limit = rq.limit
        newRequest.page = rq.page
        if rs is not None:
            __last_page = int(rs.data.items.last_page)
            __current_page = int(rs.data.items.current_page)
            if newRequest.page < __last_page:
                newRequest.page = __current_page + 1
        return newRequest

    def isEnd(self, rq: CNYESPageRq, rs: CNYESPageRs[DataVo]) -> bool:
        __last_page = int(rs.data.items.last_page)
        __current_page = int(rs.data.items.current_page)
        return __current_page >= __last_page
            
    def aggregrateResponse(self, responses: List[CNYESPageRs[DataVo]]) -> CNYESPageRs[DataVo]:
        collection: List[DataVo] = []
        for response in responses:
            for item in response.data.items.data:
                collection.append(item)
        result = responses[-1]
        result.data.items.data = collection
        return result
