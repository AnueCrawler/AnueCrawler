
from abc import ABCMeta, abstractmethod
from crawler.blueprint.invoker import Invoker, Request, Response
from typing import Generic, List, TypeVar, Union, final
import logging, copy

Rq = TypeVar('Rq')
Rs = TypeVar('Rs')
class ApiClient(Generic[Rq, Rs], metaclass=ABCMeta):
    def __init__(self, url: str, method: str = 'GET'):
        self.url = url
        self.method = method

    @abstractmethod
    def sendAndReceive(self, rq: Rq) -> Rs:
        pass


class BaseApiClientRequest(metaclass=ABCMeta):
    @abstractmethod
    def generateRequest(self) -> Request:
        pass


class BaseApiClientResponse(metaclass=ABCMeta):
    pass


InvokerRq = TypeVar('InvokerRq')
InvokerRs = TypeVar('InvokerRs')
class BaseApiClient(ApiClient[InvokerRq, InvokerRs]):
    def __init__(self, invoker: Invoker, url: str, method: str = 'GET'):
        self.__invoker = invoker
        ApiClient.__init__(self, url, method)

    @final
    def sendAndReceive(self, rq: InvokerRq) -> InvokerRs:
        request = self.prepareRequest(rq)
        request.url = self.url
        request.method = self.method
        return self.parseResponse(self.__invoker.invoke(request))

    @abstractmethod
    def prepareRequest(self, rq: InvokerRq) -> Request:
        pass

    @abstractmethod
    def parseResponse(self, response: Response) -> InvokerRs:
        pass


class PagedApiClient(ApiClient[InvokerRq, InvokerRs]):
    logger = logging.getLogger('PagedApiClient')
    def __init__(self, client: ApiClient[InvokerRq, InvokerRs]):
        self.__client = client
        self.__responses: List[InvokerRs] = []
        
    @final
    def sendAndReceive(self, rq: InvokerRq) -> InvokerRs:
        self.originRequest = copy.deepcopy(rq)
        rs: Union[InvokerRs, None] = None
        while True:
            rq = self.newRequest(rq, rs)
            PagedApiClient.logger.debug(rq)
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