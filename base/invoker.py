from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import final


from requests import session as RequestsSession, Request as RequestRequest
from asyncio_throttle import Throttler
import copy, logging

@dataclass
class Request:
    url: str
    method: str
    headers: dict
    params: dict
    body: object

@dataclass
class Response:
    text: str
    status_code: int
    headers: dict

class Invoker(metaclass=ABCMeta):
    def __init__(self, host: str, protocol: str = 'http', port: int = 80):
        self.__host = host
        self.__protocol = protocol
        self.__port = port

    @final
    def url(self) -> str:
        return self.__protocol + '://' + self.__host + ':' + str(self.__port)

    def invoke(self, request: Request) -> Response:
        copyRequest = copy.deepcopy(request)
        copyRequest.url = self.url()
        return self.sendRequest(copyRequest)

    @abstractmethod
    def sendRequest(self, request: Request) -> Response:
        pass
    


class SimpleInvoker(Invoker):
    def __init__(self):
        self.__session = RequestsSession()
    def sendRequest(self, request: Request) -> Response:
        __request = RequestRequest()
        __request.method = request.method.strip().upper()
        __request.params = request.params
        __request.headers = request.headers
        __request.url = request.url
        __request.json = request.body
        __response = self.__session.send(__request)
        response = Response()
        response.text = __response.text
        response.headers = __response.headers
        response.status_code = __response.status_code
        return response

# pip install asyncio-throttle
# 實作控制每一段時間打出去的Request數量 (可參考: https://pypi.org/project/asyncio-throttle/，但需要搭配static variable)
class ThrottledInvokerAdapter(Invoker):
    def __init__(self, invoker: Invoker, rate_limit: int, period: int):
        self.__invoker = invoker
        self.__throttler = Throttler(rate_limit=rate_limit, period=period) 
    
    @final
    def invoke(self, request: Request) -> Response:
        self.__throttler.acquire()
        result = self.__invoker.invoke(request)
        self.__throttler.flush()
        return result

class LoggingInvokerAdapter(Invoker):
    def __init__(self, invoker: Invoker, logger: logging.Logger):
        self.__invoker = invoker
        self.__logger = logger
    
    @final
    def invoke(self, request: Request) -> Response:
        if self.__logger.level >= logging.DEBUG:
            self.__logger.debug('Request: %s', request)
        response = self.__invoker.invoke(request)
        if self.__logger.level >= logging.DEBUG:
            self.__logger.debug('Response: %s', response)  
        return response      



# 將翻頁邏輯寫在這裡
class PagedInvokerAdapter(Invoker, metaclass=ABCMeta):
    logger = logging.getLogger('PagedInvokerAdapter')

    def __init__(self, invoker: Invoker):
        self.__invoker = LoggingInvokerAdapter(invoker, PagedInvokerAdapter.logger)

    @abstractmethod
    def isEnd(self, request:Request, response: Response) -> bool:
        pass

    @abstractmethod
    def nextRequest(self, request:Request, response: Response) -> Request:
        pass

    @abstractmethod
    def foldResponses(self, responses: list[Response]) -> Response:
        pass

    @final
    def invoke(self, request: Request) -> Response:
        responses = []
        __response = self.__invoker.invoke(request)
        responses.append(__response)
        while not self.isEnd(request, __response):
            request = self.nextRequest(request, __response)
            __response = self.__invoker.invoke(request)
            responses.append(__response)
        return self.foldResponses(responses)