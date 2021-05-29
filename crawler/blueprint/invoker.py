from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import MutableMapping, Union, final

from requests.sessions import Session as RequestsSession
from requests import Request as RequestRequest
from asyncio_throttle import Throttler
import logging

@dataclass
class Request:
    url: str
    method: str
    headers: dict
    params: dict
    body: Union[object, None]

@dataclass
class Response:
    text: str
    status_code: int
    headers: MutableMapping

class Invoker(metaclass=ABCMeta):
    """
    A basic interface that defines what Invoker can do.
    """
    @abstractmethod
    def invoke(self, request: Request) -> Response:
        pass

class LoggingInvokerAdapter(Invoker):
    """
    An InvokerAdaptor that print the reqest and reponse into log.
    """
    def __init__(self, invoker: Invoker, logger: logging.Logger):
        self.__invoker = invoker
        self.__logger = logger
    
    @final
    def invoke(self, request: Request) -> Response:
        if self.__logger.isEnabledFor(logging.DEBUG):
            self.__logger.debug('Request: %s', request)
        response = self.__invoker.invoke(request)
        if self.__logger.isEnabledFor(logging.DEBUG):
            self.__logger.debug('Response: %s', response)  
        return response  

class ThrottledInvokerAdapter(Invoker):
    """
    An adapter that can control how many request could be sent in a period of time
    """
    def __init__(self, invoker: Invoker, rate_limit: int, period: int):
        self.__invoker = invoker
        self.__throttler = Throttler(rate_limit=rate_limit, period=period) 
    
    @final
    def invoke(self, request: Request) -> Response:
        self.__throttler.acquire()
        result = self.__invoker.invoke(request)
        self.__throttler.flush()
        return result    
class SimpleInvoker(Invoker):
    """
    A very simple invoker that use requests packge to send request
    """
    logger = logging.getLogger('SimpleInvoker')
    def __init__(self, pSession: RequestsSession = None):
        if pSession is not None:
            self.session = pSession
        else:
            self.session = RequestsSession()
    def invoke(self, request: Request) -> Response:
        __request = RequestRequest()
        __request.method = request.method.strip().upper()
        __request.params = request.params
        __request.headers = request.headers
        __request.url = request.url
        __request.json = request.body
        preparedRequest = __request.prepare()
        SimpleInvoker.logger.debug(preparedRequest.url)
        __response = self.session.send(preparedRequest)
        return Response(__response.text, __response.status_code, __response.headers)