
from abc import ABCMeta, abstractmethod
from base.invoker import Invoker, Request, Response
from typing import Generic, TypeVar, final


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
    @abstractmethod
    def fit(self, response: Response):
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
    def parseResponse(self, response: Response) -> Rs:
        pass
