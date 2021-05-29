from abc import abstractmethod
from crawler.blueprint.api_client import ApiClient
from crawler.crawler.api.cnyes.base import CNYESPagedApiClient
from crawler.crawler.api.cnyes.headline import HeadlineApiClient, HeadlineRequest
from crawler.blueprint.invoker import LoggingInvokerAdapter, SimpleInvoker
import datetime
import logging
import logging.config
import yaml
import datetime

if __name__ == '__main__':
    config = yaml.SafeLoader(open('config/logging.yaml', 'r')).get_data()
    logging.config.dictConfig(config)
    logger = logging.getLogger()
    invoker: ApiClient = CNYESPagedApiClient(HeadlineApiClient(LoggingInvokerAdapter(
        SimpleInvoker(), logger)))
    request = HeadlineRequest(30, 1, datetime.datetime(year=2021, month=5, day=1), datetime.datetime(year=2021, month=6, day=30))
    data = invoker.sendAndReceive(request).data
    logger.debug(data)
