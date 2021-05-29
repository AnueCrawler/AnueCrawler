from base.invoker import LoggingInvokerAdapter, SimpleInvoker
from base.api_client import ApiClient
from crawler.api.cnyes.headline import HeadlineApiClient, HeadlineRequest
from crawler.api.cnyes.base import CNYESPagedApiClient
import datetime
import logging
import logging.config
import yaml
import datetime

if __name__ == '__main__':
    # 初始化 logger 
    config = yaml.SafeLoader(open('config/logging.yaml', 'r')).get_data()
    logging.config.dictConfig(config)
    logger = logging.getLogger()

    # 建立Headline的ApiClient
    apiClient: ApiClient = HeadlineApiClient(SimpleInvoker()) # 最簡易的ApiClient (沒有翻頁)
    apiClient = HeadlineApiClient(LoggingInvokerAdapter(SimpleInvoker(), logger)) # 會紀錄Http Request/Response的ApiClient (沒有翻頁)
    apiClient = CNYESPagedApiClient(HeadlineApiClient(LoggingInvokerAdapter(SimpleInvoker(), logger))) # 會紀錄Http Request/Response的ApiClient，且會自動翻頁
    request = HeadlineRequest(30, 1, datetime.datetime(year=2021, month=5, day=1), datetime.datetime(year=2021, month=6, day=30))
    response = apiClient.sendAndReceive(request)
    data = response.data.items.data
    logger.debug(data)
