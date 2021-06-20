import urllib3, json, logging, csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import time

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=(logging.DEBUG), filename='api.log', filemode='w', format=FORMAT)
today = datetime.today()
todaystamp = str(int(today.timestamp()))
default_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'}
default_params = {'limit':'30',  'startAt':todaystamp,  'endAt':todaystamp,  'page':'1'}

class request:

    def __init__(self, url, method, headers=default_headers, params=default_params):
        self.url = url
        self.method = method
        self.headers = headers
        self.params = params


@dataclass
class response:
    status: int
    data = ''
    data: str


def _singlepage(rq: request) -> response:
    http = urllib3.PoolManager()
    rs = http.request(url=(rq.url), method=(rq.method),
      headers=(rq.headers),
      fields=(rq.params))
    if rs.status == 200:
        return response(rs.status, rs.data)
    if rs.status == 422:
        logging.error('{startat} : {endAt} -> Response Statu: 422'.format(startat=(rq.params['startAt'])), endAt=(rq.params['endAt']))
        return response(rs.status, json.loads(rs.data)['message'])


def _mulitPages(rq: request, rs: response) -> list:
    datalist = []
    while True:
        if rs.status == 200:
            json_data = json.loads(rs.data)['items']
            logging.debug('{sa} - {ea} => Count: {count}, MaxPages: {pages}, CurrentPage: {cpage}'.format(sa=(datetime.fromtimestamp(int(rq.params['startAt']))),
              ea=(datetime.fromtimestamp(int(rq.params['endAt']))),
              count=(json_data['total']),
              pages=(json_data['last_page']),
              cpage=(json_data['current_page'])))
            if rs.status == 200:
                currentpage = json_data['current_page']
                lastpage = json_data['last_page']
                for news in json_data['data']:
                    logging.debug('NewsID:{ID} publish at {publicAt}'.format(ID=(news['newsId']), publicAt=(news['publishAt'])))
                    datalist.append(news)

                if lastpage != 0:
                    rq.params['page'] = str(int(rq.params['page']) + 1)
            else:
                rq.params['page'] = 0

        if currentpage >= lastpage:
            break

        rs = _singlepage(rq)

    return datalist

class cnyes_list(list):
    def __init__(self, data: list):
        self.extend(data)

    def to_csv(self, filename: str='Output.csv'):
        csv_columns = self[0].keys()
        csv_file = filename
        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in self:
                    writer.writerow(data)

        except IOError:
            print('I/O error')


class News_API(list):

    def __init__(self, category: str):
        self._url = 'https://api.cnyes.com/media/api/v1/newslist/category/' + category
        self.extend(self.browse()._datalist)

    def data_list(self) -> list:
        try:
            print(type(self._datalist))
        except:
            self.browse()
        finally:
            return self._datalist

    def query(self, taget: list) -> cnyes_list:
        try:
            print(type(self._datalist))
        except:
            self.browse()
        else:
            filterlist = []
            for data in self._datalist:
                dict = {}
                for t in taget:
                    dict[t] = data[t]
                else:
                    filterlist.append(dict)

            else:
                final_list = cnyes_list(filterlist)
                return final_list

    def browse(self, startdate_str: str=None, enddate_str: str=None):
        self._startdate_str = startdate_str
        self._enddate_str = enddate_str
        defdate = lambda datestr: today if datestr == None else datetime.strptime(datestr, '%Y-%m-%d')
        targetdelta = lambda td, ed: timedelta(days=50) if ed - td > timedelta(days=50) else ed - td
        today = datetime.today()
        startdate = defdate(self._startdate_str)
        enddate = defdate(self._enddate_str)
        enddate = datetime.combine(enddate, time(23, 59, 59))
        targetdate = startdate
        all_data_list = []
        while True:
            if self._enddate_str != None:
                targetdate = startdate + targetdelta(targetdate, enddate)
                targetdate = datetime.combine(targetdate, time(23, 59, 59))
            else:
                targetdate = datetime.combine(today, time(23, 59, 59))
            startstamp = str(int(startdate.timestamp()))
            tartgetstamp = str(int(targetdate.timestamp()))
            params = {'limit':'30',  'startAt':startstamp,  'endAt':tartgetstamp,  'page':'1'}
            rq = request(self._url, 'GET', default_headers, params)
            rs = _singlepage(rq)
            all_data_list = all_data_list + _mulitPages(rq, rs)

            if targetdate >= enddate:
                break
            else:
                startdate = targetdate + timedelta(days=1)
                startdate = startdate.replace(hour=0, minute=0, second=0)

        self._datalist = all_data_list
        return self



if __name__ == '__main__' :
    test = News_API('headline')
    print(test.browse('2021-6-1').data_list())
