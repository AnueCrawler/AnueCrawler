import urllib3
import json
from datetime import datetime,timedelta


def simple_demo(startAt: str = None, endAt: str = None):
    today = datetime.today()
    targetdelta  = lambda td, ed : 50 if ed - td > timedelta(days = 50) else (ed - td).days - 1
    defdate = lambda datestr : today if datestr == None else datetime.strptime(datestr, "%Y-%m-%d")

    url = 'https://api.cnyes.com/media/api/v1/newslist/category/headline'
    method: str = "GET"
    headers: dict = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'}
    http = urllib3.PoolManager()
    startdate = defdate(startAt)
    enddate = defdate(endAt)
    datalist = []
    
    targetdate = startdate
    while True:
        if startAt != None : targetdate = startdate + timedelta(days=targetdelta(targetdate,enddate))
        startstamp = str(int(startdate.timestamp())) 
        tartgetstamp = str(int(targetdate.timestamp())) 
        maxpages = 2
        p = 1    
    
        while p <= maxpages: 
            params: dict = {'startAt':startstamp ,'endAt':tartgetstamp,'limit':'30','page':p}
            response = http.request(method = method, url = url, headers = headers, fields = params)
            jsonData = json.loads(response.data.decode('utf-8'))
            print(jsonData)
            for news in jsonData['items']['data']:
                datalist.append(news['title'])
            p = p + 1
            if maxpages != jsonData['items']['last_page'] : maxpages = jsonData['items']['last_page']

        if targetdate == enddate: 
            len(datalist)
        else:
            startdate = targetdate + timedelta(days=1)     

simple_demo('2020-1-1','2020-1-2')