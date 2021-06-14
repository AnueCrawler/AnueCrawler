# AnueCrawler
基於Python語言的[鉅亨網](https://www.cnyes.com)爬蟲程式


### 使用教學
``` python
#載入模組
from AnueCrawler.News import headline, twstock, wdstock, blockchain 
```

API名稱      |說明
-----------:|------:|
headline    |頭條新聞
twstock     |台股新聞
wdstock     |國際股市
blockchain  |區塊鏈

``` python
#抓取當天資料
hd = headline
```
``` python
#指定日期範圍
hd.browse('2021-1-1','2021-1-3')
```
``` python
#擷取指定資料(依據鉅亨網API)
hd.query(['publishAt','title'])
```
``` python
#將結果輸出至CSV格式檔案(名稱可留空，預設Output.csv)
hd.query(['publishAt','title']).to_csv('Output.csv')
```
