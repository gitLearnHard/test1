import re
import urllib.request
import pymysql
import time
import chardet

LOG_FLAG = 0
STOCK_HOLDER_ALL_CNT = 0
STOCK_NUMBER_ALL_CNT = 0
DATA_ALL_CNT = 0

SQL = "no data"
conn = pymysql.connect(host='localhost',port='',user='root',passwd='yiluxiangbei',db='stockinfo',charset='utf8')
cur = conn.cursor()

#write data into file
def WriteFile(fname,data):
    f = open(fname, 'a')
    if f:
        f.write(data)
        f.close()
    else:
        return False

#codec swicth
def Code_detect(url):
    urldet = getHtml(url)
    codede = chardet.detect(urldet)['encoding']
    print('%s <- %s' %(url,codede))
    return codede

#get web all data
def getHtml(url):
    error = 0
    cnt = 1
    while error == 0 and cnt < 10:
        error = 1
        try:
            page = urllib.request.urlopen(url, timeout=520)
        except urllib.request.HTTPError as e:
            if e.code == 500:
                print(e.msg)
                return -2
            print(e.code)
            print(e.msg)
            print(url)
            time.sleep(9)
            cnt = cnt +1
            print("sleep 9 seconds. url:%s cnt:%d" % (url, cnt))
            error = 0
    if cnt == 10:
        return -1
    html_tmp = page.read()
##    codetype = chardet.detect(html_tmp)['encoding']
    html = html_tmp.decode('gb2312', 'ignore')
##    html = html.decode('gb2312')
    page.close()
    return html

#get vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/ stock number
def getHolderNum(html):
    reg = r"""colspan="4">(.*?)<"""
    hd_num = re.compile(reg, re.S)
    html = html.replace("\t", "")
    html = html.replace("\n", "")
    html = html.replace("\r", "")
    num = re.findall(hd_num, html)
    return num

#get http://quote.eastmoney.com/stocklist.html All data
def getAllStockNum(htmlEastMoney):
    reg_allStockNum = r""".html">(.*?)</a>"""
    reg_allStockNum_cmpiled = re.compile(reg_allStockNum,re.S)
    allStockNum = re.findall(reg_allStockNum_cmpiled, htmlEastMoney)
    return allStockNum

#get every season stockholder data by stockNum
def getStockData(num):
    print("ENTER getStockData(%s)" % (num))
    stockholder_info_web = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockHolder/stockid/"+num+".phtml"
    if LOG_FLAG == 1: #log1
        print(stockholder_info_web)
    time1 = time.localtime()
    htmlinfo = getHtml(stockholder_info_web)
    if htmlinfo == -1:
        return -1
    if htmlinfo == -2:
        return -2
    holderInfoSum = getHolderNum(htmlinfo)
    if LOG_FLAG == 2: #log2
        print(holderInfoSum)
    hldInfoLen = len(holderInfoSum)
    for i in range(0, hldInfoLen - 1, 4):
        global STOCK_HOLDER_ALL_CNT
        STOCK_HOLDER_ALL_CNT = STOCK_HOLDER_ALL_CNT +1
        date = DivDate(holderInfoSum[i])
        date = ''.join(date)
        announce_date = DivDate(holderInfoSum[i+1])
        announce_date = ''.join(announce_date)
        if announce_date == '':
            announce_date = '0'
        stockHolderNum = DivHldNum(holderInfoSum[i+2])
        if stockHolderNum == "":
            stockHolderNum = 0
        stockHolderNum = int(stockHolderNum)
        stockAvgNum = DivAvgNum(holderInfoSum[i+3])
        stockAvgNum = ''.join(stockAvgNum)
        if stockAvgNum == "":
            stockAvgNum = 0
        stockAvgNum = int(stockAvgNum)
        if LOG_FLAG == 2:  # log2
            print(date)
            print(announce_date)
            print(stockHolderNum)
            print(stockAvgNum)
        num = int(num)
        if stockHolderNum != 0 and stockHolderNum != 0 and announce_date != '0' and stockHolderNum != 0 and stockAvgNum != 0:
##            SQL = "insert into stockholdercnt(id_stock,stock_code,holder_date,holder_date_announce,holder_cnt,stock_cnt_one_holder) " \
##              "values (%d,%d,%s,%s,%d,%d)" %(STOCK_HOLDER_ALL_CNT,num,date,announce_date,stockHolderNum,stockAvgNum)
            SQL = "insert into stockholdercnt(stock_code,holder_date,holder_date_announce,holder_cnt,stock_cnt_one_holder) " \
            "values (%d,%s,%s,%d,%d)" % (num, date, announce_date, stockHolderNum, stockAvgNum)
##        SQL = "insert into stockholdercnt(id_stock) values (%d)" %(cnt)
            try:
                cur.execute(SQL)
            except Exception as e:
                print(e)
                print("stock_code(%d),holder_date(%s),holder_date_announce(%s),holder_cnt(%d),stock_cnt_one_holder(%d)"
                      % (num, date, announce_date, stockHolderNum, stockAvgNum))

            ##insert into table stocklist, and insert into table stockholdercnt by call getStockData()
def insertStockList(DataAll):
    global DATA_ALL_CNT
    global STOCK_NUMBER_ALL_CNT
    for i in range(0, DATA_ALL_CNT-1):
        num = DivNum(DataAll[i])
        name = DivName(DataAll[i])
        num = ''.join(num)
        name = ''.join(name)
        if ((num.find('6') == 0) or (num.find('0') == 0) or (num.find('3') == 0)) and len(num) == 6:
            STOCK_NUMBER_ALL_CNT = STOCK_NUMBER_ALL_CNT + 1
            if i > 1100 :
##                SQL = "insert into stocklist(id_stock,stocknum) values (%d,%s)" %(STOCK_NUMBER_ALL_CNT, num)
                SQL = "insert into stocklist(stocknum) values (%s)" % (num)
                SQL = SQL.encode('utf-8')
                cur.execute(SQL)
                iResult = getStockData(num)
                if iResult == -1:
                    print("insertStockList() i: %d" % (i))
                    return -1
                if iResult == -2:
                    print("No StockInfo now [%s]" %(num))
                    continue
                if i % 50 == 0:
                    conn.commit()
                    print("i = %d" % (i))

def DivAvgNum(AvgNum):
    AvgNum = AvgNum.replace("(", "")
    AvgNum = AvgNum.replace(")", "")
    reNum = r'\d.*\d'
    rulRegNum = re.compile(reNum)
    AvgNum =re.findall(rulRegNum,AvgNum)
    return AvgNum

def DivHldNum(HolderNum):
    HolderNum = HolderNum.replace("(", "")
    HolderNum = HolderNum.replace(")", "")
    return HolderNum

def DivDate(date):
    date = date.replace("(", "")
    date = date.replace(")", "")
    date = date.replace("-", "")
    return date

def DivName(StockDataOne):
    StockDataOne = StockDataOne.replace("(","")
    StockDataOne = StockDataOne.replace(")","")
    regStr = r'\D.*\D'
    rulRegStr = re.compile(regStr)
    StockDataStr =re.findall(rulRegStr,StockDataOne)
    return StockDataStr

def DivNum(StockDataOne):
    StockDataOne = StockDataOne.replace("(","")
    StockDataOne = StockDataOne.replace(")","")
    reNum = r'\d.*\d'
    rulRegNum = re.compile(reNum)
    StockDataNum =re.findall(rulRegNum,StockDataOne)
    return StockDataNum

def insert():
    global DATA_ALL_CNT
    htmlAllData = getHtml("http://quote.eastmoney.com/stocklist.html")
    DataAll = getAllStockNum(htmlAllData)
    DATA_ALL_CNT = len(DataAll)
    iBool = insertStockList(DataAll)
    if iBool == -1:
        print("Program Error!")
    conn.commit()
    cur.close()
    conn.close()
    return True

print("dataBase OK")
