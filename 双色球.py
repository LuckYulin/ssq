import  requests
from bs4 import BeautifulSoup
import re
import pymysql
from pyquery import PyQuery as pq
from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor
from multiprocessing import Process,Pool
from concurrent import futures

conn = None
conn = pymysql.connect(host='172.24.1.68', user='yumi', password='8989', database='my')

def crawl(x):
    url = 'http://kaijiang.zhcw.com/zhcw/html/ssq/list_{}.html'.format(x)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"}
    response = requests.get(url=url,headers=headers)
    response.encoding = "utf8"
    content = response.text
    soup = BeautifulSoup(content,'lxml')
    target =soup.select('tr')[2:-1]

    for tr in target:
        a = pq(str(tr)).items()
        for i in a:
            a1 = i('td').eq(0).text()
            a2 = i('td').eq(1).text()
            a3 = i('td').eq(2).text()
            a4 = i('td').eq(3).text()
            a5 = i('td').eq(4).text()
            a6 = i('td').eq(5).text()
            sav_to_mysql(a1,a2,a3,a4,a5,a6)

def sav_to_mysql(a1,a2,a3,a4,a5,a6):
    print(a1,a2,a3,a4,a5,a6)
    with conn:
        try:
            cursor = conn.cursor()
            sql = 'insert into suanseqiu (YEAR ,MONTH,NO,BONUS,menbers1,menbers2)values(%s,%s,%s,%s,%s,%s)'
            cursor.execute(sql,(a1,a2,a3,a4,a5,a6))
            conn.commit()
        except Exception as e:
            print('异常',e)


if __name__ == '__main__':
    excutor = ProcessPoolExecutor(8)
    task = [excutor.submit(crawl,i) for i in range(1,126)]
    futures.wait(task,return_when=futures.ALL_COMPLETED)
    print('存储完毕')
    if conn:
        conn.close()



