# -*- coding: utf-8 -*-
import logging
from logging import FileHandler
import time
import threading
import sys
import json
import pymysql
import requests
class Auction:
    def __init__(self,auctionId,db):
        self.auctionId=auctionId
        self.db=db
    def auction_thread(self,timewait):
        logger.info("wait "+str(timewait)+"seconds  to deal with auctionid:"+str(self.auctionId))
        time.sleep(timewait)
        self.get_bid_result()
        if mutex.acquire(1): 
            self.insert_db()
            mutex.release()
    def start_thread(self,timewait):
        t=threading.Thread(target=self.auction_thread,args=(timewait,))
        t.start()
    def get_bid_result(self):
        logger.info("request auctionid:"+str(self.auctionId))
        url="https://used-api.jd.com/auction/detail?auctionId="+str(self.auctionId)
        r=requests.get(url)
        obj=json.loads(r.text)
        self.auctionId=obj['data']['auctionInfo']['id']
        self.auctionRecordId=obj['data']['auctionInfo']['auctionRecordId']
        self.usedNo=obj['data']['auctionInfo']['usedNo']
        self.productName=obj['data']['auctionInfo']['productName']
        self.status=obj['data']['auctionInfo']['status']
        self.auditState=obj['data']['auctionInfo']['auditState']
        self.storeId=obj['data']['auctionInfo']['storeId']
        self.startTime=obj['data']['auctionInfo']['startTime']
        self.endTime=obj['data']['auctionInfo']['endTime']
        self.duringTime=obj['data']['auctionInfo']['duringTime']
        self.currentPrice=obj['data']['auctionInfo']['currentPrice']
        self.bidder=obj['data']['auctionInfo']['currentPrice']
        self.bidderNickName=obj['data']['auctionInfo']['bidderNickName']
        self.offerTime=obj['data']['auctionInfo']['offerTime']
        self.cappedPrice=obj['data']['auctionInfo']['cappedPrice']
        self.cbjPrice=obj['data']['auctionInfo']['cbjPrice']
        self.skuidNew=obj['data']['productBaseInfo']['skuidNew']
        self.skuidUsed=obj['data']['productBaseInfo']['skuidUsed']
        self.spectatorCount=obj['data']['spectatorCount']
        self.location=obj['data']['location']
        self.newPrice=obj['data']['newPrice']
    def insert_db(self):
        sql_str="SELECT COUNT(*) FROM auction.history WHERE auctionId="+str(self.auctionId)
        logger.info(sql_str)
        cursor=db.cursor()
        cursor.execute(sql_str)
        data = cursor.fetchall()
        if data[0][0] != 0:
            logger.info("data exist,the auctionId is: /t"+str(self.auctionId))
        else:
            logger.info("insert auctionid:"+str(self.auctionId))
            sql_str='INSERT INTO auction.history (auctionId,auctionRecordId,usedNo,productName,status,auditState,storeId,startTime,endTime,duringTime,currentPrice,bidder,bidderNickName,offerTime,cappedPrice,cbjPrice,skuidNew,skuidUsed,spectatorCount,location,newPrice) VALUES(\''+str(self.auctionId)+'\',\''+str(self.auctionRecordId)+'\',\''+str(self.usedNo)+'\',\''+str(self.productName)+'\',\''+str(self.status)+'\',\''+str(self.auditState)+'\',\''+str(self.storeId)+'\',\''+str(self.startTime)+'\',\''+str(self.endTime)+'\',\''+str(self.duringTime)+'\',\''+str(self.currentPrice)+'\',\''+str(self.bidder)+'\',\''+str(self.bidderNickName)+'\',\''+str(self.offerTime)+'\',\''+str(self.cappedPrice)+'\',\''+str(self.cbjPrice)+'\',\''+str(self.skuidNew)+'\',\''+str(self.skuidUsed)+'\',\''+str(self.spectatorCount)+'\',\''+str(self.location)+'\',\''+str(self.newPrice)+'\')'
            logger.info(sql_str)
            cursor.execute(sql_str)
            db.commit()
        cursor.close()
        
def get_time_diff():
    url="https://used-api.jd.com/auction/list?pageNo=1&pageSize=50&category1=&status=&orderDirection=1&orderType=1"    
    r=requests.get(url)
    obj=json.loads(r.text)
    auctionId=(obj['data']['auctionInfos'][0]['id'])
    #时间校准，校准当前服务器与京东服务器的时间差
    timestampHere=int(time.time()*1000)
    url="https://used-api.jd.com/auction/detail?auctionId="+str(auctionId)
    r=requests.get(url)
    obj=json.loads(r.text)
    timestampThere=int(obj['data']['currentTime'])
    return (timestampThere-timestampHere)
def batch_crawler(timediff,db):
    logger.info("batch start")
    url="https://used-api.jd.com/auction/list?pageNo=1&pageSize=50&category1=&status=&orderDirection=1&orderType=1"
    r=requests.get(url)
    obj=json.loads(r.text)
    endtime=0
    for item in obj['data']['auctionInfos']:
        auctionId=item['id']
        #获取距离该商品竞拍结束时间并增加一定的值
        timewait=(item['endTime']-timediff-int(time.time()*1000))/1000+10
        auction=Auction(auctionId,db)
        auction.start_thread(timewait)
        endtime=item['endTime']
    timewait=(endtime-timediff-int(time.time()*1000))/1000-1
    logger.info("wait "+str(timewait)+" seconds to execute next batch" )
    time.sleep(timewait)
    batch_crawler(timediff,db)
def logging_setup():
    #配置日志打印
    logger=logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt='[%(asctime)s]%(levelname)s:%(message)s',datefmt='%m/%d/%Y %I:%M:%S %p')
    file_logging=logging.FileHandler('auction_crawler.log')
    file_logging.setLevel(logging.INFO)
    logger.addHandler(file_logging)
    return logger

if __name__ == '__main__':
    reload(sys) 
    sys.setdefaultencoding('utf8') 
    #配置日志打印
    logger=logging_setup()
    #配置线程锁
    mutex = threading.Lock()
    #获取当前服务器与京东服务器的时间差值
    timediff=get_time_diff()
    logger.info("当前服务器比京东服务器慢"+str(timediff)+"秒")
    #连接数据库
    db = pymysql.connect("192.168.31.194","root","root","auction")
    #使用cursor()方法创建一个游标对象
    batch_crawler(timediff,db)

