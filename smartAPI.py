# -*- coding:utf-8 -*-   #使用utf-8的字符集
import time
import datetime
import math
from WindPy import w
import pymysql.cursors
import utils.MyUtils
import utils.bssignal
import utils.bssignal1
import utils.sql_select_tailn
import utils.email_send
import numpy as np
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler


def my_job():
    STOCK_ASSETS = []
    # time Init
    allstart = time.time()
    #now = datetime.datetime.now()
    # 测试用数据
    now = datetime.datetime(2017,5,11,15,1,0)
    nowdatetime = now.strftime('%Y-%m-%d %H:%M:%S')
    nowdate = now.strftime('%Y-%m-%d')
    startcoll = datetime.datetime(2015,5,11)
    startdate = startcoll.strftime('%Y-%m-%d')

    # Wind Python Modules Init
    w.start()
    print("********************************")
    print("Wind数据中心在线API是否可用 %s" % (w.isconnected()))
    # MySQL Connection
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='123456',
                                 db='wind',
                                 port=3306,
                                 charset='utf8')
    # 从数据库获取当前所有的股票代码
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ss_code FROM stock_summary"
            ss_codes_cnt = cursor.execute(sql)
            ss_codes = cursor.fetchmany(ss_codes_cnt)
            for ii in ss_codes:
                STOCK_ASSETS.append(ii[0])
    finally:
        print("********************************")
        print("获取到数据库中所有的股票信息")

    # 获取今天全部的A股信息
    sectorconstituent = w.wset("sectorconstituent", "date=" + nowdate + ";sectorid=a001010100000000")
    if sectorconstituent.ErrorCode != 0:
        exit(-1)

    # 获取退市的股票代码，并从数据库中删除
    print('********************************')
    print('检查退市股票')
    needdelete = []
    for stock in STOCK_ASSETS:
        if stock not in sectorconstituent.Data[1]:
            needdelete.append(stock)
            try:
                with connection.cursor as cursor:
                    pseudo_code = utils.MyUtils.stockconvert(stock)
                    # 清空数据表，重新获取数据
                    cursor.execute("DROP TABLE `stock_data_week_" + pseudo_code + "` ")
                    cursor.execute("DROP TABLE `stock_data_day_" + pseudo_code + "` ")
                    cursor.execute("DROP TABLE `stock_data_hour_" + pseudo_code + "` ")
                    cursor.execute("DELETE FROM stock_summary where ss_code='%s'"%stock)
                    connection.commit()
            finally:
                print('退市股票删除完毕:%s'%needdelete )

    for stock in needdelete:
        id = STOCK_ASSETS.index(stock)
        del STOCK_ASSETS[id]

    # 获取数据库中已有股票的复权信息
    rec_code = []
    if len(STOCK_ASSETS) > 0:
        try:
            with connection.cursor() as cursor:
                DAY_OFFSET = -1
                d_today = nowdate
                d_before_yesterday = w.tdaysoffset(DAY_OFFSET, d_today.replace('-', '')).Data[0][0].strftime('%Y-%m-%d')
                print("获取复权因子，日期范围[%s ~ %s]" % (d_before_yesterday, d_today))
                ret = w.wsd(STOCK_ASSETS, "adjfactor", d_before_yesterday, d_today, "")
                adjfactor = ret.Data
                for i in range(len(ret.Codes)):
                    if adjfactor[i][0] != adjfactor[i][1]:
                        rec_code.append(ret.Codes[i])
                        pseudo_code = utils.MyUtils.stockconvert(ret.Codes[i])
                        # 清空数据表，重新获取数据
                        cursor.execute("TRUNCATE  `stock_data_week_" + pseudo_code + "` ")
                        cursor.execute("TRUNCATE  `stock_data_day_" + pseudo_code + "` ")
                        cursor.execute("TRUNCATE  `stock_data_hour_" + pseudo_code + "` ")
                        connection.commit()
        finally:
            print("复权因子发生变化的股票列表 : %s" % rec_code)

    # 从Wind获取所有A股股票的上市日期
    all_market_date = w.wss(sectorconstituent.Data[1], 'ipo_date')
    stockdict = {}
    for i in range(len(all_market_date.Codes)):
        stock = all_market_date.Codes[i]
        stockdict[stock] = all_market_date.Data[0][i]

    # 保存需要插入的股票数据(上市日期满一年才可进入数据库)
    print("********************************")
    print("检查新增股票")
    needinsertion = []
    needcreation = []
    i = 0
    for stock in sectorconstituent.Codes:
        ss_market = "SH"  # 默认市场为上证
        ss_code = sectorconstituent.Data[1][i]
        ss_name = sectorconstituent.Data[2][i]
        i += 1
        if ss_code in STOCK_ASSETS:
            continue
        else:
            if "SZ" in ss_code or "sz" in ss_code:  # 设置市场
                ss_market = "SZ"
            ssmarket_date = stockdict[ss_code]
            if (now-ssmarket_date).days >= 365:
                needinsertion.append([ss_code, ss_name, ss_market, ssmarket_date])
                needcreation.append(ss_code)

    # 测试用数据
    #needcreation = needcreation[:4]
    #needinsertion = needinsertion[:4]

    if len(needcreation) > 0:
        print("新增股票列表 : %s" % needcreation)
    else:
        print("无新增股票")

    # 创建数据库里面没有的表week day hour
    if len(needinsertion) > 0:
        print("********************************")
        print("开始创建新增股票所需数据表")
        start = time.time()
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO stock_summary (ss_code, ss_name, ss_market ,ss_marketdate) VALUES (%s, %s, %s ,%s)"
                cursor.executemany(sql, needinsertion)
                for code in needcreation:
                    code = utils.MyUtils.stockconvert(code)
                    sql = "CREATE TABLE IF NOT EXISTS `stock_data_week_" + code + "` " \
                          "(  " \
                          "`_id` int(11) NOT NULL AUTO_INCREMENT,  " \
                          "`open` float(16) DEFAULT NULL COMMENT '开盘价\n.2f元',  " \
                          "`high` float(16) DEFAULT NULL COMMENT '最高价\n.2f元',  " \
                          "`low` float(16) DEFAULT NULL COMMENT '最低价\n.2f元',  " \
                          "`close` float(16) DEFAULT NULL COMMENT '收盘价\n.2f元',  " \
                          "`bs_signal_rule1` varchar(10) DEFAULT NULL COMMENT '规则1信号',"\
                          "`datetime` datetime DEFAULT NULL COMMENT '时间',  " \
                          "`EMAb_arg3` float(16) DEFAULT 0 COMMENT 'EMA_N为3的买入信号'," \
                          "`EMAb_arg5` float(16) DEFAULT 0 COMMENT 'EMA_N为5的买入信号'," \
                          "`EMAb_arg8` float(16) DEFAULT 0 COMMENT 'EMA_N为8的买入信号'," \
                          "`EMAb_arg13` float(16) DEFAULT 0 COMMENT 'EMA_N为13的买入信号'," \
                          "`EMAs1` float(16) DEFAULT 0 COMMENT '卖出信号1'," \
                          "`EMAs2` float(16) DEFAULT 0 COMMENT '卖出信号2'," \
                          "`guide` float(16) DEFAULT 0 COMMENT '指导线',"\
                          "`MA` float(16) DEFAULT 0 COMMENT '移动平均',"\
                          "PRIMARY KEY (`_id`),  " \
                          "UNIQUE KEY `_id_UNIQUE` (`_id`)) " \
                          "ENGINE=InnoDB " \
                          "DEFAULT CHARSET=utf8;"
                    # cursor.executemany(sql.replace("'", ""), needcreation)
                    cursor.execute(sql)
                    sql = "CREATE TABLE IF NOT EXISTS `stock_data_day_" + code + "` " \
                         "(  " \
                         "`_id` int(11) NOT NULL AUTO_INCREMENT,  " \
                         "`open` float(16) DEFAULT NULL COMMENT '开盘价\n.2f元',  " \
                         "`high` float(16) DEFAULT NULL COMMENT '最高价\n.2f元',  " \
                         "`low` float(16) DEFAULT NULL COMMENT '最低价\n.2f元',  " \
                         "`close` float(16) DEFAULT NULL COMMENT '收盘价\n.2f元',  " \
                         "`bs_signal_rule1` varchar(10) DEFAULT NULL COMMENT '规则1信号'," \
                         "`datetime` datetime DEFAULT NULL COMMENT '时间',  " \
                         "`EMAb_arg3` float(16) DEFAULT 0 COMMENT 'EMA_N为3的买入信号'," \
                         "`EMAb_arg5` float(16) DEFAULT 0 COMMENT 'EMA_N为5的买入信号'," \
                         "`EMAb_arg8` float(16) DEFAULT 0 COMMENT 'EMA_N为8的买入信号'," \
                         "`EMAb_arg13` float(16) DEFAULT 0 COMMENT 'EMA_N为13的买入信号'," \
                         "`EMAs1` float(16) DEFAULT 0 COMMENT '卖出信号1'," \
                         "`EMAs2` float(16) DEFAULT 0 COMMENT '卖出信号2'," \
                         "`guide` float(16) DEFAULT 0 COMMENT '指导线'," \
                         "`MA` float(16) DEFAULT 0 COMMENT '移动平均'," \
                         "PRIMARY KEY (`_id`),  " \
                         "UNIQUE KEY `_id_UNIQUE` (`_id`)) " \
                         "ENGINE=InnoDB " \
                         "DEFAULT CHARSET=utf8;"
                    # cursor.executemany(sql, needcreation)
                    cursor.execute(sql)
                    sql = "CREATE TABLE IF NOT EXISTS `stock_data_hour_" + code + "` " \
                          "(  " \
                          "`_id` int(11) NOT NULL AUTO_INCREMENT,  " \
                          "`open` float(16) DEFAULT NULL COMMENT '开盘价\n.2f元',  " \
                          "`high` float(16) DEFAULT NULL COMMENT '最高价\n.2f元',  " \
                          "`low` float(16) DEFAULT NULL COMMENT '最低价\n.2f元',  " \
                          "`close` float(16) DEFAULT NULL COMMENT '收盘价\n.2f元',  " \
                          "`bs_signal_rule1` varchar(10) DEFAULT NULL COMMENT '规则1信号'," \
                          "`datetime` datetime DEFAULT NULL COMMENT '时间',  " \
                          "`EMAb_arg3` float(16) DEFAULT 0 COMMENT 'EMA_N为3的买入信号'," \
                          "`EMAb_arg5` float(16) DEFAULT 0 COMMENT 'EMA_N为5的买入信号'," \
                          "`EMAb_arg8` float(16) DEFAULT 0 COMMENT 'EMA_N为8的买入信号'," \
                          "`EMAb_arg13` float(16) DEFAULT 0 COMMENT 'EMA_N为13的买入信号'," \
                          "`EMAs1` float(16) DEFAULT 0 COMMENT '卖出信号1'," \
                          "`EMAs2` float(16) DEFAULT 0 COMMENT '卖出信号2'," \
                          "`guide` float(16) DEFAULT 0 COMMENT '指导线'," \
                          "`MA` float(16) DEFAULT 0 COMMENT '移动平均'," \
                          "PRIMARY KEY (`_id`), " \
                          "UNIQUE KEY `_id_UNIQUE` (`_id`))" \
                          "ENGINE=InnoDB " \
                          "DEFAULT CHARSET=utf8;"
                    # cursor.executemany(sql, needcreation)
                    cursor.execute(sql)
                    print("新增[%s]结束..." % code)
                connection.commit()
        finally:
            print("所有新增股票数据表创建完毕\n耗时 %.2f s" % (time.time() - start))

    # 将复权因子发生变化的股票以及股票池中没有的股票合并，用同样的程序进行更新
    needcreation.extend(rec_code)

    # 更新股票数据
    print("********************************")
    print("开始增量同步股票数据")
    #needcreation = needcreation[329:]
    if len(needcreation) > 0:
        try:
            with connection.cursor() as cursor:
                for code in needcreation:
                    pseudo_code = utils.MyUtils.stockconvert(code)
                    ssmarket_date = stockdict[code]
                    print("开始处理[%s]..." % code)
                    wus = startcoll
                    dus = startcoll
                    hus = startcoll

                    if ssmarket_date.strftime('%Y-%m-%d') > startcoll.strftime('%Y-%m-%d'):
                        wus = ssmarket_date
                        dus = ssmarket_date
                        hus = ssmarket_date

                    usdate = dus.strftime('%Y-%m-%d')
                    suspstatus = list(w.wsd(code, "trade_status",usdate , usdate, "").Data[0][0])
                    if '停' in suspstatus:
                        suspstatus = w.wsd(code, "trade_status", usdate, nowdate, "")
                        if '交易' in suspstatus.Data[0]:
                            id = suspstatus.Data[0].index('交易')
                            ontrade = suspstatus.Times[id]
                            wus = ontrade
                            dus = ontrade
                            hus = ontrade
                        else:
                            cursor.execute("DROP TABLE `stock_data_week_" + pseudo_code + "` ")
                            cursor.execute("DROP TABLE `stock_data_day_" + pseudo_code + "` ")
                            cursor.execute("DROP TABLE `stock_data_hour_" + pseudo_code + "` ")
                            cursor.execute("DELETE FROM stock_summary where ss_code='%s'" % code)
                            connection.commit()
                            continue


                    # 获取该支股票的全部小时数据
                    husstr = hus.strftime('%Y-%m-%d %H:%M:%S')
                    print("开始下载[%s]时数据，日期范围[%s ~ %s]" % (code, husstr, nowdatetime))
                    ret = w.wsi(code, "open,high,low,close", husstr, nowdatetime,"BarSize=60;PriceAdj=F;Fill=Previous")
                    # 加一个断网判断程序


                    EMAb_arg3 = np.array(utils.bssignal1.EMA_buy(ret.Data[3])).tolist()
                    EMAb_arg5 = np.array(utils.bssignal1.EMA_buy(ret.Data[3],EMA_N=5)).tolist()
                    EMAb_arg8 = np.array(utils.bssignal1.EMA_buy(ret.Data[3], EMA_N=8)).tolist()
                    EMAb_arg13 = np.array(utils.bssignal1.EMA_buy(ret.Data[3], EMA_N=13)).tolist()
                    EMAs1 = np.array(utils.bssignal1.EMA_sell(ret.Data[3],13,89,21)).tolist()
                    EMAs2 = np.array(utils.bssignal1.EMA_sell(ret.Data[3],21,55,20)).tolist()
                    guide1 = (np.array(EMAb_arg3) + np.array(EMAb_arg5) + np.array(EMAb_arg8) + np.array(EMAb_arg13))/4
                    guide1 = guide1.tolist()
                    guide = np.array(utils.bssignal1.EMA_buy(guide1)).tolist()
                    MA = np.array(utils.bssignal1.MA(ret.Data[3])).tolist()
                    # 确认是否有买卖信号
                    bs_signal_rule1 = utils.bssignal1.rule1(EMAb_arg3, EMAs1)

                    params = ret.Data
                    params.extend([bs_signal_rule1,EMAb_arg3, EMAb_arg5, EMAb_arg8, EMAb_arg13, EMAs1, EMAs2, guide, MA])
                    params.append(ret.Times)
                    params = np.array(params[:14]).T.tolist()

                    sql = 'REPLACE INTO stock_data_hour_' + pseudo_code + \
                          ' (open, high, low, close,bs_signal_rule1,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,EMAs2,guide,MA,datetime)' \
                          ' VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.executemany(sql, params)
                    sql = 'UPDATE stock_summary set ' \
                          'ss_hour_update_status = 1, ' \
                          'ss_hour_update_start = %s, ss_hour_update_end = %s ' \
                          'where ss_code = %s'
                    cursor.execute(sql, (hus, now, code))

                    # 获取该支股票的全部天数据
                    dusstr = dus.strftime('%Y-%m-%d')
                    print("开始下载[%s]日数据，日期范围[%s ~ %s]" % (code, dusstr, nowdate))
                    ret = w.wsd(code, "open,high,low,close", dusstr, nowdate,"Period=D;Fill=Previous;PriceAdj=F")
                    EMAb_arg3 = np.array(utils.bssignal1.EMA_buy(ret.Data[3])).tolist()
                    EMAb_arg5 = np.array(utils.bssignal1.EMA_buy(ret.Data[3],EMA_N=5)).tolist()
                    EMAb_arg8 = np.array(utils.bssignal1.EMA_buy(ret.Data[3], EMA_N=8)).tolist()
                    EMAb_arg13 = np.array(utils.bssignal1.EMA_buy(ret.Data[3], EMA_N=13)).tolist()
                    EMAs1 = np.array(utils.bssignal1.EMA_sell(ret.Data[3],13,89,21)).tolist()
                    EMAs2 = np.array(utils.bssignal1.EMA_sell(ret.Data[3],21,55,20)).tolist()
                    guide1 = (np.array(EMAb_arg3) + np.array(EMAb_arg5) + np.array(EMAb_arg8) + np.array(EMAb_arg13))/4
                    guide1 = guide1.tolist()
                    guide = np.array(utils.bssignal1.EMA_buy(guide1)).tolist()
                    MA = np.array(utils.bssignal1.MA(ret.Data[3])).tolist()
                    # 确认是否有买卖信号
                    bs_signal_rule1 = utils.bssignal1.rule1(EMAb_arg3, EMAs1)


                    params = ret.Data# 确认是否有买卖信号
                    params.extend([bs_signal_rule1,EMAb_arg3, EMAb_arg5, EMAb_arg8, EMAb_arg13, EMAs1, EMAs2, guide, MA])
                    params.append(ret.Times)
                    params = np.array(params[:14]).T.tolist()

                    sql = 'REPLACE INTO stock_data_day_' + pseudo_code + \
                          ' (open, high, low, close,bs_signal_rule1,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,EMAs2,guide,MA,datetime)' \
                          ' VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.executemany(sql, params)
                    sql = 'UPDATE stock_summary set ' \
                          'ss_day_update_status = 1, ' \
                          'ss_day_update_start = %s, ss_day_update_end = %s ' \
                          'where ss_code = %s'
                    cursor.execute(sql, (dus, now, code))

                    # 获取该支股票的全部周数据
                    wusstr = wus.strftime('%Y-%m-%d')
                    print("开始下载[%s]周数据，日期范围[%s ~ %s]" % (code, wusstr, nowdate))
                    ret = w.wsd(code, "open,high,low,close", wusstr, nowdate,"Period=W;Days=Weekdays;Fill=Previous;PriceAdj=F")
                    EMAb_arg3 = np.array(utils.bssignal1.EMA_buy(ret.Data[3])).tolist()
                    EMAb_arg5 = np.array(utils.bssignal1.EMA_buy(ret.Data[3],EMA_N=5)).tolist()
                    EMAb_arg8 = np.array(utils.bssignal1.EMA_buy(ret.Data[3], EMA_N=8)).tolist()
                    EMAb_arg13 = np.array(utils.bssignal1.EMA_buy(ret.Data[3], EMA_N=13)).tolist()
                    EMAs1 = np.array(utils.bssignal1.EMA_sell(ret.Data[3],13,89,21)).tolist()
                    EMAs2 = np.array(utils.bssignal1.EMA_sell(ret.Data[3],21,55,20)).tolist()
                    guide1 = (np.array(EMAb_arg3) + np.array(EMAb_arg5) + np.array(EMAb_arg8) + np.array(EMAb_arg13))/4
                    guide1 = guide1.tolist()
                    guide = np.array(utils.bssignal1.EMA_buy(guide1)).tolist()
                    MA = np.array(utils.bssignal1.MA(ret.Data[3])).tolist()
                    # 确认是否有买卖信号
                    bs_signal_rule1 = utils.bssignal1.rule1(EMAb_arg3, EMAs1)

                    params = ret.Data
                    params.extend([bs_signal_rule1,EMAb_arg3, EMAb_arg5, EMAb_arg8, EMAb_arg13, EMAs1, EMAs2, guide, MA])
                    params.append(ret.Times)
                    params = np.array(params[:14]).T.tolist()
                    update_de = ret.Times[-1]

                    if now.weekday() in [0, 1, 2, 3]:
                        params = params[:-1]
                        update_de = ret.Times[-2:-1]

                    sql = 'REPLACE INTO stock_data_week_' + pseudo_code + \
                          ' (open, high, low, close,bs_signal_rule1,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,EMAs2,guide,MA,datetime)' \
                          ' VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    cursor.executemany(sql, params)
                    sql = 'UPDATE stock_summary set ' \
                          'ss_week_update_status = 1, ' \
                          'ss_week_update_start = %s, ss_week_update_end = %s ' \
                          'where ss_code = %s'
                    cursor.execute(sql, (wus, update_de, code))
                    connection.commit()
        finally:
            print("上市满一年的新股以及需重新复权的股票已更新完毕")

    # 更新股票池中已有的股票
    STOCK_ASSETS = []
    try:
        with connection.cursor() as cursor:
            data_cnt = cursor.execute("SELECT ss_code FROM stock_summary")
            data = cursor.fetchmany(data_cnt)
            for ii in data:
                STOCK_ASSETS.append(ii[0])
    finally:
        print('股票池中已有股票查询完毕')
    hav_stock = list(set(STOCK_ASSETS).difference(needcreation))

    if len(hav_stock) > 0:
        hour_start = datetime.datetime(now.year,now.month,now.day,10,0,0).strftime('%Y-%m-%d %H:%M:%S')
        number = 100
        bracket = int(len(hav_stock) / number)
        datadf = pd.DataFrame()
        print('需要循环%s次得到今天的所有股票数据'%bracket)
        for i in range(bracket):
            bracket_slice = hav_stock[i*number : (i+1)*number]
            ret = w.wsi(bracket_slice,"open,high,low,close",hour_start, nowdatetime, 'Barsize=60;PriceAdj=F;Fill=Previous')
            # 判断ret是否正常获得的语句
            if ret.ErrorCode != 0:
                print(i)
                #exit(1)
            ssd = ret.Data[2:]
            ssd.append(ret.Data[0])
            datadf1 = pd.DataFrame(ssd,columns=ret.Data[1])
            datadf = pd.concat([datadf,datadf1],axis=1)
            print("第%s次循环结束"%i)
        bracket_slice = hav_stock[bracket*number:]
        ret = w.wsi(bracket_slice, "open,high,low,close", hour_start, nowdatetime,'Barsize=60;PriceAdj=F;Fill=Previous')
        ssd = ret.Data[2:]
        ssd.append(ret.Data[0])
        datadf1 = pd.DataFrame(ssd, columns=ret.Data[1])
        datadf = pd.concat([datadf, datadf1], axis=1)

        ToF = np.isnan(list(np.array(datadf)[0,:]))
        nanum = sum([1 if False else 0 for i in ToF])
        if nanum ==0 :
        #print(datadf)

            try:
                with connection.cursor() as cursor:
                    for code in hav_stock:
                        pseudo_code = utils.MyUtils.stockconvert(code)
                        stockdf = datadf[code].values.tolist()

                        # 查询该支股票的后(slope_N-1)行数据，得到信号1的bs数据
                        slope_N = 13
                        extraray = utils.sql_select_tailn.sql_tail_hour('close,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,guide'
                                                                        ,cursor,pseudo_code,(slope_N-1))
                        close = extraray[:,0].tolist()
                        close.extend(stockdf[3])
                        closeb = [extraray[-1,0]]
                        closeb.extend(stockdf[3])

                        hour_EMAs1 = np.array(utils.bssignal.EMA_sell(close, extraray[:,5].tolist(),13,89,21)).tolist()[(slope_N - 1):]
                        hour_EMAb_arg3 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1,1]])).tolist()[1:]
                        hour_EMAb_arg5 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1,2]])).tolist()[1:]
                        hour_EMAb_arg8 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1,3]])).tolist()[1:]
                        hour_EMAb_arg13 = np.array(utils.bssignal.EMA_buy(closeb,[extraray[-1,4]])).tolist()[1:]
                        guide1 = (np.array(hour_EMAb_arg3)+np.array(hour_EMAb_arg5)+np.array(hour_EMAb_arg8)+np.array(hour_EMAb_arg13))/4
                        guide1 = guide1.tolist()
                        guide_data = [0]
                        guide_data.extend(guide1)
                        hour_guide = np.array(utils.bssignal.EMA_buy(guide_data,[extraray[-1,6]])).tolist()[1:]
                        #产生买卖信号
                        hour_EMAb = [extraray[-1,1]]
                        hour_EMAb.extend(hour_EMAb_arg3)
                        hour_EMAs = [extraray[-1,5]]
                        hour_EMAs.extend(hour_EMAs1)
                        hour_bs_signal_rule1 = utils.bssignal.rule1(EMAb=hour_EMAb,EMAs=hour_EMAs)[1:]

                        # 查询该支股票的后(slope_N-1)行数据，得到信号2的bs数据
                        slope_N = 21
                        extraray = utils.sql_select_tailn.sql_tail_hour('close,EMAs2',cursor, pseudo_code, (slope_N-1))
                        close = extraray[:,0].tolist()
                        close.extend(stockdf[3])
                        hour_EMAs2 = np.array(utils.bssignal.EMA_sell(close, extraray[:,1].tolist(),21,55,20)).tolist()[(slope_N - 1):]

                        # 查询该支股票的后（MA_N-1）行数据，得到MA指标
                        MA_N = 55
                        extraray = utils.sql_select_tailn.sql_tail_hour('close',cursor, pseudo_code,(MA_N - 1))
                        close = extraray[:,0].tolist()
                        close.extend(stockdf[3])
                        hour_MA = np.array(utils.bssignal.MA(close)).tolist()


                        # 得到需要写入sql的数据
                        params_hour = []
                        for i in range(len(stockdf[0])):
                            iopen = stockdf[0][i]
                            ihigh = stockdf[1][i]
                            ilow = stockdf[2][i]
                            iclose = stockdf[3][i]
                            ihour_bs_signal_rule1 = hour_bs_signal_rule1[i]
                            iEMAb_arg3 = hour_EMAb_arg3[i]
                            iEMAb_arg5 = hour_EMAb_arg5[i]
                            iEMAb_arg8 = hour_EMAb_arg8[i]
                            iEMAb_arg13 = hour_EMAb_arg13[i]
                            iEMAs1 = hour_EMAs1[i]
                            iEMAs2 = hour_EMAs2[i]
                            iguide = hour_guide[i]
                            iMA = hour_MA[i]
                            idatetime = stockdf[4][i]
                            params1 = [iopen,ihigh,ilow,iclose,ihour_bs_signal_rule1,iEMAb_arg3,iEMAb_arg5,iEMAb_arg8,iEMAb_arg13,iEMAs1,iEMAs2,iguide,iMA,idatetime]
                            params_hour.append(params1)

                        # 通过小时数据构造天数据
                        day_close = stockdf[3][-1]
                        day_open = stockdf[0][0]
                        day_high = max(stockdf[1])
                        day_low = min(stockdf[2])
                        day_datetime = datetime.datetime(now.year,now.month,now.day,0,0,0)

                        # 查询该支股票的后(slope_N-1)行数据，得到信号1的bs数据
                        slope_N = 13
                        extraray = utils.sql_select_tailn.sql_tail_day('close,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,guide'
                                                                        ,cursor,pseudo_code,(slope_N-1))
                        close = extraray[:,0].tolist()
                        close.append(day_close)
                        closeb = [extraray[-1,0],day_close]

                        day_EMAs1 = np.array(utils.bssignal.EMA_sell(close, extraray[:,5].tolist(), 13, 89, 21)).tolist()[(slope_N - 1):][0]
                        day_EMAb_arg3 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 1]])).tolist()[1:][0]
                        day_EMAb_arg5 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 2]])).tolist()[1:][0]
                        day_EMAb_arg8 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 3]])).tolist()[1:][0]
                        day_EMAb_arg13 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 4]])).tolist()[1:][0]
                        guide1 = (day_EMAb_arg3 + day_EMAb_arg5 + day_EMAb_arg8 + day_EMAb_arg13) / 4
                        guide_data = [0,guide1]
                        day_guide = np.array(utils.bssignal.EMA_buy(guide_data, [extraray[-1,6]])).tolist()[1:][0]

                        # 产生买卖信号
                        day_EMAb = [extraray[-1, 1]]
                        day_EMAb.append(day_EMAb_arg3)
                        day_EMAs = [extraray[-1, 5]]
                        day_EMAs.append(day_EMAs1)
                        day_bs_signal_rule1 = utils.bssignal.rule1(EMAb=day_EMAb, EMAs=day_EMAs)[1]

                        # 查询该支股票的后(slope_N-1)行数据，得到信号2的bs数据
                        slope_N = 21
                        extraray = utils.sql_select_tailn.sql_tail_day('close,EMAs2',cursor, pseudo_code, (slope_N - 1))
                        close = extraray[:,0].tolist()
                        close.append(day_close)
                        day_EMAs2 = np.array(utils.bssignal.EMA_sell(close, extraray[:,1].tolist(),21,55,20)).tolist()[(slope_N - 1):][0]

                        # 查询该支股票的后（MA_N-1）行数据，得到MA指标
                        MA_N = 55
                        extraray = utils.sql_select_tailn.sql_tail_day('close', cursor, pseudo_code, (MA_N - 1))
                        close = extraray[:,0].tolist()
                        close.extend(stockdf[3])
                        day_MA = np.array(utils.bssignal.MA(close)).tolist()[0]

                        # 构造需要写入sql的数据
                        params_day = [day_open,day_high,day_low,day_close,day_bs_signal_rule1,day_EMAb_arg3,day_EMAb_arg5,day_EMAb_arg8,
                                      day_EMAb_arg13,day_EMAs1,day_EMAs2,day_guide,day_MA,day_datetime]

                        # 构造周数据
                        if now.weekday() == 4:
                            week_close = day_close
                            week_open = extraray[-4,3]

                            high_for4 = list(extraray[-4:,4])
                            high_for4.append(day_high)
                            week_high = max(high_for4)

                            low_for4 = list(extraray[-4:,5])
                            low_for4.append(day_low)
                            week_low = min(low_for4)

                            week_datetime = day_datetime

                            # 查询该支股票的后(slope_N-1)行数据，得到信号1的bs数据
                            slope_N = 13
                            extraray = utils.sql_select_tailn.sql_tail_week('close,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,guide',
                                                                            cursor, pseudo_code, (slope_N - 1))
                            close = extraray[:,0].tolist()
                            close.append(week_close)
                            closeb = [extraray[-1,0],week_close]

                            week_EMAs1 = np.array(utils.bssignal.EMA_sell(close, extraray[:, 5].tolist(), 13, 89, 21)).tolist()[(slope_N - 1):][0]
                            week_EMAb_arg3 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 1]])).tolist()[1:][0]
                            week_EMAb_arg5 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 2]])).tolist()[1:][0]
                            week_EMAb_arg8 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 3]])).tolist()[1:][0]
                            week_EMAb_arg13 = np.array(utils.bssignal.EMA_buy(closeb, [extraray[-1, 4]])).tolist()[1:][0]
                            guide1 = (week_EMAb_arg3 + week_EMAb_arg5 + week_EMAb_arg8 + week_EMAb_arg13) / 4
                            guide_data = [0, guide1]
                            week_guide = np.array(utils.bssignal.EMA_buy(guide_data, [extraray[-1, 6]])).tolist()[1:][0]

                            # 产生买卖信号
                            week_EMAb = [extraray[-1, 1]]
                            week_EMAb.append(week_EMAb_arg3)
                            week_EMAs = [extraray[-1, 5]]
                            week_EMAs.append(week_EMAs1)
                            week_bs_signal_rule1 = utils.bssignal.rule1(EMAb=week_EMAb, EMAs=week_EMAs)[1]

                            # 查询该支股票的后(slope_N-1)行数据，得到信号2的bs数据
                            slope_N = 21
                            extraray = utils.sql_select_tailn.sql_tail_week('close,EMAs2',cursor, pseudo_code, (slope_N - 1))
                            close = extraray[:,0].tolist()
                            close.append(week_close)
                            week_EMAs2 = np.array(utils.bssignal.EMA_sell(close, extraray[:,1].tolist(),21,55,20)).tolist()[(slope_N - 1):][0]

                            # 查询该支股票的后（MA_N-1）行数据，得到MA指标
                            MA_N = 55
                            MA_cnt = cursor.execute('select MA from stock_data_week_' + pseudo_code)
                            if MA_cnt >= (MA_N-1):
                                extraray = utils.sql_select_tailn.sql_tail_week('close', cursor, pseudo_code, (MA_N - 1))
                                close = extraray[:,0].tolist()
                                close.extend(stockdf[3])
                                week_MA = np.array(utils.bssignal.MA(close)).tolist()[0]
                            else:
                                week_MA = 0

                            # 构造需要写入sql的数据
                            params_week = [week_open,week_high,week_low,week_close,week_bs_signal_rule1,week_EMAb_arg3,week_EMAb_arg5,week_EMAb_arg8,week_EMAb_arg13
                            ,week_EMAs1,week_EMAs2,week_guide,week_MA,week_datetime]

                            sql = 'REPLACE INTO stock_data_week_' + pseudo_code + \
                                  ' (open, high, low, close,bs_signal_rule1,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,EMAs2,guide,MA,datetime)' \
                                  ' VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                            cursor.execute(sql, params_week)

                        update_de = datetime.datetime(now.year,now.month,now.day,15,1,0)
                        hus = day_datetime
                        sql = 'REPLACE INTO stock_data_hour_' + pseudo_code + \
                              ' (open, high, low, close,bs_signal_rule1,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,EMAs2,guide,MA,datetime)' \
                              ' VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                        cursor.executemany(sql, params_hour)


                        sql = 'REPLACE INTO stock_data_day_' + pseudo_code + \
                              ' (open, high, low, close,bs_signal_rule1,EMAb_arg3,EMAb_arg5,EMAb_arg8,EMAb_arg13,EMAs1,EMAs2,guide,MA,datetime)' \
                              ' VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                        cursor.execute(sql, params_day)

                        sql = 'UPDATE stock_summary set ' \
                              'ss_hour_update_status = 1, ' \
                              'ss_day_update_status = 1, '\
                              'ss_week_update_status = 1, ' \
                              'ss_hour_update_start = %s, ss_hour_update_end = %s, ' \
                              'ss_day_update_start = %s, ss_day_update_end = %s ,'\
                              'ss_week_update_start = %s, ss_week_update_end = %s ' \
                              'where ss_code = %s'
                        cursor.execute(sql, (hus, update_de, hus, update_de, hus, update_de,code))
                        connection.commit()
                        print("%s今日股票数据更新完毕" % code)
            finally:
                print("********************************")
                print("本次数据更新全部结束\n耗时:%.2f s" % (time.time() - allstart))
                connection.close()
                print("数据库安全关闭")
                print("祝您生活愉快!")

        else:
            print("存在NAN类型的数据")
            connection.close()

my_job()
#sched = BlockingScheduler()  # 创建调度器
#sched.add_job(my_job, 'cron', day_of_week = '0-4',hour = '17',minute = '29',id='my_job')  # 执行时间点执行
#sched.start()



