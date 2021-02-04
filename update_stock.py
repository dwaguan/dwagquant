import datetime
import tushare as ts
import pymysql
import config
import time

if __name__ == '__main__':

    # 设置tushare pro的token并获取连接
    ts.set_token(config.token)
    pro = ts.pro_api()
    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='stock', charset='utf8')
    cursor = db.cursor()
    # 设定获取日线行情的初始日期和终止日期，其中初始日期设为数据库上次更新到的日期的下一天，终止日期设定为昨天。
    # 获取已有数据库的最早和最晚日期
    sql_select_first_date = "SELECT MIN(state_dt) FROM stock_all"
    cursor.execute(sql_select_first_date)
    first_start_dt = cursor.fetchone()[0] #格式'%Y-%m-%d'，需转换去掉杠
    first_start_dt = (datetime.datetime.strptime(first_start_dt, "%Y-%m-%d")).strftime('%Y%m%d')
    sql_select_last_date = "SELECT MAX(state_dt) FROM stock_all"
    cursor.execute(sql_select_last_date)
    pre_end_dt = cursor.fetchone()[0] #格式'%Y-%m-%d'，需转换去掉杠
    pre_end_dt = (datetime.datetime.strptime(pre_end_dt, "%Y-%m-%d")).strftime('%Y%m%d')
    # start_dt 是已有最晚日期后一天
    time_temp = datetime.datetime.strptime(pre_end_dt, "%Y%m%d") + datetime.timedelta(days=1)
    start_dt = time_temp.strftime('%Y%m%d')
    time_temp = datetime.datetime.now()
    end_dt = time_temp.strftime('%Y%m%d')
    # 设定需要获取数据的股票池
    stock_codes = pro.stock_basic(exchange='',list_status='L',fields='ts_code,name')
    stock_pool = stock_codes['ts_code'].tolist()
    #stock_pool = stock_pool_all[551:555] + stock_pool_all[1658:1661] + stock_pool_all[2685:2691] + stock_pool_all[2734:2745] + stock_pool_all[2751:2752] + stock_pool_all[2762:2764]
    # 将股票池分为上次更新后发生除权的组合上次更新后未发生除权的组
    # 获取复权因子，检测上次更新最新日起，除权因子最大值最小值是否相等，如果不相等，加入除权组，否则加入未除权组
    stock_pool_adj = [] 
    total = len(stock_pool)
    for i in range(len(stock_pool)):
        adjfactor = pro.adj_factor(ts_code=stock_pool[i],start_date=pre_end_dt, end_date=end_dt,fields="adj_factor")
        adjfactor = adjfactor['adj_factor'].tolist()
        print(str(i)+'/'+str(total)+'grouped')
        if adjfactor:
            if max(adjfactor) != min(adjfactor):
                popcode = stock_pool.pop(i)
                stock_pool_adj.append(popcode)
                print(popcode+'adjusted')
        time.sleep(0.2)
    # 对于未发生除权的组，循环获取单个股票的日线行情,获取的数据还没有前复权【【【改日期为此股票更新到的日期
    total = len(stock_pool)
    for i in range(len(stock_pool)):
        time.sleep(0.2)
        try:
            df = ts.pro_bar(ts_code=stock_pool[i], adj='qfq', start_date=start_dt, end_date=end_dt)
			# 打印进度
            print('Seq: ' + str(i+1) + ' of ' + str(total) + '   Code: ' + str(stock_pool[i]))
            c_len = df.shape[0]
        except Exception as aa:
            print(aa)
            print('No DATA Code: ' + str(i))
            continue
        for j in range(c_len):
            resu0 = list(df.iloc[c_len-1-j])
            resu = []
            for k in range(len(resu0)):
                if str(resu0[k]) == 'nan':
                    resu.append(-1)
                else:
                    resu.append(resu0[k])
            state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
            try:
                sql_insert = "INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as err:
                continue
    # 对于发生除权的组，删除后从数据库最早日期更新
    total = len(stock_pool_adj)
    for i in range(len(stock_pool_adj)):
        time.sleep(0.2)
        thiscode = str(stock_pool_adj[i])
        #print(thiscode)
        sql_delete = "DELETE FROM stock_all WHERE stock_code ='" + thiscode + "'"
        cursor.execute(sql_delete)
        db.commit()
        try:
            df = ts.pro_bar(ts_code=stock_pool_adj[i], adj='qfq', start_date=first_start_dt, end_date=end_dt)
			# 打印进度
            print('AdjSeq: ' + str(i+1) + ' of ' + str(total) + '   Code: ' + str(stock_pool_adj[i]))
            c_len = df.shape[0]
        except Exception as aa:
            print(aa)
            print('No DATA Code: ' + str(i))
            continue
        for j in range(c_len):
            resu0 = list(df.iloc[c_len-1-j])
            resu = []
            for k in range(len(resu0)):
                if str(resu0[k]) == 'nan':
                    resu.append(-1)
                else:
                    resu.append(resu0[k])
            state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
            try:
                sql_insert = "INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as err:
                continue    
    cursor.close()
    db.close()
    print('All Finished!')
