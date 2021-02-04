import datetime
import tushare as ts
import pymysql
import config

if __name__ == '__main__':

    # 设置tushare pro的token并获取连接
    ts.set_token(config.token)
    pro = ts.pro_api()
    # 设定获取日线行情的初始日期和终止日期，其中终止日期设定为昨天。
    start_dt = '20100101'
    time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
    end_dt = time_temp.strftime('%Y%m%d')
    # 建立数据库连接,剔除已入库的部分
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='stock', charset='utf8')
    cursor = db.cursor()
    # 设定需要获取数据的股票池
    stock_codes = pro.stock_basic(exchange='',list_status='L',fields='ts_code,name')
    stock_pool = stock_codes['ts_code'].tolist()
    #stock_pool = stock_pool_all[551:555] + stock_pool_all[1658:1661] + stock_pool_all[2685:2691] + stock_pool_all[2734:2745] + stock_pool_all[2751:2752] + stock_pool_all[2762:2764]
    total = len(stock_pool)
    # 循环获取单个股票的日线行情,获取的数据还没有前复权
    for i in range(len(stock_pool)):
        #thiscode = str(stock_pool[i])
        #print(thiscode)
        #sql_delete = "DELETE FROM stock_all WHERE stock_code ='" + thiscode + "'"
        #cursor.execute(sql_delete)
        #db.commit()
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
    cursor.close()
    db.close()
    print('All Finished!')