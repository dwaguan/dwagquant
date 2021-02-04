from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
#import os.path  # To manage paths
#import sys  # To find out the script name (in argv[0])
import tushare as ts
import config
import pandas as pd
import pymysql
import calendar
from myprintAnalyzer import *
from stampdutycommision import *
# Import the backtrader platform
import backtrader as bt

# Set TuShare token
ts.set_token(config.token)
pro = ts.pro_api()
# Get data from MYSQL LOCAL DATABASE
def acquire_data(stock, start_date, end_date):
    db = pymysql.connect(host='127.0.0.1', user='root', passwd='123456', db='stock', charset='utf8')
    cursor = db.cursor()
    sql_done_set = "SELECT state_dt,stock_code,open,high,low,close,vol FROM stock_all a where stock_code = '%s' and state_dt >= '%s' and state_dt <= '%s' order by state_dt asc" % (stock, start_date, end_date)
    cursor.execute(sql_done_set)
    done_set = cursor.fetchall()
    if len(done_set) == 0:
        print(stock + ' no data')
        return pd.DataFrame()
    df = pd.DataFrame(done_set, columns=['datetime', 'code','open', 'high', 'low',
        'close', 'volume'])
    '''
    date_seq = []
    open_list = []
    high_list = []
    low_list = []
    close_list = []
    vol_list = []
    for i in range(len(done_set)):
        date_seq.append(done_set[i][0])
        open_list.append(float(done_set[i][2]))
        high_list.append(float(done_set[i][3]))
        low_list.append(float(done_set[i][4]))
        close_list.append(float(done_set[i][5]))
        vol_list.append(float(done_set[i][6]))
    '''
    cursor.close()
#    df = pd.DataFrame({'dateseq':date_seq,'open':open_list,'high':high_list,'low':low_list,'close':close_list,'volume':vol_list})
    df.index = pd.to_datetime(df.datetime)
#    df = df.sort_index()
#    df['datetime'] = pd.to_datetime(df.dateseq)
    df['openinterest'] = 0
    df = df[['open', 'high', 'low',
        'close', 'volume', 'openinterest']]
    df = df.fillna(0)
    return df

def acquire_stock_list(indexpool, end_date):
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    start_dt = end_dt - datetime.timedelta(days=31)
    start_dt = start_dt.strftime('%Y%m%d')
    #_, num_days = calendar.monthrange(end_dt.year, end_dt.month)
    #end_dt = end_dt.replace(day=num_days).strftime('%Y%m%d')
    end_dt = end_dt.strftime('%Y%m%d')
    stock_list = []
    for code in indexpool:
        df = pro.index_weight(index_code = code,start_date=start_dt,end_date=end_dt,fields='con_code,trade_date')
        max_date = max(df['trade_date'].tolist())
        df = df[df['trade_date'] == max_date]
        df = df['con_code'].tolist()
        stock_list += df
    stock_list = list(dict.fromkeys(stock_list))
    print('Number of stocks is: ' + str(len(stock_list)))
    return stock_list

#针对多个指数的功能还没写好
def acquire_index_data(index_code,start_date,end_date):
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime('%Y%m%d')
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime('%Y%m%d')
    df = ts.pro_bar(ts_code = index_code,asset='I',start_date=start_dt,end_date=end_dt)
    df['datetime'] = df.trade_date
    df['volume'] = df.vol
    df.index = pd.to_datetime(df.datetime)
    df['openinterest'] = 0
    df = df[['open', 'high', 'low',
        'close', 'volume', 'openinterest']]
    df = df.fillna(0)
    df = df.sort_index()
    return df

class stampDutyCommissionScheme(bt.CommInfoBase):
    '''
    本佣金模式下，买入股票仅支付佣金，卖出股票支付佣金和印花税.    
    '''
    params = (
        ('stamp_duty', 0.5), # 印花税率
        ('commission', 0.1), # 佣金率
        ('stocklike', True),
        ('commtype', bt.CommInfoBase.COMM_PERC),
        )
 
    def _getcommission(self, size, price, pseudoexec):
        '''
        If size is greater than 0, this indicates a long / buying of shares.
        If size is less than 0, it idicates a short / selling of shares.
        '''
        #print('self.p.commission',self.p.commission)
        if size > 0: # 买入，不考虑印花税
            return  size * price * self.p.commission * 100
        elif size < 0: # 卖出，考虑印花税
            return - size * price * (self.p.stamp_duty + self.p.commission*100)
        else:
            return 0 #just in case for some reason the size is 0.
            
# Create a Stratey

class TestStrategy(bt.Strategy):
    params = (
        #('maperiod',200),
        ('flatperiod',30),
        ('trail',0.20), 
        ('percentsizer',0.07),
        ('fluctuation',0.15),
        ('targetyearreturn',2.0)
    )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        #print(self.datas[0].datetime.date())
        self.inds = dict()
        self.order = dict()
        self.buyprice = dict()
        self.buydate = dict()
        #print(self.datas[0])
        for i,d in enumerate(self.datas):
            self.inds[d] = dict()
            # Keep a reference to the "close" line in the data[0] dataseries【多数据时这里用datas[0]有问题】
            #self.dataclose = self.datas[].close

            # To keep track of pending orders and buy price/commission
            self.order[d._name] = None
            self.buyprice[d._name] = None
            self.buydate[d._name] = None
            #self.buycomm = None

            # Add a MovingAverageSimple indicator
            #self.inds[d]['sma'] = bt.indicators.SimpleMovingAverage(
            #    self.datas[i], period=self.params.maperiod)
            # Add a indicator
            
            self.inds[d]['highest'] = bt.indicators.Highest(
                self.datas[i], period=self.params.flatperiod, plot=False)
            self.inds[d]['lowest'] = bt.indicators.Lowest(
                self.datas[i], period=self.params.flatperiod, plot=False)
            self.inds[d]['av'] = bt.indicators.Average(
                self.datas[i], period=self.params.flatperiod, plot=False)
            self.inds[d]['ifflat'] = (-(self.inds[d]['highest'] - self.inds[d]['lowest']) / 
                                      self.inds[d]['av'] + self.params.fluctuation + 1)
            
            '''
            # Indicators for the plotting show
            bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
            bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
                                                subplot=True)
            bt.indicators.StochasticSlow(self.datas[0])
            bt.indicators.MACDHisto(self.datas[0])
            rsi = bt.indicators.RSI(self.datas[0])
            bt.indicators.SmoothedMovingAverage(rsi, period=10)
            bt.indicators.ATR(self.datas[0], plot=False)
            '''
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    order.data._name + ', BUY EXECUTED, Price: %.2f, Cost: %.2f, Size: %.i, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.size,
                     order.executed.comm),order.data.datetime.date(0))

                self.buyprice[order.data._name] = order.executed.price
                self.buydate[order.data._name] = self.datetime.date(0)
                #self.buycomm = order.executed.comm
                #if not self.p.trail:
                #    stop_price = order.executed.price * (1.0 - self.p.stop_loss)
                #    self.sell(exectype=bt.Order.Stop, price=stop_price)
                #else:
                self.order[order.data._name] = self.sell(data=order.data._name, size=order.executed.size, exectype=bt.Order.StopTrail, trailpercent=self.p.trail)
            else:  # Sell
                self.log(order.data._name + ', SELL EXECUTED, Price: %.2f, Cost: %.2f, Size: %.i, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.size,
                          order.executed.comm),order.data.datetime.date(0))
                self.order[order.data._name] = None

            #self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            if order.isbuy():
                self.log(order.data._name + ', BUY Order Canceled/Margin/Rejected')
            elif order.issell():
                self.log(order.data._name + ', SELL Order Canceled/Margin/Rejected')
            else:
                self.log(order.data._name + ', OTHER Order Canceled/Margin/Rejected')
        # Write down: no pending order
        #print(self.order[order.data._name])
            self.order[order.data._name] = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        #self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
        #         (trade.pnl, trade.pnlcomm))
        self.log('{} Closed: PnL Gross {}, Net {}'.format(
                                                trade.data._name,
                                                round(trade.pnl,2),
                                                round(trade.pnlcomm,2)))
    def prenext(self):
        self.next()

    def next(self):
        for i,d in enumerate(self.datas):##【这个及多datafeed还有bug】
            if i>0:
                if len(d) >= max(self.p.flatperiod,0):
                    dt,dn = self.datas[i].datetime.date(), d._name
                    pos = self.getposition(d).size

                    if dt != self.datetime.date(0): #如果今天停牌则跳过
                        #print(dn + ' no trade at ' + self.datetime.date(0).isoformat() + ' stock date is ' + dt.isoformat())
                        continue
                    # Simply log the closing price of the series from the reference
                    # self.log(dn + ', Close, %.2f' % self.datas[i].close[0] + ' len(d) is ' + str(len(d)),dt)

                    # Check if an order is pending ... if yes, we cannot send a 2nd one
                    if self.order[d._name]:
                        if self.order[d._name].isbuy():
                            return
                        elif pos and self.order[d._name].issell():
                            dayspassed = dt - self.buydate[d._name]
                            dayspassed = dayspassed.days
                            if self.order[d._name].status in [self.order[d._name].Accepted]:
                                targetreturn = self.p.targetyearreturn ** (dayspassed // 250 + 1)
                                #if d.close[0] < self.inds[d]['sma'][0] or d.close[0] > self.buyprice[d._name] * targetreturn:
                                if d.close[0] > self.buyprice[d._name] * targetreturn:
                                    # SELL, SELL, SELL!!! (with all possible default parameters)
                                    self.log(dn + ', SELL CREATE, %.2f' % self.datas[i].close[0])

                                    # Keep track of the created order to avoid a 2nd order
                                    #print(self.order[d._name])
                                    self.broker.cancel(self.order[d._name]) 
                                    self.order[d._name] = None
                                    self.order[d._name] = self.sell(data=self.datas[i],size = pos)
                    else:
                        # Check if we are in the market
            #            if not self.position:
                        if not pos:
                            # self.log(dn + ' no pos')
                            # Not yet ... we MIGHT BUY if ...
                            if (d.close[0] > self.inds[d]['highest'][-1]) and int(self.inds[d]['ifflat'][0]):# and (d.close[0] > self.inds[d]['sma'][0]):

                                # BUY, BUY, BUY!!! (with all possible default parameters)
                                self.log(dn + ', BUY CREATE, %.2f' % d.close[0],dt)
                                try:
                                    #calculate size
                                    size = int(abs((self.broker.get_value() * self.params.percentsizer) / d.open[1]) // 100 * 100)
                                except Exception:
                                    #calculate size
                                    size = int(abs((self.broker.get_value() * self.params.percentsizer) / d.close[0]) // 100 * 100)
                                # Keep track of the created order to avoid a 2nd order
                                self.order[d._name] = self.buy(data = d, size = size)
                       
                        
        #self.log('Finished')

if __name__ == '__main__':
    starttime = datetime.datetime.now()
    # Create a cerebro entity
    cerebro = bt.Cerebro(stdstats=False)

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
#    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
#    datapath = os.path.join(modpath, '../../datas/orcl-1995-2014.txt')

    # acquire data
    start_date = '2014-06-08'
    end_date = '2020-12-24'
    indexpool = ['399300.SZ','000905.SH']
    stock = acquire_stock_list(indexpool,end_date)
    #stock = stock[:3]
    #stock = ['000001.SZ']
    #print(len(stock))
    
    ref_indexdata = acquire_index_data(indexpool[0],start_date,end_date)
    #print(ref_indexdata.columns)
    data = bt.feeds.PandasData(dataname=ref_indexdata,plot=1)
    cerebro.adddata(data, name = indexpool[0])
    
    for i in range(len(stock)):
        df = acquire_data(stock[i], start_date, end_date)
        if df.empty:
            continue
        #if i ==0:
        #    print(df.columns)
        # Create a Data Feed
        data = bt.feeds.PandasData(dataname=df,plot=0)
        # Add the Data Feed to Cerebro
        cerebro.adddata(data, name = stock[i])
        print('data' + str(i) + '/' + str(len(stock)) + 'feeded')
    # Set our desired cash start
    cerebro.broker.setcash(150000.0)

    # Add a FixedSize sizer according to the stake
    #cerebro.addsizer(bt.sizers.PercentSizer, percents=50)

    # Set the commission
    #cerebro.broker.setcommission(commission=0.00025)
    comminfo = stampDutyCommissionScheme(stamp_duty=0.005,commission=0.00025)
    cerebro.broker.addcommissioninfo(comminfo)
    
    # Set slipage
    cerebro.broker.set_slippage_fixed(0.03)
    '''
    #Analyzer
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.05,annualize=True, _name='sharp_ratio')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual_return')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
    '''
    # 定义图表输出哪些观察者observer
    cerebro.addobserver(bt.observers.Broker)
    cerebro.addobserver(bt.observers.BuySell)
    #cerebro.addobserver(bt.observers.Value)
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.Trades)

    # 定义用于策略绩效评价的分析者analyzers，可以随意增加
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.0, annualize=True, timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.VWR, _name='vwr')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='txn')

    # Add writer
    writername = start_date + ' TO ' + end_date + ' AT ' + str(datetime.datetime.now()) +'.csv'
    writername = writername.replace(':','-')
    cerebro.addwriter(bt.WriterFile,out=writername,rounding = 2)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    backtest = cerebro.run()
    backtest_results = backtest[0]
    
    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # 输出分析者结果
    printTradeAnalysis(cerebro, backtest_results.analyzers)

    '''
    #print analyzers
    for a in thestrats.analyzers:
        a.print()
    '''
    # Plot the result
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    start_date = start_date + datetime.timedelta(days=50)
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    cerebro.plot(start=start_date, end=end_date,style='candlestick', plotforce=True, volume=True)
    '''
    # 图表输出
    cerebro.plot(style='candlestick', volume=False)
    '''

    endtime = datetime.datetime.now()
    usedtime = endtime - starttime
    print('The code ran for ' + usedtime)