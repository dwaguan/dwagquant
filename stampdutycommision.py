import backtrader as bt

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
        print('self.p.commission',self.p.commission)
        if size > 0: # 买入，不考虑印花税
            return  size * price * self.p.commission * 100
        elif size < 0: # 卖出，考虑印花税
            return - size * price * (self.p.stamp_duty + self.p.commission*100)
        else:
            return 0 #just in case for some reason the size is 0.