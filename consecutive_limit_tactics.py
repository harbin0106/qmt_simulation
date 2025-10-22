#encoding:gbk
import pandas as pd
import numpy as np
import talib
# Global variables
class G():
	pass
g = G()

def init(contextInfo):
	stock_list = ['603938.SH']
	contextInfo.trade_code_list=['603938.SH']
	contextInfo.set_universe(contextInfo.trade_code_list)
	contextInfo.buy = True
	contextInfo.sell = False
	contextInfo.stock_data = {}
	for stock in stock_list:
		contextInfo.stock_data[stock] = {
			'last_close': 0,
			'current_open': 0,
			'current_high': 0,
			'current_close': 0,
			'volume': 0
		}
	g.account = '100228644'
	
def handlebar(contextInfo):
	period = contextInfo.period
	if period != '1m':
		print(f'Error! period != "1m"! period={period}')
		return
	stock_list = contextInfo.get_universe()
	for stock in stock_list:
		market_data = contextInfo.get_market_data(['open', 'high', 'low', 'close', 'volume'],
				stock_code=[stock], period='1m', count=1)
		#print(f'market_data={market_data}')
		print(f'market_data["close"][0]={market_data["close"][0]}')
		if market_data['close'][0] >= 19.58:
			# Buy
			passorder(
				opType=23,                # 操作类型：23-股票买入，24-股票卖出
				orderType=1101,              # 下单方式：1101-按股数下单
				accountid='100228644',        # 交易账号
				orderCode=stock,             # 股票代码
				prType=12,                # 价格类型：11-指定价
				modelprice=target_price,      # 指定价格
				volume=100,            # 交易数量
				ContextInfo=contextInfo
			)
			
def account_callback(ContextInfo, accountInfo):
	print('accountInfo')
	# 输出资金账号状态
	print(accountInfo.m_strStatus)

# 委托主推函数
def order_callback(ContextInfo, orderInfo):
	print('orderInfo')
	# 输出委托证券代码
	print(orderInfo.m_strInstrumentID)

# 成交主推函数
def deal_callback(ContextInfo, dealInfo):
	print('dealInfo')
	# 输出成交证券代码
	print(dealInfo.m_strInstrumentID)

# 持仓主推函数
def position_callback(ContextInfo, positonInfo):
	print('positonInfo')
	# 输出持仓证券代码
	print(positonInfo.m_strInstrumentID)

#下单出错回调函数
def orderError_callback(ContextInfo, passOrderInfo, msg):
	print('orderError_callback')
	#输出下单信息以及错误信息
	print (passOrderInfo.orderCode)
	print (msg)

