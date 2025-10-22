#encoding:gbk
import pandas as pd
import numpy as np
import talib
# Global trade variables
class T():
	pass
T = T()

def init(contextInfo):
	T.stock_list = ['603938.SH']
	T.account_type = 'STOCK'
	T.account = '100228644'
	T.opType_buy = 23 	# 操作类型：23-股票买入，24-股票卖出
	T.opType_sell = 24	# 操作类型：23-股票买入，24-股票卖出
	T.orderType_value = 1102	# 单股、单账号、普通、金额(元)方式下单(只支持股票) 
	T.prType_market = 12
	T.amount = 10000
	T.volume_value = 10000
	contextInfo.set_universe(T.stock_list)
	contextInfo.set_slippage(1, 0.003)
	contextInfo.set_commission(0.0001)
	contextInfo.capital = 1000000
	contextInfo.set_account(T.account)
	contextInfo.max_single_order = 10000
	contextInfo.max_position = 0.99

def handlebar(contextInfo):
#	obj_list = get_trade_detail_data(T.account,'stock','ACCOUNT')
#	for obj in obj_list:
#		print(dir(obj))#查看有哪些属性字段
#	return
	account = get_trade_detail_data(T.account, T.account_type, 'ACCOUNT')
	if len(account) == 0:
		print(f'Error! account {T.account} is not logged in!')
		return
#	for obj in account:
#		print(dir(obj))
	#print(f'account[0].m_dAvailable={account[0].m_dAvailable}')
	available_cash = int(account[0].m_dAvailable)
	#print(f'available_cash={available_cash}')
	# 实盘下单
	#passorder(opType=T.opType_buy, orderType=T.orderType_value, accountid=T.account, 
	#		orderCode=T.stock_list[0], prType=T.prType_market, modelprice=-1, volume=T.volume_value, 
	#		strategyName='buy_test', quickTrade=1, ContextInfo=contextInfo)
	# 模拟盘下单
	order_shares(T.stock_list[0], 100, contextInfo)
	order_shares(T.stock_list[0], -100, contextInfo)
	return
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
		if market_data['close'][0] >= 19.58 or True:
			# Buy
			order_result = passorder(
				opType=23,                # 操作类型：23-股票买入，24-股票卖出
				orderType=1101,              # 下单方式：1101-按股数下单
				accountid='100228644',        # 交易账号
				orderCode=stock,             # 股票代码
				prType=12,                # 价格类型：11-指定价
				modelprice=19.58,      # 指定价格
				volume=100,            # 交易数量
				ContextInfo=contextInfo
			)
			print(f'order_result={order_result}')
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

