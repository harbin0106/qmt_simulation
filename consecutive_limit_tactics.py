#encoding:gbk
import pandas as pd
import numpy as np
import talib
import datetime

class T():
	pass
T = T()

def init(contextInfo):
	T.orderCodes = ['603938.SH', '301468.SZ']
	# T.orderCodes = ['603938.SH']
	T.accountid_type = 'STOCK'
	T.accountid = '100200109'	#'100200109'。account变量是模型交易界面 添加策略时选择的资金账号，不需要手动填写
	T.opType_buy = 23 	# 操作类型：23-股票买入，24-股票卖出
	T.opType_sell = 24	# 操作类型：23-股票买入，24-股票卖出
	T.orderType = 1101	# 单股、单账号、普通、股/手方式下单 
	T.prType = 5		# 0：卖5价 1：卖4价 2：卖3价 3：卖2价 4：卖1价 5：最新价 6：买1价 7：买2价（组合不支持） 8：买3价（组合不支持） 9：买4价（组合不支持）
						# 10：买5价（组合不支持） 11：（指定价）模型价（只对单股情况支持,对组合交易不支持） 12：涨跌停价 13：挂单价 14：对手价
	T.volume = 100
	T.strategyName = 'consecutive_limit_tactics'
	T.quickTrade = 2 # 0-非立即下单。1-实盘下单（历史K线不起作用）。2-仿真下单，不会等待k线走完再委托。可以在after_init函数、run_time函数注册的回调函数里进行委托 
	T.userOrderId = '投资备注'
	T.price=-1
	contextInfo.set_universe(T.orderCodes)
	return
	contextInfo.set_slippage(1, 0.003)
	contextInfo.set_commission(0.0001)
	contextInfo.capital = 1000000
	#contextInfo.set_account(T.accountid)
	contextInfo.max_single_order = 10000
	contextInfo.max_position = 0.99

def after_init(contextInfo):
	print(f'after_init()')
	# 按照最新价买入
	# passorder(T.opType_buy, T.orderType, T.accountid, T.orderCodes[0], T.prType, T.price, T.volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	# 按照最新价卖出
	passorder(T.opType_sell, T.orderType, T.accountid, T.orderCodes[1], T.prType, T.price, T.volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	# order_shares(T.orderCodes[0], 200, contextInfo)
	
def handlebar(contextInfo):
	trade_on_market_open(contextInfo)
	return
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account)==0:
		print(f'账号{A.acct} 未登录 请检查')
		return
	account = account[0]
	available_cash = int(account.m_dAvailable)
	print(f'available_cash={available_cash}')
	df = contextInfo.get_market_data_ex(['open', 'high', 'low', 'close'], T.orderCodes, period='1m', count=1)
	print(f'df={df[T.orderCodes[0]]}')
	open_price = df[T.orderCodes[0]].values[0][0]
	print(f'open_price={open_price}')
	if not contextInfo.is_last_bar() and False:
		# print(f'contextInfo.is_last_bar()={contextInfo.is_last_bar()}')
		return
	print(f'contextInfo.is_last_bar()={contextInfo.is_last_bar()}')
	period = contextInfo.period
	if period != '1m':
		print(f'Error! period != "1m"! period={period}')
		return
	current_date = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d')
	bar_time= timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	print(f'bar_time={bar_time}')
	if bar_time == '09:30:00':
		print(f'bar_time == 09:30:00')
		trade_on_market_open(T.orderCodes[0], contextInfo)
		return
	stock_list = contextInfo.get_universe()
	for stock in stock_list:
		# df = contextInfo.get_market_data_ex(['close'], T.orderCodes, end_time='', period='tick', count=60, subscribe=False)
		df = contextInfo.get_market_data_ex(['time', 'open', 'high', 'low', 'close'],
				stock_code=[stock], period='tick', start_time='', end_time='', count=-1, dividend_type='follow', fill_data=True, subscribe=True)
		#closes = list(df[T.orderCodes[0]].iloc[:, 0])
		# print(f'closes=\n{closes}')
			# Buy
#	get_924_open_price(contextInfo, T.orderCodes[0], '2025-10-22')
#	obj_list = get_trade_detail_data(T.accountid,'stock','ACCOUNT')
#	for obj in obj_list:
#		print(dir(obj))#查看有哪些属性字段
#	return
#	for obj in account:
#		print(dir(obj))

def trade_on_market_open(contextInfo):
	print(f'trade_on_market_open()')
	# 获取开盘价
	market_data = contextInfo.get_market_data_ex(['time', 'open', 'high', 'low', 'close'], T.orderCodes, period='1m', count=1)
	print(f'df=\n{market_data[T.orderCodes[0]].values[0][0]}')
	datetime = pd.to_datetime(market_data[T.orderCodes[0]].values[0][0], unit='s')
	print(f'datetime={datetime}')
	pass
	
def get_924_open_price(contextInfo, stock_code, target_date):
	"""
	获取指定股票在9:24分的开盘价
	:param stock_code: 股票代码，如'600000.SH'
	:param target_date: 目标日期，格式'YYYY-MM-DD'
	:return: 9:24分开盘价，如不存在返回None
	"""
	try:
		# 获取当日所有分钟线数据
		df = contextInfo.get_market_data_ex(['open', 'high', 'low', 'close'],
			stock_code=stock_code, period='tick', start_time='', 
			end_time='', count=-1, dividend_type='follow', 
			fill_data=True, subscribe=True)

		# 提取9:24分数据（实际为9:24-9:25的K线）
		time_index = None
		for i, timestamp in enumerate(df[stock_code].index):
			if '09:24:00' <= str(timestamp.time()) <= '09:25:00':
				time_index = i
				break
		
		if time_index is not None:
			open_price = df[stock_code]['open'][time_index]
			print(f"{stock_code} {target_date} 9:24开盘价: {open_price}")
			return open_price
		else:
			print(f"未找到{stock_code} {target_date} 9:24分数据")
			return None
			
	except Exception as e:
		print(f"获取数据失败: {str(e)}")
		return None

def account_callback(contextInfo, accountInfo):
	print('accountInfo')
	# 输出资金账号状态
	print(accountInfo.m_strStatus)
	# order_shares(T.orderCodes[0], 100, contextInfo)


# 委托主推函数
def order_callback(contextInfo, orderInfo):
	print('orderInfo')
	# 输出委托证券代码
	print(orderInfo.m_strInstrumentID)

# 成交主推函数
def deal_callback(contextInfo, dealInfo):
	print('dealInfo')
	# 输出成交证券代码
	print(dealInfo.m_strInstrumentID)

# 持仓主推函数
def position_callback(contextInfo, positonInfo):
	print('positonInfo')
	# 输出持仓证券代码
	print(positonInfo.m_strInstrumentID)

#下单出错回调函数
def orderError_callback(contextInfo, passOrderInfo, msg):
	print('orderError_callback')
	#输出下单信息以及错误信息
	print (passOrderInfo.orderCode)
	print (msg)

