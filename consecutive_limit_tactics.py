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
	contextInfo.set_account(T.accountid)
	return
	contextInfo.set_slippage(1, 0.003)
	contextInfo.set_commission(0.0001)
	contextInfo.capital = 1000000
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
	bar_time= timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	print(f'bar_time={bar_time}')
	# Validate period
	if contextInfo.period != '1m':
		print(f'Error! contextInfo.period != "1m"! contextInfo.period={contextInfo.period}')
		return
	# Skip history bars
	if not contextInfo.is_last_bar() and False:
		# print(f'contextInfo.is_last_bar()={contextInfo.is_last_bar()}')
		return
	# 确认当前k线的时刻是09:30:00
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time == '09:30:00':
		trade_on_market_open(contextInfo)
		return
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		print(f'账号{A.acct} 未登录 请检查')
		return
	# 检查是否出现了卖出信号
	trade_on_sell_signal_check(contextInfo)
	
	account = account[0]
	available_cash = int(account.m_dAvailable)
	print(f'available_cash={available_cash}')
	market_data = contextInfo.get_market_data_ex(['close'], T.orderCodes, period='1m', start_time=bar_time, end_time=bar_time, count=-1)
	# print(f'market_data={market_data[T.orderCodes[0]]}')
	stock_list = contextInfo.get_universe()
	for stock in stock_list:
		close_price = market_data[stock].values[0][0]
		print(f'{stock} 现价: {close_price}')

#	get_924_open_price(contextInfo, T.orderCodes[0], '2025-10-22')
#	obj_list = get_trade_detail_data(T.accountid,'stock','ACCOUNT')
#	for obj in obj_list:
#		print(dir(obj))#查看有哪些属性字段
#	return
#	for obj in account:
#		print(dir(obj))

def trade_on_sell_signal_check(contextInfo):
	print(f'trade_on_sell_signal_check()')
	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	print(f'current_time={current_time}')

	stock_list = contextInfo.get_universe()
	for stock in stock_list:
		# 获取当前股价
		market_data = contextInfo.get_market_data_ex(['close'], [stock], period='1m', start_time=bar_time, end_time=bar_time, count=1)
		if market_data[stock].empty:
			continue
		current_price = market_data[stock]['close'].iloc[0]
		print(f'{stock} 当前股价: {current_price}')

		# 获取昨日收盘价
		df_yesterday = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=2)
		if df_yesterday[stock].empty:
			continue
		yesterday_close = df_yesterday[stock]['close'].iloc[0]  # iloc[0]是昨天
		print(f'{stock} 昨日收盘价: {yesterday_close}')

		# 计算跌停价 (A股跌停价为昨日收盘价的90%)
		limit_down_price = round(yesterday_close * 0.9, 2)
		print(f'{stock} 跌停价: {limit_down_price}')

		# 条件1: 在14:55时刻，股价相对于昨日收盘价下跌超过3%
		if current_time == '14:55:00':
			pct = (current_price - yesterday_close) / yesterday_close * 100
			print(f'{stock} 下跌百分比: {pct:.2f}%')
			if pct < -3:
				print(f'{stock} 满足条件1: 14:55下跌超过3%，准备卖出')
				trade_sell_stock(contextInfo, stock)

		# 条件2: 触及跌停价
		if current_price <= limit_down_price:
			print(f'{stock} 触及跌停价，准备卖出')
			trade_sell_stock(contextInfo, stock)

def trade_query_info(C):
	orders = get_trade_detail_data(T.accountid, 'stock', 'order')
	for o in orders:
		print(f'股票代码: {o.m_strInstrumentID}, 市场类型: {o.m_strExchangeID}, 证券名称: {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
		f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')

	deals = get_trade_detail_data('8000000213', 'stock', 'deal')
	for dt in deals:
		print(f'股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}', 
		f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')

	positions = get_trade_detail_data('8000000213', 'stock', 'position')
	for dt in positions:
		print(f'股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	accounts = get_trade_detail_data('8000000213', 'stock', 'account')
	for dt in accounts:
		print(f'总资产: {dt.m_dBalance:.2f}, 净资产: {dt.m_dAssureAsset:.2f}, 总市值: {dt.m_dInstrumentValue:.2f}', 
		f'总负债: {dt.m_dTotalDebit:.2f}, 可用金额: {dt.m_dAvailable:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	return orders, deals, positions, accounts
	
def trade_sell_stock(contextInfo, stock):
#	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
#	for dt in positions:
#		print(f'股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
#		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	# 获取持仓量
	try:
		position = contextInfo.get_instrument_detail('600491.SH')
		print(f'position={position}')
		volume = position['LastVolume']  # 可卖数量
		if volume != None and volume > 0:
			passorder(T.opType_sell, T.orderType, T.accountid, stock, T.prType, T.price, volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
			print(f'{stock} 卖出 {volume} 股')
		else:
			print(f'{stock} 无可卖持仓')
	except Exception as e:
		print(f'获取持仓失败: {e}')

def trade_on_market_open(contextInfo):
	print(f'trade_on_market_open()')
	# 确认当前k线的时刻是09:30:00
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time != '09:30:00':
		#print(f'当前时间不是09:30:00，当前时间: {current_time}')
		return

	start_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# print(f'start_time={start_time}, contextInfo.barpos={contextInfo.barpos}')
	for stock in T.orderCodes:
		# 获取开盘价 (1分钟K线)
		# 获取开盘价 (1分钟K线，count=-1，取09:30:00的开盘价)
		df_open = contextInfo.get_market_data_ex(['open'], [stock], period='1m', count=1, start_time=start_time, end_time=start_time)
		# print(f'df_open={df_open}')
		open_price = None
		for i, stime in enumerate(df_open[stock].index):
			dt = pd.to_datetime(str(stime), format='%Y%m%d%H%M%S')
			if dt.time() == datetime.time(9, 30, 0):
				open_price = df_open[stock]['open'].iloc[i]
				break
		if open_price is None:
			print(f'{stock} 未找到09:30:00的开盘价数据，跳过')
			continue
		print(f'{stock} 开盘价: {open_price}')

		# 获取昨日收盘价 (日线数据，count=2，取第二个)
		df_yesterday = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=2)
		# print(f'df_yesterday={df_yesterday}')
		yesterday_close = df_yesterday[stock]['close'].iloc[0]  # iloc[0]是昨天，iloc[1]是今天
		print(f'{stock} 昨日收盘价: {yesterday_close}')

		# 计算涨幅
		if yesterday_close == 0:
			print(f'{stock} 昨日收盘价为0，跳过')
			continue
		pct = round((open_price - yesterday_close) / yesterday_close * 100, 2)
		print(f'{stock} 涨幅: {pct}%')

		# 计算5日均价 (日线数据)
		df_ma = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=5)
		ma5 = round(df_ma[stock]['close'].mean(), 2)
		print(f'{stock} 5日均价: {ma5}')

		# 策略逻辑
		if 3 <= pct <= 8:
			# 以开盘价下单买入500股
			passorder(T.opType_buy, T.orderType, T.accountid, stock, 11, open_price, 500, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
			print(f'{stock} 以开盘价 {open_price} 买入500股')
		elif (1 <= pct < 3) or (8 < pct <= 9):
			# 以开盘价下单买入200股
			passorder(T.opType_buy, T.orderType, T.accountid, stock, 11, open_price, 200, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
			print(f'{stock} 以开盘价 {open_price} 买入200股')
		elif pct < 1:
			# 以5日均线价格挂单买入 (假设买入100股，可根据需要调整)
			passorder(T.opType_buy, T.orderType, T.accountid, stock, 13, ma5, 100, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
			print(f'{stock} 以5日均价 {ma5} 挂单买入100股')
		else:
			print(f'{stock} 不满足买入条件')

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
	# 输出资金账号状态
	print(f'account_callback(): accountInfo={accountInfo}, accountInfo.m_strStatus={accountInfo.m_strStatus}')

# 委托主推函数
def order_callback(contextInfo, orderInfo):
	# 输出委托证券代码
	print(f'order_callback(): orderInfo={orderInfo}, orderInfo.m_strInstrumentID={orderInfo.m_strInstrumentID}')

# 成交主推函数
def deal_callback(contextInfo, dealInfo):
	print(f'deal_callback(): dealInfo={dealInfo}, dealInfo.m_strInstrumentID={dealInfo.m_strInstrumentID}')

# 持仓主推函数
def position_callback(contextInfo, positonInfo):
	# 输出持仓证券代码
	print(f'position_callback(): positonInfo={positionInfo}, positonInfo.m_strInstrumentID={positonInfo.m_strInstrumentID}')
	
#下单出错回调函数
def orderError_callback(contextInfo, passOrderInfo, msg):
	print(f'orderError_callback(): passOrderInfo.orderCode={passOrderInfo.orderCode}, msg={msg}')