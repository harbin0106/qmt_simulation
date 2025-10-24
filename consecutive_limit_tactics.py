#encoding:gbk
import pandas as pd
import numpy as np
import talib
import datetime

class T():
	pass
T = T()

def get_stock_name(contextInfo, stock):
    try:
        instrument = contextInfo.get_instrument_detail(stock)
        return instrument.get('InstrumentName')
    except:
        return "未知"

def init(contextInfo):
	T.orderCodes = ['603938.SH', '301468.SZ']
	# T.orderCodes = ['603938.SH']
	T.accountid_type = 'STOCK'
	T.accountid = '100200109'	#'100200109'。account变量是模型交易界面 添加策略时选择的资金账号，不需要手动填写
	T.opType_buy = 23 	# 操作类型：23-股票买入，24-股票卖出
	T.opType_sell = 24	# 操作类型：23-股票买入，24-股票卖出
	T.orderType = 1101	# 单股、单账号、普通、股/手方式下单 
	T.prType = 5		# 0：卖5价 1：卖4价 2：卖3价 3：卖2价 4：卖1价 5：最新价 
						# 6：买1价 7：买2价（组合不支持） 8：买3价（组合不支持） 9：买4价（组合不支持）
						# 10：买5价（组合不支持） 11：（指定价）模型价（只对单股情况支持,对组合交易不支持）
						# 12：涨跌停价 13：挂单价 14：对手价
	T.volume = 100
	T.strategyName = 'consecutive_limit_tactics'
	T.quickTrade = 2 	# 0-非立即下单。1-实盘下单（历史K线不起作用）。
						# 2-仿真下单，不会等待k线走完再委托。可以在after_init函数、run_time函数注册的回调函数里进行委托 
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
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		print(f'after_init(): Error! 账号{T.accountid} 未登录! 请检查!')
		return
	# 按照最新价买入
	# passorder(T.opType_buy, T.orderType, T.accountid, T.orderCodes[0], T.prType, T.price, T.volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	# 按照最新价卖出
	# passorder(T.opType_sell, T.orderType, T.accountid, T.orderCodes[1], T.prType, T.price, T.volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	# order_shares(T.orderCodes[0], 200, contextInfo)
	# trade_query_info(contextInfo)
	trade_sell_stock(contextInfo, T.orderCodes[0])

def handlebar(contextInfo):
	bar_time= timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# print(f"\nhandlebar(): bar_time={timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y-%m-%d %H:%M:%S')}")
	# Validate period
	if contextInfo.period != '1m':
		print(f'handlebar(): Error! contextInfo.period != "1m"! contextInfo.period={contextInfo.period}')
		return
	# Skip history bars ####################################
	if not contextInfo.is_last_bar() and False:
		# print(f'handlebar(): contextInfo.is_last_bar()={contextInfo.is_last_bar()}')
		return

	# 开盘交易逻辑
	trade_on_market_open(contextInfo)
	# 检查是否出现了卖出信号
	trade_on_sell_signal_check(contextInfo)

def trade_on_sell_signal_check(contextInfo):
	# print(f'trade_on_sell_signal_check()')
	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	stock_list = contextInfo.get_universe()
	for stock in stock_list:
		# 获取当前股价
		market_data = contextInfo.get_market_data_ex(['close'], [stock], period='1m', start_time=bar_time, end_time=bar_time, count=1)
		if market_data[stock].empty:
			print(f'trade_on_sell_signal_check(): Error! 未获取到{stock} {get_stock_name(contextInfo, stock)} 的当前股价数据，跳过!')
			continue
		current_price = market_data[stock]['close'].iloc[0]
		# print(f'trade_on_sell_signal_check(): {stock} 当前股价：{current_price:.2f}')

		# 获取昨日收盘价
		market_data_yesterday = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=2)
		if market_data_yesterday[stock].empty:
			print(f'trade_on_sell_signal_check(): Error! 未获取到{stock} {get_stock_name(contextInfo, stock)} 的昨日收盘价数据，跳过!')
			continue
		yesterday_close = market_data_yesterday[stock]['close'].iloc[0]  # iloc[0]是昨天
		# print(f'trade_on_sell_signal_check(): {stock} 昨日收盘价: {yesterday_close:.2f}')

		# 计算跌停价 (A股跌停价为昨日收盘价的90%)
		limit_down_price = round(yesterday_close * 0.9, 2)
		# print(f'trade_on_sell_signal_check(): {stock} 跌停价: {limit_down_price:.2f}')

		# 条件1: 在14:55时刻，股价相对于昨日收盘价下跌超过3%
		current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
		if current_time == '14:55:00':
			pct = (current_price - yesterday_close) / yesterday_close * 100
			# print(f'trade_on_sell_signal_check(): {stock} 涨幅: {pct:.2f}%')
			if pct < -3:
				print(f'trade_on_sell_signal_check(): {stock} {get_stock_name(contextInfo, stock)} 满足条件1: 14:55下跌超过3%，准备卖出')
				trade_sell_stock(contextInfo, stock)

		# 条件2: 触及跌停价
		if current_price <= limit_down_price:
			print(f'trade_on_sell_signal_check(): {stock} {get_stock_name(contextInfo, stock)} 触及跌停价，准备卖出')
			trade_sell_stock(contextInfo, stock)

def trade_query_info(contextInfo):
	current_date = datetime.datetime.now().date()
	N_days_ago = current_date - datetime.timedelta(days=7)

	orders = get_trade_detail_data(T.accountid, 'stock', 'order')
	print("trade_query_info(): 最近7天的委托记录:")
	for o in orders:
		full_code = f"{o.m_strInstrumentID}.{o.m_strExchangeID}"
		if full_code not in T.orderCodes:
			continue
		try:
			order_date = datetime.datetime.strptime(o.m_strInsertTime, '%Y%m%d%H%M%S').date()
			if order_date >= N_days_ago:
				print(f'trade_query_info(): 股票代码: {o.m_strInstrumentID}, 市场类型: {o.m_strExchangeID}, 证券名称: {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
				f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')
		except (AttributeError, ValueError):
			# 如果没有时间字段或格式不匹配，打印所有
			print(f'trade_query_info(): 股票代码: {o.m_strInstrumentID}, 市场类型: {o.m_strExchangeID}, 证券名称: {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
			f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')

	deals = get_trade_detail_data(T.accountid, 'stock', 'deal')
	print("trade_query_info(): 最近7天的成交记录:")
	for dt in deals:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code not in T.orderCodes:
			continue
		try:
			deal_date = datetime.datetime.strptime(dt.m_strTime, '%Y%m%d%H%M%S').date()
			if deal_date >= N_days_ago:
				print(f'trade_query_info(): 股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
				f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')
		except (AttributeError, ValueError):
			# 如果没有时间字段或格式不匹配，打印所有
			print(f'trade_query_info(): 股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
			f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')

	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	print("trade_query_info(): 当前持仓状态:")
	for dt in positions:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code not in T.orderCodes:
			continue
		print(f'trade_query_info(): 股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	accounts = get_trade_detail_data(T.accountid, 'stock', 'account')
	print("trade_query_info(): 当前账户状态:")
	for dt in accounts:
		print(f'trade_query_info(): 总资产: {dt.m_dBalance:.2f}, 净资产: {dt.m_dAssureAsset:.2f}, 总市值: {dt.m_dInstrumentValue:.2f}',
		f'总负债: {dt.m_dTotalDebit:.2f}, 可用金额: {dt.m_dAvailable:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	return orders, deals, positions, accounts
	
def trade_sell_stock(contextInfo, stock):
	print(f'trade_sell_stock(): stock={stock} {get_stock_name(contextInfo, stock)}')
	volume = 0
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	for dt in positions:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code != stock:
			continue
		print(f'trade_sell_stock(): 股票代码: {dt.m_strInstrumentID}, 市场类型: {dt.m_strExchangeID}, 证券名称: {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')
		volume = 100 # dt.m_nCanUseVolume  # 可卖数量
		break
	if volume == 0:
		print(f'trade_sell_stock(): Error! volume == 0! 没有可卖的持仓，跳过卖出操作')
		return
	passorder(T.opType_sell, T.orderType, T.accountid, stock, T.prType, T.price, volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	print(f'trade_sell_stock(): {stock} {get_stock_name(contextInfo, stock)} 卖出 {volume} 股')

def trade_buy_stock(contextInfo, stock, buy_volume):
	print(f'trade_buy_stock(): stock={stock} {get_stock_name(contextInfo, stock)}, buy_volume={buy_volume}')

	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		print(f'trade_buy_stock(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	print(f'trade_buy_stock(): 当前可用资金: {available_cash:.2f}')

	# 获取当前股价
	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	market_data = contextInfo.get_market_data_ex(['close'], [stock], period='1m', start_time=bar_time, end_time=bar_time, count=1)
	if market_data[stock].empty:
		print(f'trade_buy_stock(): Error! 未获取到{stock} {get_stock_name(contextInfo, stock)} 的当前股价数据，跳过!')
		return
	current_price = market_data[stock]['close'].iloc[0]
	print(f'trade_buy_stock(): {stock} {get_stock_name(contextInfo, stock)} 当前股价: {current_price:.2f}')

	# 计算资金是否够用
	cost = current_price * buy_volume
	if cost <= available_cash:
		passorder(T.opType_buy, T.orderType, T.accountid, stock, T.prType, T.price, buy_volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
		print(f'trade_buy_stock(): {stock} {get_stock_name(contextInfo, stock)} 买入 {buy_volume} 股，成本{cost:.2f}')
	else:
		print(f'trade_buy_stock(): Error! {stock} {get_stock_name(contextInfo, stock)} 资金不足，跳过买入{buy_volume}股，所需{cost:.2f}，可用{available_cash:.2f}')
	
def trade_on_market_open(contextInfo):
	# 确认当前k线的时刻是09:30:00
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time != '09:30:00':
		#print(f'trade_on_market_open(): 当前时间不是09:30:00，当前时间: {current_time}')
		return
	# print(f'trade_on_market_open()')

	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# print(f'trade_on_market_open(): start_time={start_time}, contextInfo.barpos={contextInfo.barpos}')
	for stock in T.orderCodes:
		# 获取开盘价 (1分钟K线，count=-1，取09:30:00的开盘价)
		market_data = contextInfo.get_market_data_ex(['open'], [stock], period='1m', count=1, start_time=bar_time, end_time=bar_time)
		# print(f'trade_on_market_open(): market_data={market_data}')
		open_price = None
		for i, stime in enumerate(market_data[stock].index):
			dt = pd.to_datetime(str(stime), format='%Y%m%d%H%M%S')
			if dt.time() == datetime.time(9, 30, 0):
				open_price = market_data[stock]['open'].iloc[i]
				break
		if open_price is None:
			print(f'trade_on_market_open(): Error! {stock} {get_stock_name(contextInfo, stock)} 未找到09:30:00的开盘价数据，跳过!')
			continue
		print(f'\ntrade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 开盘价: {open_price:.2f}')

		# 获取昨日收盘价 (日线数据，count=2，取第二个)
		market_data_yesterday = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=2)
		# print(f'market_data_yesterday={market_data_yesterday}')
		yesterday_close = market_data_yesterday[stock]['close'].iloc[0]  # iloc[0]是昨天，iloc[1]是今天
		print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 昨日收盘价: {yesterday_close:.2f}')

		# 计算涨幅
		if yesterday_close == 0:
			print(f'trade_on_market_open(): Error! {stock} {get_stock_name(contextInfo, stock)} 昨日收盘价为0，跳过!')
			continue
		pct = round((open_price - yesterday_close) / yesterday_close * 100, 2)
		print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 涨幅: {pct}%')

		# 计算5日均价 (日线数据)
		market_data_ma = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=5)
		ma5 = round(market_data_ma[stock]['close'].mean(), 2)
		print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 5日均价: {ma5}')

		# 策略逻辑
		if 3 <= pct <= 8:
			# 以开盘价下单买入500股
			volume = 500
			trade_buy_stock(contextInfo, stock, volume)
		elif (1 <= pct < 3) or (8 < pct <= 9):
			# 以开盘价下单买入200股
			volume = 200
			trade_buy_stock(contextInfo, stock, volume)
		elif pct < 1:
			# 以5日均线价格挂单买入 (假设买入100股，可根据需要调整)
			volume = 100
			trade_buy_stock(contextInfo, stock, volume)
		else:
			print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 不满足买入条件')

def account_callback(contextInfo, accountInfo):
	# 输出资金账号状态
	if accountInfo.m_strStatus != '登录成功':
		print(f'account_callback(): Error! 账号状态异常! m_strStatus={accountInfo.m_strStatus}')

# 委托主推函数
def order_callback(contextInfo, orderInfo):
	# 输出委托证券代码
	stock = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, stock)
	full_code = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	if full_code not in T.orderCodes:
		return
	# print(f'order_callback(): {stock} {name}, m_nOrderStatus={orderInfo.m_nOrderStatus}, m_dLimitPrice={orderInfo.m_dLimitPrice}, m_nOpType={orderInfo.m_nOpType}, m_nVolumeTotalOriginal={orderInfo.m_nVolumeTotalOriginal}, m_nVolumeTraded={orderInfo.m_nVolumeTraded}')
	# 检查委托状态并记录成交结果
	if orderInfo.m_nOrderStatus == 56:  # 已成
		print(f'order_callback(): 委托已全部成交 - 股票: {stock} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 成交数量: {orderInfo.m_nVolumeTraded}, 成交均价: {orderInfo.m_dTradedPrice:.2f}')
	elif orderInfo.m_nOrderStatus == 55:  # 部成
		print(f'order_callback(): 委托部分成交 - 股票: {stock} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 已成交数量: {orderInfo.m_nVolumeTraded}, 剩余数量: {orderInfo.m_nVolumeTotal}')
	elif orderInfo.m_nOrderStatus == 54:  # 已撤
		print(f'order_callback(): 委托已撤销 - 股票: {stock} {name}, 委托ID: {orderInfo.m_strOrderSysID}')
	else:
		return
		# print(f'order_callback(): 委托状态更新 - 股票: {stock} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 状态: {orderInfo.m_nOrderStatus}')
	# # 打印所有成员变量的内容
	# print('order_callback(): All attributes of orderInfo:')
	# for attr in dir(orderInfo):
	# 	if not attr.startswith('_'):
	# 		try:
	# 			value = getattr(orderInfo, attr)
	# 			print(f'  {attr}: {value}')
	# 		except:
	# 			print(f'  {attr}: <无法获取>')

# 成交主推函数
def deal_callback(contextInfo, dealInfo):
	stock = f"{dealInfo.m_strInstrumentID}.{dealInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, stock)
	# print(f'deal_callback(): {stock} {name}, m_dPrice={dealInfo.m_dPrice}, m_dPrice={dealInfo.m_dPrice}, m_nVolume={dealInfo.m_nVolume}')
	# 检查成交结果并记录
	# print(f'deal_callback(): 成交确认 - 股票: {stock} {name}, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}, 买卖方向: {dealInfo.m_nDirection}')
	# 可以在这里添加更多逻辑，如更新全局变量、发送通知等
	# 例如，检查是否为买入或卖出，并更新持仓统计
	if dealInfo.m_nDirection == 48:  # 买入
		print(f'deal_callback(): {stock} {name}, 买入成交 - 更新持仓信息, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}')
	elif dealInfo.m_nDirection == 49:  # 卖出
		print(f'deal_callback(): {stock} {name}, 卖出成交 - 更新持仓信息, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}')
	# 打印所有成员变量的内容
	# print('deal_callback(): All attributes of dealInfo:')
	# for attr in dir(dealInfo):
	# 	if not attr.startswith('_'):
	# 		try:
	# 			value = getattr(dealInfo, attr)
	# 			print(f'  {attr}: {value}')
	# 		except:
	# 			print(f'  {attr}: <无法获取>')

# 持仓主推函数
def position_callback(contextInfo, positionInfo):
	# 输出持仓证券代码
	stock = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, stock)
	full_code = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	if full_code not in T.orderCodes:
		return
	# print(f'position_callback(): {stock} {name}, m_nVolume={positionInfo.m_nVolume}, m_nFrozenVolume={positionInfo.m_nFrozenVolume}')
	# 检查持仓变化并记录
	print(f'position_callback(): 持仓更新 - 股票: {stock} {name}, 总持仓量: {positionInfo.m_nVolume}, 可用数量: {positionInfo.m_nCanUseVolume}, 冻结数量: {positionInfo.m_nFrozenVolume}, 成本价: {positionInfo.m_dOpenPrice:.2f}, 持仓盈亏: {positionInfo.m_dPositionProfit:.2f}')
	# 可以在这里添加逻辑，如检查持仓是否为0，触发卖出信号等
	# if positionInfo.m_nVolume == 0:
	# 	print(f'position_callback(): 持仓清空 - 股票: {stock} {name}')
	# 打印所有成员变量的内容
	# print('position_callback(): All attributes of positionInfo:')
	# for attr in dir(positionInfo):
	# 	if not attr.startswith('_'):
	# 		try:
	# 			value = getattr(positionInfo, attr)
	# 			print(f'  {attr}: {value}')
	# 		except:
	# 			print(f'  {attr}: <无法获取>')
	
#下单出错回调函数
def orderError_callback(contextInfo, passOrderInfo, msg):
	stock = f"{passOrderInfo.orderCode}"
	name = get_stock_name(contextInfo, stock)
	print(f'\norderError_callback(): 下单错误 - 股票: {stock} {name}, 错误信息: {msg}')
	# 可以在这里添加逻辑，如重试下单或发送警报
	# 打印所有成员变量的内容
	# print(f'orderError_callback(): {stock} {name}:')
	# for attr in dir(passOrderInfo):
	# 	if not attr.startswith('_'):
	# 		try:
	# 			value = getattr(passOrderInfo, attr)
	# 			print(f'  {attr}: {value}')
	# 		except:
	# 			print(f'  {attr}: <无法获取>')
