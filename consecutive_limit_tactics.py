#encoding:gbk
import pandas as pd
import numpy as np
import talib
import datetime
import sqlite3
import time
import json

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
	T.codes_all = ['603938.SH', '301468.SZ']
	T.accountid_type = 'STOCK'
	T.accountid = '100200109'	#'100200109'。account变量是模型交易界面 添加策略时选择的资金账号，不需要手动填写
	# 获取持仓股票代码并加入T.codes_to_sell，仅加载沪深主板股票
	T.codes_to_sell = []
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	i = 0
	for dt in positions:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		# 仅加载沪深主板股票：上海主板（600000-603999.SH）和深圳主板（000001-000999.SZ）
		is_main_board = (
			(dt.m_strInstrumentID.startswith('6') and not dt.m_strInstrumentID.startswith('688') and dt.m_strExchangeID == 'SH') or
			(dt.m_strInstrumentID.startswith('000') and dt.m_strExchangeID == 'SZ')
		)
		if full_code not in T.codes_to_sell and is_main_board:
			T.codes_to_sell.append(full_code)
			i += 1
			if i >= 3:
				break
	print(f'T.codes_to_sell={T.codes_to_sell}')
	# 读取 JSON 文件获取买入股票代码
	with open('C:/a/trade/量化/中信证券/code/tushare20240930-20251028T212238.json', 'r', encoding='utf-8') as f:
		data = json.load(f)
	T.codes_to_buy_on_market_open = []
	for stock in data['stocks']:
		T.codes_to_buy_on_market_open.append(stock['code'])
	print(f'T.codes_to_buy_on_market_open={T.codes_to_buy_on_market_open}')

	T.codes_all.extend(T.codes_to_buy_on_market_open)
	T.codes_all = list(set(T.codes_all))
	# 获取持仓股票代码并加入T.codes_to_sell_on_market_open
	T.codes_all.extend(T.codes_to_sell)
	T.codes_all = list(set(T.codes_all))
	print(f'T.codes_all={T.codes_all}')
	T.opType_buy = 23 	# 操作类型：23-股票买入，24-股票卖出
	T.opType_sell = 24	# 操作类型：23-股票买入，24-股票卖出
	T.orderType_volume = 1101	# 单股、单账号、普通、股/手方式下单
	T.orderType_amount = 1102	# 单股、单账号、普通、金额方式下单 
	T.prType_sell_1 = 4		# 0：卖5价 1：卖4价 2：卖3价 3：卖2价 4：卖1价 5：最新价 
						# 6：买1价 7：买2价（组合不支持） 8：买3价（组合不支持） 9：买4价（组合不支持）
						# 10：买5价（组合不支持） 11：（指定价）模型价（只对单股情况支持,对组合交易不支持）
						# 12：涨跌停价 13：挂单价 14：对手价
	T.prType_buy_1 = 6
	T.volume = 100
	T.strategyName = 'consecutive_limit_tactics'
	T.quickTrade = 2 	# 0-非立即下单。1-实盘下单（历史K线不起作用）。
						# 2-仿真下单，不会等待k线走完再委托。可以在after_init函数、run_time函数注册的回调函数里进行委托 
	T.userOrderId = '投资备注'
	T.price_invalid = -1
	contextInfo.set_universe(T.codes_all)
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
	# trade_query_info(contextInfo)
	# trade_sell_stock(contextInfo, T.codes_all[8])
	# trade_buy_stock(contextInfo, T.codes_all[0], 10000)
	data_download_stock(contextInfo)

def handlebar(contextInfo):
	return
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

def trade_on_buy_signal_check(contextInfo):
	# print(f'trade_on_buy_signal_check()')
	
	pass

def trade_on_sell_signal_check(contextInfo):
	# print(f'trade_on_sell_signal_check()')
	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	stock_list = contextInfo.get_universe()
	for stock in T.codes_to_sell:
		# 获取当前股价
		market_data = contextInfo.get_market_data_ex(['close'], [stock], period='1m', start_time=bar_time, end_time=bar_time, count=1, dividend_type='front')
		if market_data[stock].empty:
			print(f'trade_on_sell_signal_check(): Error! 未获取到{stock} {get_stock_name(contextInfo, stock)} 的当前股价数据，跳过!')
			continue
		current_price = market_data[stock]['close'].iloc[0]
		# print(f'trade_on_sell_signal_check(): {stock} 当前股价：{current_price:.2f}')

		# 获取昨日收盘价
		market_data_yesterday = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=2, dividend_type='front')
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
				print(f'trade_on_sell_signal_check(): {stock} {get_stock_name(contextInfo, stock)} 满足条件1: 14:55下跌超过3%，卖出')
				trade_sell_stock(contextInfo, stock)

		# 条件2: 触及跌停价
		if current_price <= limit_down_price:
			print(f'trade_on_sell_signal_check(): {stock} {get_stock_name(contextInfo, stock)} 触及跌停价，卖出')
			trade_sell_stock(contextInfo, stock)

def trade_query_info(contextInfo):
	current_date = datetime.datetime.now().date()
	N_days_ago = current_date - datetime.timedelta(days=7)
	orders = get_trade_detail_data(T.accountid, 'stock', 'order')
	print("trade_query_info(): 最近7天的委托记录:")
	for o in orders:
		full_code = f"{o.m_strInstrumentID}.{o.m_strExchangeID}"
		if full_code not in T.codes_all:
			continue
		try:
			order_date = datetime.datetime.strptime(o.m_strInsertTime, '%Y%m%d%H%M%S').date()
			if order_date >= N_days_ago:
				print(f'trade_query_info(): {o.m_strInstrumentID}.{o.m_strExchangeID} {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
				f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')
		except (AttributeError, ValueError):
			# 如果没有时间字段或格式不匹配，打印所有
			print(f'trade_query_info(): {o.m_strInstrumentID}.{o.m_strExchangeID} {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
			f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')

	deals = get_trade_detail_data(T.accountid, 'stock', 'deal')
	print("trade_query_info(): 最近7天的成交记录:")
	for dt in deals:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code not in T.codes_all:
			continue
		try:
			deal_date = datetime.datetime.strptime(dt.m_strTime, '%Y%m%d%H%M%S').date()
			if deal_date >= N_days_ago:
				print(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
				f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')
		except (AttributeError, ValueError):
			# 如果没有时间字段或格式不匹配，打印所有
			print(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
			f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')

	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	print("trade_query_info(): 当前持仓状态:")
	for dt in positions:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code not in T.codes_all:
			continue
		print(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
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
		print(f'trade_sell_stock(): 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')
		volume = dt.m_nCanUseVolume  # 可卖数量
		break
	if volume == 0:
		print(f'trade_sell_stock(): Error! volume == 0! 没有可卖的持仓，跳过卖出操作')
		return
	volume = 100  # 测试时先卖100股
	passorder(T.opType_sell, T.orderType_volume, T.accountid, stock, T.prType_buy_1, T.price_invalid, volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	print(f'trade_sell_stock(): 卖出 {volume} 股')

def trade_buy_stock(contextInfo, stock, buy_amount):
	print(f'trade_buy_stock(): stock={stock} {get_stock_name(contextInfo, stock)}, buy_amount={buy_amount:.2f}元')

	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		print(f'trade_buy_stock(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	print(f'trade_buy_stock(): 当前可用资金: {available_cash:.2f}')

	# 检查买入金额是否超过可用资金
	if buy_amount > available_cash:
		print(f'trade_buy_stock(): Error! 买入金额{buy_amount:.2f}超过可用资金{available_cash:.2f}，跳过!')
		return

	# 使用passorder进行市价买入，orderType=1102表示金额方式
	passorder(T.opType_buy, T.orderType_amount, T.accountid, stock, T.prType_sell_1, T.price_invalid, buy_amount, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	print(f'trade_buy_stock(): {stock} {get_stock_name(contextInfo, stock)} 市价买入金额 {buy_amount:.2f}元')
	
def trade_on_market_open(contextInfo):
	# 确认当前k线的时刻是09:30:00
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time != '09:30:00':
		#print(f'trade_on_market_open(): 当前时间不是09:30:00，当前时间: {current_time}')
		return
	# print(f'trade_on_market_open()')

	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# print(f'trade_on_market_open(): start_time={start_time}, contextInfo.barpos={contextInfo.barpos}')
	for stock in T.codes_to_buy_on_market_open:
		# 获取开盘价 (1分钟K线，count=-1，取09:30:00的开盘价)
		market_data = contextInfo.get_market_data_ex(['open'], [stock], period='1m', count=1, start_time=bar_time, end_time=bar_time, dividend_type='front')
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

		# 获取昨日收盘价 (日线数据，count=2，取第1个)
		market_data_yesterday = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=2, dividend_type='front')
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
		market_data_ma = contextInfo.get_market_data_ex(['close'], [stock], period='1d', count=5, dividend_type='front')
		ma5 = round(market_data_ma[stock]['close'].mean(), 2)
		print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 5日均价: {ma5}')

		# 策略逻辑
		if 3 <= pct <= 8:
			# 以开盘价下单买入500股
			volume = 500
			buy_amount = volume * open_price
			print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 满足买入条件 3% <= pct <= 8%，买入金额{buy_amount:.2f}元')
			trade_buy_stock(contextInfo, stock, buy_amount)
		elif (1 <= pct < 3) or (8 < pct <= 9):
			# 以开盘价下单买入200股
			volume = 200
			buy_amount = volume * open_price
			print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 满足买入条件 1% <= pct < 3% 或 8% < pct <= 9%，买入金额{buy_amount:.2f}元')
			trade_buy_stock(contextInfo, stock, buy_amount)
		elif pct < 1:
			# 以5日均线价格挂单买入 (假设买入100股，可根据需要调整)
			volume = 100
			buy_amount = volume * open_price
			print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 满足买入条件 pct < 1%，买入金额{buy_amount:.2f}元')
			trade_buy_stock(contextInfo, stock, buy_amount)
		else:
			print(f'trade_on_market_open(): {stock} {get_stock_name(contextInfo, stock)} 不满足买入条件')

def account_callback(contextInfo, accountInfo):
	# 输出资金账号状态
	if accountInfo.m_strStatus != '登录成功':
		print(f'account_callback(): Error! 账号状态异常! m_strStatus={accountInfo.m_strStatus}')

# 委托主推函数
def order_callback(contextInfo, orderInfo):
	stock = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, stock)
	full_code = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	if full_code not in T.codes_all:
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
	stock = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, stock)
	full_code = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	if full_code not in T.codes_all:
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

def data_init_db_stock():
    """初始化股票SQLite数据库"""
    conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/stock_data.db')
    cursor = conn.cursor()

    # 创建股票表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stocks (
        ts_code TEXT PRIMARY KEY,
        name TEXT
    )
    ''')

    # 创建合并的股票数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        ts_code TEXT,
        trade_date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        pre_close REAL,
        volume REAL,
        amount REAL,
        turnover_rate REAL,
        pe REAL,
        circ_mv REAL,
        PRIMARY KEY (ts_code, trade_date),
        FOREIGN KEY (ts_code) REFERENCES stocks(ts_code)
    )
    ''')

    conn.commit()
    conn.close()

def data_save_stock_data(df):
    """保存股票数据到数据库，按照data_init_db_stock()的表结构"""
    if df is None or df.empty:
        return
    try:
        conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/stock_data.db')
        cursor = conn.cursor()

        # 提取股票信息
        ts_code = df['ts_code'].iloc[0]
        name = df['name'].iloc[0]

        # 插入stocks表（如果不存在）
        cursor.execute('INSERT OR IGNORE INTO stocks (ts_code, name) VALUES (?, ?)', (ts_code, name))

        # 排序数据按日期
        df_sorted = df.sort_values('trade_date').reset_index(drop=True)

        # 插入数据到stock_data表
        for _, row in df_sorted.iterrows():
            trade_date = row['trade_date']
            open = row['open']
            high = row['high']
            low = row['low']
            close = row['close']
            pre_close = row['pre_close']
            volume = row['volume']
            amount = row['amount']
            turnover_rate = row.get('turnover_rate', None)
            pe = row.get('pe', None)
            circ_mv = row.get('circ_mv', None)

            # 插入stock_data
            cursor.execute('INSERT OR REPLACE INTO stock_data (ts_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate, pe, circ_mv) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (ts_code, trade_date, open, high, low, close, pre_close, volume, amount, turnover_rate, pe, circ_mv))

        conn.commit()
        conn.close()
        # print(f"成功保存 {ts_code} 数据，共 {len(df)} 条记录")
    except Exception as e:
        print(f"保存股票数据时出错: {e}")

def data_download_single_stock_data(contextInfo, ts_code, start_date, end_date):
    """
    使用QMT接口获取单只股票的历史行情数据。
    参数:
        contextInfo: QMT上下文
        ts_code: 股票代码 (如 '600000.SH' 或 '000001.SZ')
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    返回: DataFrame 或 None (如果出错)
    """
    try:
        # 用down_history_data下载数据
        down_history_data(ts_code, '1d', start_date, end_date)
        # time.sleep(0.1)  # 等待下载完成

        # 用get_market_data_ex获取数据，包括close和pre_close
        market_data = contextInfo.get_market_data_ex(['open', 'high', 'low', 'close', 'preClose', 'volume', 'amount'], [ts_code], period='1d', start_time=start_date, end_time=end_date, count=-1, dividend_type='front')
        if ts_code not in market_data or market_data[ts_code].empty:
            print(f'Error! 未获取到 {ts_code} 的市场数据')
            return None
        # print(f'market_data=\n{market_data[ts_code].head()}')
        df = market_data[ts_code].reset_index()
        df['trade_date'] = pd.to_datetime(df['stime'], format='%Y%m%d').dt.strftime('%Y%m%d')
        df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'preClose': 'pre_close', 'volume': 'volume', 'amount': 'amount'})
        # print(f'df=\n{df.head()}')
        df['ts_code'] = ts_code

        # 获取股票名称
        name = get_stock_name(contextInfo, ts_code)
        df['name'] = name

        # 获取换手率
        turnover_df = contextInfo.get_turnover_rate([ts_code], start_date, end_date)
        if not turnover_df.empty:
            turnover_df['trade_date'] = turnover_df.index.astype(str)
            # 假设换手率数据以股票代码为列名，需要重命名为 'turnover_rate'
            if ts_code in turnover_df.columns:
                turnover_df = turnover_df.rename(columns={ts_code: 'turnover_rate'})
                df = df.merge(turnover_df[['trade_date', 'turnover_rate']], on='trade_date', how='left')
            else:
                df['turnover_rate'] = None
                print(f'Warning! {ts_code} 的换手率数据列不存在')
        else:
            df['turnover_rate'] = None
            print(f'Error! 未获取到 {ts_code} 的换手率数据')

        # 获取市盈率和流通市值
        try:
            pe_data = contextInfo.get_financial_data(['利润表.净利润', 'CAPITALSTRUCTURE.circulating_capital', 'CAPITALSTRUCTURE.total_capital'], [ts_code], start_date, end_date, report_type='report_time')
            if pe_data is not None and not pe_data.empty:
                # pe_data的索引是日期，列是s_fa_eps_basic, circulating_capital
                df = df.merge(pe_data, left_on='trade_date', right_index=True, how='left')
                # 计算市盈率
                # df['pe'] = df.apply(lambda row: row['close'] * row['total_capital'] / row['净利润'] if pd.notna(row['净利润']) and row['净利润'] != 0 else None, axis=1)
                df['pe'] = np.nan
                # 计算流通市值
                df['circ_mv'] = df['circulating_capital'] * df['close'] / 10000  # 转换为万元
            else:
                df['pe'] = None
                df['circ_mv'] = None
                print(f'Error! 未获取到 {ts_code} 的财务数据')
        except Exception as e:
            print(f'Error! 获取 {ts_code} 的财务数据失败: {e}')
            df['pe'] = None
            df['circ_mv'] = None

        # 选择需要的列
        df = df[['ts_code', 'name', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount', 'turnover_rate', 'pe', 'circ_mv']]

        return df
    except Exception as e:
        print(f"获取 {ts_code} 数据时出错: {e}")
        return None

def data_get_stock_list(contextInfo):
    """
    获取A股股票代码列表，使用QMT API获取整个市场的股票列表，包括沪深两市，创业板，科创板，和北交所股票。
    返回: 股票代码列表（symbol格式，如 '600000'）
    """
    try:
        # 尝试获取整个A股市场的股票列表
        # QMT API 支持 get_stock_list_in_sector，可以尝试使用 'A股' 或类似板块名
        try:
            all_stocks = contextInfo.get_stock_list_in_sector('沪深A股')
        except:
            # 如果 'A股' 不支持，尝试其他可能的板块名
            try:
                all_stocks = contextInfo.get_stock_list_in_sector('沪深A股')
            except:
                # 如果都不支持，使用指数成份股作为近似
                print("Error! QMT API 不支持直接获取完整A股列表，使用主要指数成份股作为近似")

        # 转换为symbol格式（去掉市场后缀）
        all_codes = []
        for stock in all_stocks:
            if stock.endswith(('.SH', '.SZ', '.BJ')):
                symbol = stock.split('.')[0]
                all_codes.append(symbol)

        # 去重并排序
        all_codes = sorted(list(set(all_codes)))

        # 筛选掉ST股票（通过名称过滤）
        filtered_codes = []
        for code in all_codes:
            if code.startswith('6') or code.startswith('9'):  # 上海（包括科创板）
                ts_code = code + '.SH'
            elif code.startswith(('0', '3')):  # 深圳（包括创业板）
                ts_code = code + '.SZ'
            elif code.startswith('8') or code.startswith('4'):  # 北交所
                ts_code = code + '.BJ'
            else:
                continue

            try:
                name = get_stock_name(contextInfo, ts_code)
                if name and 'ST' not in name:
                    filtered_codes.append(code)
            except:
                continue

        print(f"从QMT API获取并过滤后共发现 {len(filtered_codes)} 只股票")
        return filtered_codes
    except Exception as e:
        print(f"获取股票代码列表失败: {e}")
        return []

def data_download_stock(contextInfo):
    """
    获取所有A股（沪深京）的股票代码列表，并保存到数据库。
    使用QMT接口。
    """
    start_date = '20200101'
    end_date = '20251026'
    # base_delay = 1

    # 初始化数据库
    data_init_db_stock()

    # 获取股票列表
    all_codes = data_get_stock_list(contextInfo)
    if not all_codes:
        print("无法获取股票列表，退出")
        return

    total_stocks = len(all_codes)
    successful_downloads = 0
    failed_downloads = 0

    for i, code in enumerate(all_codes):
        # 计算 ts_code
        if code.startswith('6'):
            ts_code = code + '.SH'
        elif code.startswith(('0', '3')):
            ts_code = code + '.SZ'
        else:
            ts_code = code + '.BJ'

        success = False
        for attempt in range(3):  # 最多重试3次
            try:
                df = data_download_single_stock_data(contextInfo, ts_code, start_date, end_date)
                if df is not None and not df.empty:
                    data_save_stock_data(df)
                    success = True
                    successful_downloads += 1
                    break
                else:
                    print(f"{ts_code} 数据为空，重试中...")
            except Exception as e:
                print(f"获取 {ts_code} 数据失败 (尝试 {attempt + 1}/3): {e}")
                # delay = base_delay + random.uniform(0, 2)
                # time.sleep(delay)
                continue

        if not success:
            print(f"获取 {ts_code} 数据失败，已达到最大重试次数")
            failed_downloads += 1

        # 打印进度
        progress = (i + 1) / total_stocks * 100
        print(f"\r进度: {i + 1}/{total_stocks} ({progress:.1f}%) - 成功: {successful_downloads}, 失败: {failed_downloads}", end='')

    print(f"\n下载完成! 总计: {total_stocks}, 成功: {successful_downloads}, 失败: {failed_downloads}")

def data_load_stock(stock_code, start_date='20150101'):
    """直接从数据库加载指定股票数据"""
    # 转换 stock_code 到 ts_code
    if not stock_code.endswith(('.SH', '.SZ', '.BJ')):
        if stock_code.startswith('6'):
            ts_code = stock_code + '.SH'
        elif stock_code.startswith(('0', '3')):
            ts_code = stock_code + '.SZ'
        else:
            ts_code = stock_code + '.BJ'
    else:
        ts_code = stock_code

    columns = ['股票代码', '股票名称', '日期', '开盘', '收盘', '前收盘', '最高', '最低', '成交量', '成交额', '换手率', '市盈率', '流通市值']
    try:
        conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/stock_data.db')
        cursor = conn.cursor()

        # 查询指定股票数据
        cursor.execute('''
            SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pre_close, d.volume, d.amount, d.turnover_rate, d.pe, d.circ_mv, s.name
            FROM stocks s
            JOIN stock_data d ON s.ts_code = d.ts_code
            WHERE d.ts_code = ? AND d.trade_date >= ?
            ORDER BY d.trade_date
        ''', (ts_code, start_date))
        rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame(columns=columns)

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount', 'turnover_rate', 'pe', 'circ_mv', 'name'])
        # 重命名列为中文
        df = df.rename(columns={
            'ts_code': '股票代码',
            'name': '股票名称',
            'trade_date': '日期',
            'open': '开盘',
            'close': '收盘',
            'pre_close': '前收盘',
            'high': '最高',
            'low': '最低',
            'volume': '成交量',
            'amount': '成交额',
            'turnover_rate': '换手率',
            'pe': '市盈率',
            'circ_mv': '流通市值'
        })
        df = df.reset_index(drop=True)
        return df
    except Exception as e:
        print(f"从数据库加载股票 {stock_code} 数据失败: {e}")
        return pd.DataFrame(columns=columns)
    finally:
        if 'conn' in locals():
            conn.close()
