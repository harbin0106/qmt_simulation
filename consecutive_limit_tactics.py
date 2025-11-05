#encoding:gbk
import pandas as pd
import numpy as np
import talib
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
import sqlite3
import time
from xtquant import xtdata
# Global trade variables
class T():
	pass
T = T()

def init(contextInfo):
	log('\n' + '=' * 20 + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '=' * 20)
	init_trade_parameters(contextInfo)
	db_init()
	init_load_codes_in_position(contextInfo)
	init_load_recommendations_from_excel(contextInfo)
	init_load_recommendations_from_db(contextInfo)

	T.codes_all = list(set(T.codes_recommendated.keys()) | set(T.codes_in_position.keys()))
	log(f'init(): T.codes_all=\n{T.codes_all}')
	contextInfo.set_universe(T.codes_all)
	contextInfo.set_account(T.accountid)
	# Start the opening call auction timer
	today = date.today()
	# log(f'today={today}')
	startTime = today.strftime('%Y-%m-%d') + ' 09:15:00'
	# For testing only
	# startTime = "2025-10-31 09:15:00"
	contextInfo.run_time("on_timer", "3nSecond", startTime)
	return
	contextInfo.set_slippage(1, 0.003)
	contextInfo.set_commission(0.0001)
	contextInfo.max_single_order = 10000
	contextInfo.max_position = 0.99

def init_load_codes_in_position(contextInfo):
	# 获取持仓股票代码并加入T.codes_in_position
	T.codes_in_position = {}
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	for dt in positions:
		code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if code not in T.codes_in_position:
			T.codes_in_position[code] = get_stock_name(contextInfo, full_code)
	log(f'init_load_codes_in_position(): T.codes_in_position=\n{T.codes_in_position}')

def init_load_recommendations_from_excel(contextInfo):
	# 从Excel文件中读取report_df
	report_df = pd.read_excel('C:/a/trade/量化/中信证券/code/龙头股票筛选结果2025-11-05T08-49-21.xlsx', sheet_name='Report')
	# 按照日期从小到大排序
	report_df = report_df.sort_values(by='指定日期T', ascending=True)
	# 去掉不需要的列
	report_df = report_df.drop(columns=['T+1增加率', 'T+2增加率', 'T+3增加率', 'T+4增加率', 'T+5增加率'])
	log(f'init_load_recommendations_from_excel(): report_df=\n{report_df}')
	# 保存股票状态到数据库
	for _, row in report_df.iterrows():
		code = row['股票代码']
		name = row['股票名称']
		r_date = str(row['指定日期T'])
		db_save_stock_status(code, name, r_date, None, None, None, None)

def init_load_recommendations_from_db(contextInfo):
	T.codes_recommendated = {}
	# 获取上一个交易日
	trading_dates = contextInfo.get_trading_dates('000001.SH', '', '', 2, '1d')
	if len(trading_dates) != 2:
		log(f'init_load_recommendations_from_db(): Error! 未获取到交易日期数据 for stock 000001.SH!')
		return
	log(f'init_load_recommendations_from_db(): trading_dates={trading_dates}')
	# 规避trading_dates不能真实反映当前日期的问题
	current_date = date.today().strftime('%Y%m%d')
	if trading_dates[1] == current_date:
		recommendation_date = trading_dates[0]
	else:
		recommendation_date = trading_dates[1]
	# 从数据库加载上一个交易日的推荐股票
	df_all = db_load_all()
	df_filtered = df_all[df_all['r_date'] == recommendation_date]
	for df in df_filtered.itertuples():
		T.codes_recommendated[df.code] = {}
		T.codes_recommendated[df.code]['name'] = df.name
		T.codes_recommendated[df.code]['r_date'] = df.r_date
		T.codes_recommendated[df.code]['sell_status'] = ''
	T.codes_to_sell = T.codes_recommendated.copy()
	log(f'init_load_recommendations_from_db(): recommendation_date={recommendation_date}, T.codes_recommendated=\n{T.codes_recommendated}')
	if len(df_filtered) == 0:
		log(f'init_load_recommendations_from_db(): Error! Number of recommendation is 0!')

def init_trade_parameters(contextInfo):
	T.accountid_type = 'STOCK'
	#'100200109'。account变量是模型交易界面 添加策略时选择的资金账号，不需要手动填写
	T.accountid = '10100002555'
	# 操作类型：23-股票买入，24-股票卖出
	T.opType_buy = 23
	# 操作类型：23-股票买入，24-股票卖出
	T.opType_sell = 24
	# 单股、单账号、普通、股/手方式下单
	T.orderType_volume = 1101
	# 单股、单账号、普通、金额方式下单
	T.orderType_amount = 1102
	# 0：卖5价 1：卖4价 2：卖3价 3：卖2价 4：卖1价 5：最新价 6：买1价 7：买2价（组合不支持）8：买3价（组合不支持） 9：买4价（组合不支持）10：买5价（组合不支持）11：（指定价）模型价（只对单股情况支持,对组合交易不支持）12：涨跌停价 13：挂单价 14：对手价
	T.prType_sell_1 = 4
	T.prType_buy_1 = 6
	T.prType_designated = 11
	T.volume = 100
	T.strategyName = 'consecutive_limit_tactics'
	# 0-非立即下单。1-实盘下单（历史K线不起作用）。2-仿真下单，不会等待k线走完再委托。可以在after_init函数、run_time函数注册的回调函数里进行委托 
	T.quickTrade = 2 	
	T.userOrderId = '投资备注'
	T.price_invalid = -1
	T.codes_to_buy = []
	T.codes_to_sell_at_close = []
	T.codes_to_sell_at_open = []
	T.codes_to_sell_immediate = []
	T.SLOPE = np.log(1.1098)
	T.BUY_THRESHOLD = 1.096

def on_timer(contextInfo):
	if not hasattr(on_timer, 'stop_timer'):
		on_timer.stop_timer = False
	if on_timer.stop_timer:
		return
	current_time = datetime.now().strftime("%H:%M:%S")
	STOP_TIMER_TIME = "09:25:00"
	CHECK_PRICE_TIME = "09:24:00"
	BUY_STOCK_TIME = "09:24:30"
	if current_time > STOP_TIMER_TIME:
		log("on_timer(): 集合竞价结束")
		on_timer.stop_timer = True
		return
	# Check prices only
	if current_time >= CHECK_PRICE_TIME and current_time < BUY_STOCK_TIME:
		log(f'\non_timer(): current_time={current_time}, check price......')
		ticks = contextInfo.get_full_tick(list(set(T.codes_recommendated.keys())))
		# log(f'on_timer(): ticks=\n{ticks}')
		for code in list(set(T.codes_recommendated.keys())):
			last_price = ticks[code]['lastPrice']
			# trading_dates = contextInfo.get_trading_dates('000001.SH', '', '', 2, '1d')
			# if len(trading_dates) < 2:
			# 	log(f'on_timer(): Error! 未获取到交易日期数据 for stock 000001.SH!')
			# 	continue
			recommendation_date = T.codes_recommendated[code]['r_date']
			to_buy = trade_is_to_buy(contextInfo, code, last_price, recommendation_date)
			log(f'on_timer(): {code} {get_stock_name(contextInfo, code)}, current_time={current_time}, last_price={last_price:.2f}, recommendation_date={recommendation_date}, to_buy={to_buy}')
			if to_buy and code not in T.codes_to_buy:
				T.codes_to_buy.append(code)
		log(f'on_timer(): T.codes_to_buy={T.codes_to_buy}')
		return
	# 下单买入
	if current_time > BUY_STOCK_TIME and current_time <= STOP_TIMER_TIME and len(T.codes_to_buy) > 0:
		log(f'\non_timer(): current_time={current_time}, buy stock......')
		amount_of_each_stock = T.cash / len(T.codes_to_buy) / 1000
		for code in T.codes_to_buy:
			log(f'on_timer(): {code} {get_stock_name(contextInfo, code)}, buying at amount {amount_of_each_stock:.2f}元')
			trade_buy_stock_at_up_stop_price(contextInfo, code, amount_of_each_stock)  # 买入1万元
			# 更新qmt数据库? 在回调里做? 待定
		# Clear the buy list
		T.codes_to_buy = []
		return
	
def after_init(contextInfo):
	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'after_init(): Error! 账号未登录! 请检查!')
		return
	T.cash = float(account[0].m_dAvailable)	
	log(f'after_init(): T.cash={T.cash:.0f} 元')	
	# account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	# if len(account) == 0:
	# 	log(f'after_init(): Error! 账号{T.accountid} 未登录! 请检查!')
	# 	return
	# trade_query_info(contextInfo)
	# trade_sell_stock(contextInfo, T.codes_all[8])
	# trade_buy_stock(contextInfo, T.codes_all[0], 10000)
	# trade_buy_stock_at_up_stop_price(contextInfo, '002759.SZ', 10000)
	# data_download_stock(contextInfo)

def handlebar(contextInfo):
	# bar_time= timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# log(f"handlebar(): bar_time={timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y-%m-%d %H:%M:%S')}")
	# Validate period
	if contextInfo.period != 'tick':
		log(f'handlebar(): Error! contextInfo.period != "tick"! contextInfo.period={contextInfo.period}')
		return
	# Skip history bars ####################################
	if not contextInfo.is_last_bar():
		# log(f'handlebar(): contextInfo.is_last_bar()={contextInfo.is_last_bar()}')
		return
	trade_is_to_sell(contextInfo)
	return

	# # 开盘交易逻辑
	# trade_on_market_open(contextInfo)
	# # 检查是否出现了卖出信号
	trade_on_sell_signal_check(contextInfo)

def trade_is_to_sell(contextInfo):
	current_date = date.today().strftime('%Y%m%d')
	bar_date = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d')
	if current_date != bar_date:
		return
	CHECK_CLOSE_PRICE_TIME = '14:56:00'
	SELL_AT_CLOSE_TIME = '14:56:40'
	log(f'\ntrade_is_to_sell(): {T.codes_to_sell}')
	# 获取当前时间
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time < SELL_AT_CLOSE_TIME or True:
		for code in list(set(T.codes_to_sell.keys())):
			# 获取今日开盘价
			market_data_open = contextInfo.get_market_data_ex(['open'], [code], period='1d', count=1, dividend_type='front', fill_data=False, subscribe=True)
			open = market_data_open[code]['open'].iloc[0]
			# 获取昨日收盘价
			market_data_pre_close = contextInfo.get_market_data_ex(['close'], [code], period='1d', count=2, dividend_type='front', fill_data=False, subscribe=True)
			pre_close = market_data_pre_close[code]['close'].iloc[0]  # iloc[0]是昨天，iloc[1]是今天
			# 获取当前的最新价格
			market_data_last_price = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', count=1, dividend_type='front', fill_data=False, subscribe=True)
			current = market_data_last_price[code]['lastPrice'].iloc[0]
			recommendation_date = T.codes_to_sell[code]['r_date']
			up_stop_price = contextInfo.get_instrument_detail(code).get('UpStopPrice')
			support_price = trade_get_support_price(contextInfo, code, recommendation_date)
			# if code == '002255.SZ':
			# 	open = support_price - 0.01
			# 	pre_close = open / 1.05
			log(f'{current_time} trade_is_to_sell(): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommendation_date={recommendation_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
			# 低于支撑线开盘, 且开盘价低于4%, 以收盘价卖出
			if open <= support_price and open <= pre_close * 1.04 and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_is_to_sell(SELL_AT_CLOSE): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommendation_date={recommendation_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_AT_CLOSE'
				continue
			# 低于支撑线开盘, 且开盘价高于4%, 则以开盘价卖出
			if open <= support_price and open > pre_close * 1.04 and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_is_to_sell(SELL_AT_OPEN): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommendation_date={recommendation_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_AT_OPEN'
				continue
			# 高于支撑线开盘, 股价下行穿过支撑线, 则以支撑线价格卖出
			if open > support_price and current <= support_price and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_is_to_sell(SELL_IMMEDIATE): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommendation_date={recommendation_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_IMMEDIATE'
				continue
			# 高于支撑线开盘, 且股价没有下行穿过支撑线, 但是收盘不涨停, 以收盘价卖出
			# log(f'{open > support_price} {current > support_price} {current < up_stop_price} {current_time >= CHECK_CLOSE_PRICE_TIME} {current_time < SELL_AT_CLOSE_TIME} {T.codes_to_sell[code]["sell_status"] == ""} {current_time}')
			# log(f'{open} > {support_price} {current} > {support_price} {current} < {up_stop_price} {current_time} >= {CHECK_CLOSE_PRICE_TIME} {current_time} < {SELL_AT_CLOSE_TIME} {T.codes_to_sell[code]["sell_status"] == ""} {current_time}')
			if open > support_price and current > support_price and current < up_stop_price and current_time >= CHECK_CLOSE_PRICE_TIME and current_time < SELL_AT_CLOSE_TIME and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_is_to_sell(SELL_AT_CLOSE): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommendation_date={recommendation_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_AT_CLOSE'
				continue
	
	# log(f'trade_is_to_sell()1: {T.codes_to_sell}')
	for code in list(set(T.codes_to_sell.keys())):
		if  T.codes_to_sell[code]['sell_status'] == '':
			continue
		if current_time >= SELL_AT_CLOSE_TIME and T.codes_to_sell[code]['sell_status'] == 'SELL_AT_CLOSE':
			log(f'trade_is_to_sell(): 当前时间>={SELL_AT_CLOSE_TIME}，卖出以收盘价卖出的股票')
			# 卖出以收盘价卖出的股票
			trade_sell_stock(contextInfo, code)
			T.codes_to_sell[code]['sell_status'] = 'SELL_AT_CLOSE_DONE'
			continue
		# 卖出以开盘价卖出的股票. 稍后加入迟滞算法. 如何避免反复进入? 要全局存储标志位, 要写入数据库, 并且在初始化时要加载进来. TODO
		if T.codes_to_sell[code]['sell_status'] == 'SELL_AT_OPEN':
			trade_sell_stock(contextInfo, code)
			T.codes_to_sell[code]['sell_status'] = 'SELL_AT_OPEN_DONE'
			continue
		# 卖出立即卖出的股票
		if T.codes_to_sell[code]['sell_status'] == 'SELL_IMMEDIATE':
			trade_sell_stock(contextInfo, code)
			T.codes_to_sell[code]['sell_status'] = 'SELL_IMMEDIATE_DONE'
			continue

def trade_is_to_buy(contextInfo, code, open, recommendation_date):
	# 使用 recommendation_date 获取收盘价
	market_data_recommendation_date = contextInfo.get_market_data_ex(['close'], [code], period='1d', start_time=recommendation_date, end_time=recommendation_date, count=1, dividend_type='front', fill_data=False, subscribe=True)
	if market_data_recommendation_date[code].empty:
		log(f'trade_is_to_buy(): Error! 未获取到{code} {get_stock_name(contextInfo, code)} 的昨日收盘价数据!')
		return False
	pre_close = market_data_recommendation_date[code]['close'].iloc[0]
	support_price = trade_get_support_price(contextInfo, code, recommendation_date)
	# 判断条件：支撑线上涨突破，且开盘价高于前一日收盘价的BUY_THRESHOLD
	log(f'trade_is_to_buy(): {code} {get_stock_name(contextInfo, code)}, open={open:.2f}, pre_close={pre_close:.2f}, support_price={support_price:.2f}, pre_close * T.BUY_THRESHOLD={pre_close * T.BUY_THRESHOLD:.2f}', {open >= support_price and open >= pre_close * T.BUY_THRESHOLD})
	return open >= support_price and open >= pre_close * T.BUY_THRESHOLD

def trade_on_buy_signal_check(contextInfo):
	# log(f'trade_on_buy_signal_check()')	
	pass

def trade_get_support_price(contextInfo, code='600167.SH', recommendation_date='20250923', current_date=None):
	if current_date is None:
		current_date = date.today().strftime('%Y%m%d')
	# 判断recommendation_date早于current_date
	if recommendation_date >= current_date:
		log(f'trade_get_support_price(): Error! recommendation_date {recommendation_date} is not earlier than current_date {current_date}!')
		return None
	# 获取从recommendation_date到current_date的收盘价数据
	market_data = contextInfo.get_market_data_ex(['close'], [code], period='1d', start_time=recommendation_date, end_time=current_date, count=-1, dividend_type='front', fill_data=False, subscribe=True)
	# 计算交易日天数，不包括停牌日期
	closes = market_data[code]['close']
	trading_days_count = closes.dropna().shape[0]
	recommendation_close = closes.iloc[0]
	support_price = np.exp((trading_days_count - 1) * T.SLOPE + np.log(recommendation_close * 0.9))
	# log(f'trade_get_support_price(): {code} {get_stock_name(contextInfo, code)}, trading_days_count={trading_days_count}, closes={[f"{x:.2f}" for x in closes.tolist()]}, recommendation_close={recommendation_close:.2f}, support_price={support_price:.2f}, recommendation_date={recommendation_date}, current_date={current_date}')
	return support_price

def trade_on_sell_signal_check(contextInfo):
	log(f'trade_on_sell_signal_check()')
	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	for code in T.codes_to_sell['股票代码']:
		# 获取当前股价
		market_data = contextInfo.get_market_data_ex(['close'], [code], period='tick', start_time=bar_time, end_time=bar_time, count=1, dividend_type='front', fill_data=False, subscribe=True)
		if market_data[code].empty:
			log(f'trade_on_sell_signal_check(): Error! 未获取到{code} {get_stock_name(contextInfo, code)} 的当前股价数据，跳过!')
			continue
		current_price = market_data[code]['close'].iloc[0]
		# log(f'trade_on_sell_signal_check(): {code} 当前股价：{current_price:.2f}')

		# 获取昨日收盘价
		market_data_yesterday = contextInfo.get_market_data_ex(['close'], [code], period='1d', count=2, dividend_type='front', fill_data=False, subscribe=True)
		if market_data_yesterday[code].empty:
			log(f'trade_on_sell_signal_check(): Error! 未获取到{code} {get_stock_name(contextInfo, code)} 的昨日收盘价数据，跳过!')
			continue
		pre_close = market_data_yesterday[code]['close'].iloc[0]  # iloc[0]是昨天
		# log(f'trade_on_sell_signal_check(): {code} 昨日收盘价: {pre_close:.2f}')

		# 计算跌停价 (A股跌停价为昨日收盘价的90%)
		limit_down_price = round(pre_close * 0.9, 2)
		# log(f'trade_on_sell_signal_check(): {code} 跌停价: {limit_down_price:.2f}')

		# 条件1: 在14:55时刻，股价相对于昨日收盘价下跌超过3%
		current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
		if current_time == '14:55:00':
			pct = (current_price - pre_close) / pre_close * 100
			# log(f'trade_on_sell_signal_check(): {code} 涨幅: {pct:.2f}%')
			if pct < -3:
				log(f'trade_on_sell_signal_check(): {code} {get_stock_name(contextInfo, code)} 满足条件1: 14:55下跌超过3%，卖出')
				trade_sell_stock(contextInfo, code)

		# 条件2: 触及跌停价
		if current_price <= limit_down_price:
			log(f'trade_on_sell_signal_check(): {code} {get_stock_name(contextInfo, code)} 触及跌停价，卖出')
			trade_sell_stock(contextInfo, code)

def trade_query_info(contextInfo):
	current_date = datetime.datetime.now().date()
	N_days_ago = current_date - datetime.timedelta(days=7)
	orders = get_trade_detail_data(T.accountid, 'stock', 'order')
	log("trade_query_info(): 最近7天的委托记录:")
	for o in orders:
		full_code = f"{o.m_strInstrumentID}.{o.m_strExchangeID}"
		if full_code not in T.codes_all:
			continue
		try:
			order_date = datetime.datetime.strptime(o.m_strInsertTime, '%Y%m%d%H%M%S').date()
			if order_date >= N_days_ago:
				log(f'trade_query_info(): {o.m_strInstrumentID}.{o.m_strExchangeID} {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
				f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')
		except (AttributeError, ValueError):
			# 如果没有时间字段或格式不匹配，打印所有
			log(f'trade_query_info(): Error! {o.m_strInstrumentID}.{o.m_strExchangeID} {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
			f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额:{o.m_dTradeAmount}')

	deals = get_trade_detail_data(T.accountid, 'stock', 'deal')
	log("trade_query_info(): 最近7天的成交记录:")
	for dt in deals:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code not in T.codes_all:
			continue
		try:
			deal_date = datetime.datetime.strptime(dt.m_strTime, '%Y%m%d%H%M%S').date()
			if deal_date >= N_days_ago:
				log(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
				f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')
		except (AttributeError, ValueError):
			# 如果没有时间字段或格式不匹配，打印所有
			log(f'trade_query_info(): Error! {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
			f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}')

	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	log("trade_query_info(): 当前持仓状态:")
	for dt in positions:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code not in T.codes_all:
			continue
		log(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	accounts = get_trade_detail_data(T.accountid, 'stock', 'account')
	log("trade_query_info(): 当前账户状态:")
	for dt in accounts:
		log(f'trade_query_info(): 总资产: {dt.m_dBalance:.2f}, 净资产: {dt.m_dAssureAsset:.2f}, 总市值: {dt.m_dInstrumentValue:.2f}',
		f'总负债: {dt.m_dTotalDebit:.2f}, 可用金额: {dt.m_dAvailable:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	return orders, deals, positions, accounts
	
def trade_sell_stock(contextInfo, code):
	log(f'trade_sell_stock(): {code} {get_stock_name(contextInfo, code)}')
	volume = 0
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	for dt in positions:
		full_code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if full_code != code:
			continue
		log(f'trade_sell_stock(): 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')
		volume = dt.m_nCanUseVolume  # 可卖数量
		break
	if volume == 0:
		log(f'trade_sell_stock(): Error! volume == 0! 没有可卖的持仓，跳过卖出操作')
		return
	volume = 100  # 测试时先卖100股
	passorder(T.opType_sell, T.orderType_volume, T.accountid, code, T.prType_buy_1, T.price_invalid, volume, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	log(f'trade_sell_stock(): 卖出 {volume} 股 (测试时先卖100股)')

def trade_buy_stock_at_up_stop_price(contextInfo, code, buy_amount):
	# log(f'trade_buy_stock_at_up_stop_price(): {code} {get_stock_name(contextInfo, code)}, buy_amount={buy_amount:.2f}元')
	# 获取涨停价
	instrument_detail = contextInfo.get_instrument_detail(code)
	up_stop_price = instrument_detail.get('UpStopPrice')
	if up_stop_price is None or up_stop_price <= 0:
		log(f'trade_buy_stock_at_up_stop_price(): Error! 无法获取{code}的涨停价!')
		return
	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'trade_buy_stock_at_up_stop_price(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	# 检查买入金额是否超过可用资金
	if buy_amount > available_cash:
		log(f'trade_buy_stock_at_up_stop_price(): Error! 买入金额{buy_amount:.2f} 元超过可用资金{available_cash:.2f} 元，跳过!')
		return
	# 使用passorder进行指定价买入，prType=11，price=up_stop_price
	passorder(T.opType_buy, T.orderType_amount, T.accountid, code, T.prType_designated, up_stop_price, buy_amount, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	log(f'trade_buy_stock_at_up_stop_price(): {code} {get_stock_name(contextInfo, code)} 以涨停价{up_stop_price:.2f}买入金额 {buy_amount:.0f}元')

def trade_buy_stock(contextInfo, code, buy_amount):
	log(f'trade_buy_stock(): {code} {get_stock_name(contextInfo, code)}, buy_amount={buy_amount:.2f}元')

	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'trade_buy_stock(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	log(f'trade_buy_stock(): 当前可用资金: {available_cash:.2f}')

	# 检查买入金额是否超过可用资金
	if buy_amount > available_cash:
		log(f'trade_buy_stock(): Error! 买入金额{buy_amount:.2f}超过可用资金{available_cash:.2f}，跳过!')
		return

	# 使用passorder进行市价买入，orderType=1102表示金额方式
	passorder(T.opType_buy, T.orderType_amount, T.accountid, code, T.prType_sell_1, T.price_invalid, buy_amount, T.strategyName, T.quickTrade, T.userOrderId, contextInfo)
	log(f'trade_buy_stock(): {code} {get_stock_name(contextInfo, code)} 市价买入金额 {buy_amount:.2f}元')
	
def trade_on_market_open(contextInfo):
	# 确认当前k线的时刻是09:30:00
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time != '09:30:00':
		#log(f'trade_on_market_open(): 当前时间不是09:30:00，当前时间: {current_time}')
		return
	# log(f'trade_on_market_open()')

	bar_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# log(f'trade_on_market_open(): start_time={start_time}, contextInfo.barpos={contextInfo.barpos}')
	for code in T.codes_recommendated:
		# 获取开盘价 (1分钟K线，count=-1，取09:30:00的开盘价)
		market_data = contextInfo.get_market_data_ex(['open'], [code], period='1m', count=1, start_time=bar_time, end_time=bar_time, dividend_type='front', fill_data=False, subscribe=True)
		# log(f'trade_on_market_open(): market_data={market_data}')
		open = None
		for i, stime in enumerate(market_data[code].index):
			dt = pd.to_datetime(str(stime), format='%Y%m%d%H%M%S')
			if dt.time() == datetime.time(9, 30, 0):
				open = market_data[code]['open'].iloc[i]
				break
		if open is None:
			log(f'trade_on_market_open(): Error! {code} {get_stock_name(contextInfo, code)} 未找到09:30:00的开盘价数据, 跳过!')
			continue
		log(f'\ntrade_on_market_open(): {code} {get_stock_name(contextInfo, code)} 开盘价: {open:.2f}')

		# 获取昨日收盘价 (日线数据，count=2，取第1个)
		market_data_yesterday = contextInfo.get_market_data_ex(['close'], [code], period='1d', count=2, dividend_type='front', fill_data=False, subscribe=True)
		# log(f'market_data_yesterday={market_data_yesterday}')
		pre_close = market_data_yesterday[code]['close'].iloc[0]  # iloc[0]是昨天，iloc[1]是今天
		log(f'trade_on_market_open(): {code} {get_stock_name(contextInfo, code)} 昨日收盘价: {pre_close:.2f}')

		# 计算涨幅
		if pre_close == 0:
			log(f'trade_on_market_open(): Error! {code} {get_stock_name(contextInfo, code)} 昨日收盘价为0，跳过!')
			continue
		pct = round((open - pre_close) / pre_close * 100, 2)
		log(f'trade_on_market_open(): {code} {get_stock_name(contextInfo, code)} 涨幅: {pct}%')

		# 计算5日均价 (日线数据)
		market_data_ma = contextInfo.get_market_data_ex(['close'], [code], period='1d', count=5, dividend_type='front', fill_data=False, subscribe=True)
		ma5 = round(market_data_ma[code]['close'].mean(), 2)
		log(f'trade_on_market_open(): {code} {get_stock_name(contextInfo, code)} 5日均价: {ma5}')

		# 使用 trade_is_to_buy 判断是否买入
		yesterday_date_str = market_data_yesterday[code]['close'].index[0]
		if trade_is_to_buy(contextInfo, code, open, yesterday_date_str):
			# 买入逻辑，根据涨幅决定买入金额
			if 3 <= pct <= 8:
				volume = 500
			elif (1 <= pct < 3) or (8 < pct <= 9):
				volume = 200
			else:
				volume = 100
			buy_amount = volume * open
			log(f'trade_on_market_open(): {code} {get_stock_name(contextInfo, code)} 满足买入条件，买入金额{buy_amount:.2f}元')
			trade_buy_stock(contextInfo, code, buy_amount)
		else:
			log(f'trade_on_market_open(): {code} {get_stock_name(contextInfo, code)} 不满足买入条件')

def account_callback(contextInfo, accountInfo):
	# 输出资金账号状态
	if accountInfo.m_strStatus != '登录成功':
		log(f'account_callback(): Error! 账号状态异常! m_strStatus={accountInfo.m_strStatus}')

# 委托主推函数
def order_callback(contextInfo, orderInfo):
	code = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, code)
	full_code = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	if full_code not in T.codes_all:
		return
	# log(f'order_callback(): {code} {name}, m_nOrderStatus={orderInfo.m_nOrderStatus}, m_dLimitPrice={orderInfo.m_dLimitPrice}, m_nOpType={orderInfo.m_nOpType}, m_nVolumeTotalOriginal={orderInfo.m_nVolumeTotalOriginal}, m_nVolumeTraded={orderInfo.m_nVolumeTraded}')
	# 检查委托状态并记录成交结果
	if orderInfo.m_nOrderStatus == 56:  # 已成
		log(f'order_callback(): 委托已全部成交 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 成交数量: {orderInfo.m_nVolumeTraded}, 成交均价: {orderInfo.m_dTradedPrice:.2f}, 成交金额: {orderInfo.m_dTradeAmount:.2f}')
	elif orderInfo.m_nOrderStatus == 55:  # 部成
		log(f'order_callback(): 委托部分成交 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 已成交数量: {orderInfo.m_nVolumeTraded}, 剩余数量: {orderInfo.m_nVolumeTotal}, 成交金额: {orderInfo.m_dTradeAmount:.2f}')
	elif orderInfo.m_nOrderStatus == 54:  # 已撤
		log(f'order_callback(): 委托已撤销 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}')
	else:
		return
		# log(f'order_callback(): 委托状态更新 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 状态: {orderInfo.m_nOrderStatus}')

# 成交主推函数
def deal_callback(contextInfo, dealInfo):
	code = f"{dealInfo.m_strInstrumentID}.{dealInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, code)
	# log(f'deal_callback(): {code} {name}, m_dPrice={dealInfo.m_dPrice}, m_dPrice={dealInfo.m_dPrice}, m_nVolume={dealInfo.m_nVolume}')
	# 检查成交结果并记录
	# log(f'deal_callback(): 成交确认 - 股票: {code} {name}, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}, 买卖方向: {dealInfo.m_nDirection}')
	# 可以在这里添加更多逻辑，如更新全局变量、发送通知等
	# 例如，检查是否为买入或卖出，并更新持仓统计
	if dealInfo.m_nDirection == 48:  # 买入
		log(f'deal_callback(): {code} {name}, 买入成交 - 更新持仓信息, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}')
	elif dealInfo.m_nDirection == 49:  # 卖出
		log(f'deal_callback(): {code} {name}, 卖出成交 - 更新持仓信息, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}')

# 持仓主推函数
def position_callback(contextInfo, positionInfo):
	code = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, code)
	full_code = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	if full_code not in T.codes_all:
		return
	# log(f'position_callback(): {code} {name}, m_nVolume={positionInfo.m_nVolume}, m_nFrozenVolume={positionInfo.m_nFrozenVolume}')
	# 检查持仓变化并记录
	log(f'position_callback(): 持仓更新 - 股票: {code} {name}, 总持仓量: {positionInfo.m_nVolume}, 可用数量: {positionInfo.m_nCanUseVolume}, 冻结数量: {positionInfo.m_nFrozenVolume}, 成本价: {positionInfo.m_dOpenPrice:.2f}, 持仓盈亏: {positionInfo.m_dPositionProfit:.2f}')
	# 可以在这里添加逻辑，如检查持仓是否为0，触发卖出信号等
	# if positionInfo.m_nVolume == 0:
	# 	log(f'position_callback(): 持仓清空 - 股票: {code} {name}')
	
#下单出错回调函数
def orderError_callback(contextInfo, passOrderInfo, msg):
	code = f"{passOrderInfo.orderCode}"
	name = get_stock_name(contextInfo, code)
	log(f'\norderError_callback(): 下单错误 - 股票: {code} {name}, 错误信息: {msg}')
	# 可以在这里添加逻辑，如重试下单或发送警报

def get_stock_name(contextInfo, code):
	try:
		instrument = contextInfo.get_instrument_detail(code)
		return instrument.get('InstrumentName')
	except:
		return "get_stock_name(): Error! 未知"

def log(*args):
	message = ' '.join(str(arg) for arg in args)
	with open(r'C:\a\trade\量化\中信证券\code\QMT.txt', 'a', encoding='utf-8') as f:
		f.write(message + '\n')

def db_init():
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS stock_status (
		code TEXT PRIMARY KEY,
		name TEXT,
		r_date TEXT,
		b_date TEXT,
		b_price REAL,
		s_date TEXT,
		s_price REAL
	)
	''')
	conn.commit()
	conn.close()

def db_save_stock_status(code, name, r_date, b_date, b_price, s_date, s_price):
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('''
	INSERT OR REPLACE INTO stock_status (code, name, r_date, b_date, b_price, s_date, s_price)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	''', (code, name, r_date, b_date, b_price, s_date, s_price))
	conn.commit()
	conn.close()

def db_load_all():
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	df = pd.read_sql_query("SELECT * FROM stock_status", conn)
	conn.close()
	return df

def db_load_stock_status(r_date):
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('SELECT code, name, r_date, b_date, b_price, s_date, s_price FROM stock_status WHERE r_date = ?', (r_date,))
	rows = cursor.fetchall()
	conn.close()
	stock_status_list = []
	for row in rows:
		stock_status_list.append({
			'code': row[0],
			'name': row[1],
			'r_date': row[2],
			'b_date': row[3],
			'b_price': row[4],
			's_date': row[5],
			's_price': row[6]
		})
	return stock_status_list

def db_load_stock_status(code):
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('SELECT code, name, r_date, b_date, b_price, s_date, s_price FROM stock_status WHERE code = ?', (code,))
	row = cursor.fetchone()
	conn.close()
	if row:
		return {
			'code': row[0],
			'name': row[1],
			'r_date': row[2],
			'b_date': row[3],
			'b_price': row[4],
			's_date': row[5],
			's_price': row[6]
		}
	else:
		return None
	
def data_init_db():
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
		vol REAL,
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
		print(f'data_save_stock_data(): Error! df is None or df.empty')
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
			vol = row['volume']
			amount = row['amount']
			turnover_rate = row.get('turnover_rate', None)
			pe = row.get('pe', None)
			circ_mv = row.get('circ_mv', None)

			# 插入stock_data
			cursor.execute('INSERT OR REPLACE INTO stock_data (ts_code, trade_date, open, high, low, close, pre_close, vol, amount, turnover_rate, pe, circ_mv) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (ts_code, trade_date, open, high, low, close, pre_close, vol, amount, turnover_rate, pe, circ_mv))

		conn.commit()
		conn.close()
		# print(f"成功保存 {ts_code} 数据，共 {len(df)} 条记录")
	except Exception as e:
		print(f"data_save_stock_data(): Error! 保存股票数据时出错: {e}")

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
		market_data = contextInfo.get_market_data_ex(['open', 'high', 'low', 'close', 'preClose', 'volume', 'amount'], [ts_code], period='1d', start_time=start_date, end_time=end_date, count=-1, dividend_type='front', fill_data=False)
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
			print(f'data_download_single_stock_data(): Error! 获取 {ts_code} 的财务数据失败: {e}')
			df['pe'] = None
			df['circ_mv'] = None

		# 选择需要的列
		df = df[['ts_code', 'name', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount', 'turnover_rate', 'pe', 'circ_mv']]

		return df
	except Exception as e:
		print(f"data_download_single_stock_data(): Error! 获取 {ts_code} 数据时出错: {e}")
		return None

def data_get_stock_list(contextInfo):
	"""
	获取A股股票代码列表，使用QMT API获取整个市场的股票列表，包括沪深两市，创业板，科创板，和北交所股票。
	返回: 股票代码列表（带市场后缀，如 '600000.SH'）
	"""
	# 尝试获取整个A股市场的股票列表
	# QMT API 支持 get_stock_list_in_sector，可以尝试使用 'A股' 或类似板块名
	try:
		all_stocks = contextInfo.get_stock_list_in_sector('沪深A股')
	except:
		# 如果都不支持，使用指数成份股作为近似
		print("data_get_stock_list(): Error! QMT API 不支持直接获取完整A股列表!")
		return []

	# 筛选掉ST股票（通过名称过滤）
	filtered_codes = []
	for code in all_stocks:
		try:
			name = get_stock_name(contextInfo, code)
			if name and 'ST' not in name:
				filtered_codes.append(code)
		except:
			print("data_get_stock_list(): Error! get_stock_name() exception!")
			return []

	print(f"从QMT API获取并过滤后共发现 {len(filtered_codes)} 只股票. 过滤前总数: {len(all_stocks)}")
	return filtered_codes

def data_download_stock(contextInfo):
	"""
	获取所有A股（沪深京）的股票代码列表，并保存到数据库。
	使用QMT接口。
	"""
	end_date = date.today().strftime('%Y%m%d')
	start_date = (date.today() - relativedelta(months=1)).strftime('%Y%m%d')
	# base_delay = 1

	# 初始化数据库
	data_init_db()

	# 获取股票列表
	all_codes = data_get_stock_list(contextInfo)
	if not all_codes:
		print("无法获取股票列表，退出")
		return

	total_stocks = len(all_codes)
	successful_downloads = 0
	failed_downloads = 0

	for i, ts_code in enumerate(all_codes):
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
				print(f"data_download_stock(): Error! 获取 {ts_code} 数据失败 (尝试 {attempt + 1}/3): {e}")
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

def data_load_stock(code, start_date='20200101'):
	"""直接从数据库加载指定股票数据"""
	# 转换 code 到 ts_code
	if not code.endswith(('.SH', '.SZ', '.BJ')):
		print(f'data_load_stock(): Error! 股票代码 {code} 格式不正确，缺少市场后缀(.SH/.SZ/.BJ)')
		return pd.DataFrame()
	ts_code = code
	columns = ['股票代码', '股票名称', '日期', '开盘', '收盘', '前收盘', '最高', '最低', '成交量', '成交额', '换手率', '市盈率', '流通市值']
	try:
		conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/stock_data.db')
		cursor = conn.cursor()

		# 查询指定股票数据
		cursor.execute('''
			SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pre_close, d.vol, d.amount, d.turnover_rate, d.pe, d.circ_mv, s.name
			FROM stocks s
			JOIN stock_data d ON s.ts_code = d.ts_code
			WHERE d.ts_code = ? AND d.trade_date >= ?
			ORDER BY d.trade_date
		''', (ts_code, start_date))
		rows = cursor.fetchall()

		if not rows:
			return pd.DataFrame(columns=columns)

		# 转换为DataFrame
		df = pd.DataFrame(rows, columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'vol', 'amount', 'turnover_rate', 'pe', 'circ_mv', 'name'])
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
			'vol': '成交量',
			'amount': '成交额',
			'turnover_rate': '换手率',
			'pe': '市盈率',
			'circ_mv': '流通市值'
		})
		df = df.reset_index(drop=True)
		return df
	except Exception as e:
		print(f"data_load_stock(): Error! 从数据库加载股票 {code} 数据失败: {e}")
		return pd.DataFrame(columns=columns)
	finally:
		if 'conn' in locals():
			conn.close()
