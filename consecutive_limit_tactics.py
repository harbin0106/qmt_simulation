#encoding:gbk
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
import sqlite3
import time
import os
# Global trade variables
class T():
	pass
T = T()

def init(contextInfo):
	T.download_mode = False
	if T.download_mode:
		return
	log('\n' + '=' * 40 + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '=' * 40)
	init_trade_parameters(contextInfo)
	db_init()
	init_load_codes_in_position(contextInfo)
	# init_load_recommendations_from_excel(contextInfo)
	init_load_recommendations_from_db(contextInfo)
	contextInfo.set_universe(list(set(T.codes_recommended.keys()) | set(T.codes_in_position.keys())))
	contextInfo.set_account(T.accountid)
	# Start the opening call auction timer
	today = date.today()
	# log(f'today={today}')
	startTime = today.strftime('%Y-%m-%d') + ' 09:15:00'
	# For testing only
	# startTime = "2025-10-31 09:15:00"
	contextInfo.run_time("on_timer", "3nSecond", startTime)
	init_open_log_file(contextInfo)

def init_load_codes_in_position(contextInfo):
	# 获取持仓股票代码并加入T.codes_in_position
	T.codes_in_position = {}
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	codes = {f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}" for dt in positions}

	# 获取成交记录，筛选买入操作，记录首次买入日期
	deals = get_trade_detail_data(T.accountid, 'stock', 'deal')
	buy_dates = {}
	for deal in deals:
		code = f"{deal.m_strInstrumentID}.{deal.m_strExchangeID}"
		print(f'deal = {deal}')
		if code in codes and deal.m_nDirection == 48:  # 48 表示买入
			trade_date = deal.m_strTradeDate
			if code not in buy_dates or trade_date < buy_dates[code]:
				buy_dates[code] = trade_date

	# 构建 T.codes_in_position
	for dt in positions:
		code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		if code not in T.codes_in_position:
			T.codes_in_position[code] = {}
			T.codes_in_position[code]['name'] = dt.m_strInstrumentName
			T.codes_in_position[code]['buy_date'] = buy_dates.get(code, '')  # 使用成交日期
			T.codes_in_position[code]['recommend_date'] = ''
			T.codes_in_position[code]['sell_status'] = ''
	log(f'init_load_codes_in_position(): T.codes_in_position=\n{T.codes_in_position}')

def init_load_recommendations_from_excel(contextInfo):
	recommend_date = trade_get_previous_trade_date(contextInfo)
	# 从Excel文件中读取report_df
	path = 'C:/a/trade/量化/中信证券/code/'
	file_name = 'QMT ' + recommend_date + '.xlsx'
	# log(f'path + file_name={path + file_name}')
	report_df = pd.read_excel(path + file_name, sheet_name='Report')
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
	T.codes_recommended = {}
	# 获取上一个交易日
	recommend_date = trade_get_previous_trade_date(contextInfo)
	# 从数据库加载上一个交易日的推荐股票
	df_all = db_load_all()
	# 判断recommend_date是否是数据库里的最新日期
	latest_recommend_date = df_all['recommend_date'].max()
	if recommend_date != latest_recommend_date:
		log(f'init_load_recommendations_from_db(): Warning! recommend_date {recommend_date} is not the latest in database {latest_recommend_date}!')
	df_filtered = df_all[df_all['recommend_date'] == recommend_date]
	for df in df_filtered.itertuples():
		T.codes_recommended[df.code] = {}
		T.codes_recommended[df.code]['name'] = df.name
		T.codes_recommended[df.code]['recommend_date'] = df.recommend_date
		T.codes_recommended[df.code]['sell_status'] = ''
		T.codes_recommended[df.code]['buy_status'] = ''
	T.codes_to_sell = T.codes_recommended.copy()
	log(f'init_load_recommendations_from_db(): recommend_date={recommend_date}, T.codes_recommended=\n{T.codes_recommended}')
	if len(df_filtered) == 0:
		log(f'init_load_recommendations_from_db(): Error! Number of recommendations is 0!')

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
	# 0-卖5价 1-卖4价 2-卖3价 3-卖2价 4-卖1价 5-最新价 6-买1价 7-买2价（组合不支持）8-买3价（组合不支持） 9-买4价（组合不支持）10-买5价（组合不支持）11-（指定价）模型价（只对单股情况支持,对组合交易不支持）12-涨跌停价 13-挂单价 14-对手价
	T.prType_sell_1 = 4
	T.prType_buy_1 = 6
	T.prType_latest = 5
	T.prType_designated = 11
	T.strategyName = 'UpStopTactics'
	# 0-非立即下单。1-实盘下单（历史K线不起作用）。2-仿真下单，不会等待k线走完再委托。可以在after_init函数、run_time函数注册的回调函数里进行委托 
	T.quickTrade = 2 	
	T.price_invalid = 0
	# 佣金
	T.commission_rate = 0.0001
	T.commission_minimum = 5
	# 过户
	T.transfer_fee_rate = 0.00001
	# 印花税
	T.sale_stamp_duty_rate = 0.0005
	# 算法参数
	T.SLOPE = np.log(1.1098)
	T.BUY_THRESHOLD = 1.096

def init_open_log_file(contextInfo):
	# 打开日志文件
	current_date = date.today().strftime('%Y%m%d')
	path = 'C:/a/trade/量化/中信证券/code/'
	file_name = 'QMT ' + current_date + ' log.txt'
	os.startfile(path + file_name)

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
		ticks = contextInfo.get_full_tick(list(set(T.codes_recommended.keys())))
		# log(f'on_timer(): ticks=\n{ticks}')
		for code in list(set(T.codes_recommended.keys())):
			last_price = ticks[code]['lastPrice']
			recommend_date = T.codes_recommended[code]['recommend_date']
			to_buy = trade_is_to_buy(contextInfo, code, last_price, recommend_date)
			if to_buy and T.codes_recommended[code]['buy_status'] == '':
				log(f'on_timer(BUY_AT_CALL_AUCTION): {code} {get_stock_name(contextInfo, code)}, current_time={current_time}, last_price={last_price:.2f}, recommend_date={recommend_date}, to_buy={to_buy}')
				T.codes_recommended[code]['buy_status'] = 'BUY_AT_CALL_AUCTION'

	log(f'on_timer(): T.codes_recommended={T.codes_recommended}')
	# 下单买入
	# 计算标记为'BUY_AT_CALL_AUCTION'的股票个数
	if current_time >= BUY_STOCK_TIME and current_time <= STOP_TIMER_TIME:
		buy_at_open_count = sum(1 for code in T.codes_recommended if T.codes_recommended[code].get('buy_status') == 'BUY_AT_CALL_AUCTION')
		if buy_at_open_count == 0:
			log(f'on_timer(): no stocks to buy......')
			return
		amount_of_each_stock = (trade_get_cash(contextInfo) / buy_at_open_count - T.commission_minimum) / (1 + T.commission_rate + T.transfer_fee_rate) / 1000
		for code in list(set(T.codes_recommended.keys())):
			if T.codes_recommended[code]['buy_status'] != 'BUY_AT_CALL_AUCTION':
				continue
			log(f'on_timer(BUY_AT_CALL_AUCTION): {code} {get_stock_name(contextInfo, code)}, buying at amount {amount_of_each_stock:.2f}元')
			trade_buy_stock_at_up_stop_price_by_amount(contextInfo, code, amount_of_each_stock, 'BUY_AT_CALL_AUCTION')
			T.codes_recommended[code]['buy_status'] = 'BUY_AT_CALL_AUCTION_DONE'
			# 更新qmt数据库? 在回调里做? 待定
	
def after_init(contextInfo):
	if T.download_mode:
		data_download_stock(contextInfo)
	trade_query_info(contextInfo)
	# trade_buy_stock_at_up_stop_price_by_amount(contextInfo, list(T.codes_recommended.keys())[0], 10000, 'test trade_buy_stock_at_up_stop_price_by_amount()')
	# trade_buy_stock_by_amount(contextInfo, list(T.codes_recommended.keys())[0], 3000, 'test trade_buy_stock_by_amount()')
	# trade_buy_stock_by_volume(contextInfo, list(T.codes_recommended.keys())[2], 100, 'test trade_buy_stock_by_volume()')
	# trade_buy_stock_at_up_stop_price_by_volume(contextInfo, list(T.codes_recommended.keys())[1], 100, 'test trade_buy_stock_at_up_stop_price_by_volume()')

def handlebar(contextInfo):
	if T.download_mode:
		return
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
	trade_on_handle_bar(contextInfo)

def trade_get_previous_trade_date(contextInfo):
	today = date.today()
	current_date = today.strftime('%Y%m%d')
	# 否则返回之前的交易日
	trading_dates = contextInfo.get_trading_dates('000001.SH', '', '', 2, '1d')
	if len(trading_dates) != 2:
		log(f'trade_get_previous_trade_date(): Error! 未获取到交易日期数据 for stock 000001.SH! trading_dates={trading_dates}')
		# 如果今日是周六或者周日, 返回最近的周五
		if today.weekday() >= 5:
			previous_trade_date = today - timedelta(days=today.weekday()-4)
			return previous_trade_date.strftime('%Y%m%d')
		return today.strftime('%Y%m%d')
	# 规避trading_dates不能真实反映当前日期的问题
	if trading_dates[1] == current_date:
		recommend_date = trading_dates[0]
	else:
		recommend_date = trading_dates[1]
	return recommend_date

def trade_get_cash(contextInfo):
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'after_init(): Error! 账号未登录! 请检查!')
		return
	return float(account[0].m_dAvailable)	

def trade_on_handle_bar(contextInfo):
	current_date = date.today().strftime('%Y%m%d')
	bar_date = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d')
	if current_date != bar_date:
		log(f'trade_on_handle_bar(): Error! current_date != bar_date! {current_date}, {bar_date}')
		return
	CHECK_CLOSE_PRICE_TIME = '14:56:00'
	SELL_AT_CLOSE_TIME = '14:56:40'
	df = pd.DataFrame.from_dict(T.codes_to_sell, orient='index')
	# log(f'\ntrade_on_handle_bar(): T.codes_to_sell=\n{df.to_string()}')
	# 获取当前时间
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if current_time < SELL_AT_CLOSE_TIME:
		for code in list(set(T.codes_to_sell.keys())):
			# 获取今日开盘价
			market_data_open = contextInfo.get_market_data_ex(['open'], [code], period='1d', count=1, dividend_type='front', fill_data=False, subscribe=True)
			open = market_data_open[code]['open'].iloc[0]
			# 获取昨日收盘价
			market_data_pre_close = contextInfo.get_market_data_ex(['close'], [code], period='1d', count=2, dividend_type='front', fill_data=False, subscribe=True)
			# iloc[0]是昨天，iloc[1]是今天
			pre_close = market_data_pre_close[code]['close'].iloc[0]
			# 获取当前的最新价格
			market_data_last_price = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', count=1, dividend_type='front', fill_data=False, subscribe=True)
			current = market_data_last_price[code]['lastPrice'].iloc[0]
			recommend_date = T.codes_to_sell[code]['recommend_date']
			up_stop_price = contextInfo.get_instrument_detail(code).get('UpStopPrice')
			support_price = trade_get_support_price(contextInfo, code, recommend_date)
			# if code == '002255.SZ':
			# 	open = support_price - 0.01
			# 	pre_close = open / 1.05
			# log(f'{current_time} trade_on_handle_bar(): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommend_date={recommend_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
			# 低于支撑线开盘, 且开盘价低于4%, 以收盘价卖出
			if open <= support_price and open <= pre_close * 1.04 and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_on_handle_bar(SELL_AT_CLOSE): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommend_date={recommend_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_AT_CLOSE'
				continue
			# 低于支撑线开盘, 且开盘价高于4%, 则以开盘价卖出
			if open <= support_price and open > pre_close * 1.04 and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_on_handle_bar(SELL_AT_OPEN): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommend_date={recommend_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_AT_OPEN'
				continue
			# 高于支撑线开盘, 股价下行穿过支撑线, 则以支撑线价格卖出
			if open > support_price and current <= support_price and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_on_handle_bar(SELL_IMMEDIATE): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommend_date={recommend_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_IMMEDIATE'
				continue
			# 高于支撑线开盘, 且股价没有下行穿过支撑线, 但是收盘不涨停, 以收盘价卖出
			# log(f'{open > support_price} {current > support_price} {current < up_stop_price} {current_time >= CHECK_CLOSE_PRICE_TIME} {current_time < SELL_AT_CLOSE_TIME} {T.codes_to_sell[code]["sell_status"] == ""} {current_time}')
			# log(f'{open} > {support_price} {current} > {support_price} {current} < {up_stop_price} {current_time} >= {CHECK_CLOSE_PRICE_TIME} {current_time} < {SELL_AT_CLOSE_TIME} {T.codes_to_sell[code]["sell_status"] == ""} {current_time}')
			if open > support_price and current > support_price and current < up_stop_price and current_time >= CHECK_CLOSE_PRICE_TIME and current_time < SELL_AT_CLOSE_TIME and T.codes_to_sell[code]['sell_status'] == '':
				log(f'{current_time} trade_on_handle_bar(SELL_AT_CLOSE): {code} {get_stock_name(contextInfo, code)}, pre_close={pre_close:.2f}, open={open:.2f}, current={current:.2f}, recommend_date={recommend_date}, up_stop_price={up_stop_price:.2f}, support_price={support_price:.2f}')
				T.codes_to_sell[code]['sell_status'] = 'SELL_AT_CLOSE'
				continue
	
	# log(f'trade_on_handle_bar()1: {T.codes_to_sell}')
	for code in list(set(T.codes_to_sell.keys())):
		if  T.codes_to_sell[code]['sell_status'] == '':
			continue
		if current_time >= SELL_AT_CLOSE_TIME and T.codes_to_sell[code]['sell_status'] == 'SELL_AT_CLOSE':
			# 卖出以收盘价卖出的股票
			trade_sell_stock(contextInfo, code, 'SELL_AT_CLOSE')
			T.codes_to_sell[code]['sell_status'] = 'SELL_AT_CLOSE_DONE'
			continue
		# 卖出以开盘价卖出的股票. 稍后加入迟滞算法. TODO
		if T.codes_to_sell[code]['sell_status'] == 'SELL_AT_OPEN':
			trade_sell_stock(contextInfo, code, 'SELL_AT_OPEN')
			T.codes_to_sell[code]['sell_status'] = 'SELL_AT_OPEN_DONE'
			continue
		# 卖出立即卖出的股票
		if T.codes_to_sell[code]['sell_status'] == 'SELL_IMMEDIATE':
			trade_sell_stock(contextInfo, code, 'SELL_IMMEDIATE')
			T.codes_to_sell[code]['sell_status'] = 'SELL_IMMEDIATE_DONE'
			continue

def trade_is_to_buy(contextInfo, code, open, recommend_date):
	# 使用 recommend_date 获取收盘价
	market_data_recommend = contextInfo.get_market_data_ex(['close'], [code], period='1d', start_time=recommend_date, end_time=recommend_date, count=1, dividend_type='front', fill_data=False, subscribe=True)
	if market_data_recommend[code].empty:
		log(f'trade_is_to_buy(): Error! 未获取到{code} {get_stock_name(contextInfo, code)} 的推荐日{recommend_date}收盘价数据!')
		return False
	pre_close = market_data_recommend[code]['close'].iloc[0]
	support_price = trade_get_support_price(contextInfo, code, recommend_date)
	result = open >= support_price and open >= pre_close * T.BUY_THRESHOLD
	# 判断条件：支撑线上涨突破，且开盘价高于前一日收盘价的BUY_THRESHOLD
	log(f'trade_is_to_buy(): {code} {get_stock_name(contextInfo, code)}, open={open:.2f}, pre_close={pre_close:.2f}, support_price={support_price:.2f}, pre_close * T.BUY_THRESHOLD={pre_close * T.BUY_THRESHOLD:.2f}', result={result})
	return result

def trade_get_support_price(contextInfo, code='600167.SH', recommend_date='20250923', current_date=None):
	if current_date is None:
		current_date = date.today().strftime('%Y%m%d')
	# 判断recommend_date早于current_date，使用日期对象比较
	if datetime.strptime(recommend_date, '%Y%m%d').date() >= datetime.strptime(current_date, '%Y%m%d').date():
		log(f'trade_get_support_price(): Error! recommend_date {recommend_date} is not earlier than current_date {current_date}!')
		return None
	# 获取从recommend_date到current_date的收盘价数据
	market_data = contextInfo.get_market_data_ex(['close'], [code], period='1d', start_time=recommend_date, end_time=current_date, count=-1, dividend_type='front', fill_data=False, subscribe=True)
	# 计算交易日天数，不包括停牌日期
	closes = market_data[code]['close']
	trading_days_count = closes.dropna().shape[0]
	recommendation_close = closes.iloc[0]
	support_price = np.exp((trading_days_count - 1) * T.SLOPE + np.log(recommendation_close * 0.9))
	# log(f'trade_get_support_price(): {code} {get_stock_name(contextInfo, code)}, trading_days_count={trading_days_count}, closes={[f"{x:.2f}" for x in closes.tolist()]}, recommendation_close={recommendation_close:.2f}, support_price={support_price:.2f}, recommend_date={recommend_date}, current_date={current_date}')
	return support_price

def trade_query_info(contextInfo):
	current_date = datetime.now().date()
	N_days_ago = current_date - timedelta(days=7)
	orders = get_trade_detail_data(T.accountid, 'stock', 'order')
	log("trade_query_info(): 最近7天的委托记录:")
	for o in orders:
		order_date = datetime.strptime(o.m_strInsertDate, '%Y%m%d').date()
		if order_date >= N_days_ago:
			log(f'trade_query_info(): {o.m_strInstrumentID}.{o.m_strExchangeID} {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
			f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice}, 成交数量: {o.m_nVolumeTraded}, 成交金额: {o.m_dTradeAmount}, 委托时间: {o.m_strInsertDate} T {o.m_strInsertTime}')

	deals = get_trade_detail_data(T.accountid, 'stock', 'deal')
	log("trade_query_info(): 最近7天的成交记录:")
	for dt in deals:
		deal_date = datetime.strptime(dt.m_strTradeDate, '%Y%m%d').date()
		if deal_date >= N_days_ago:
			log(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
			f'成交价格: {dt.m_dPrice}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount}, 成交时间: {dt.m_strTradeDate} T {dt.m_strTradeTime}')

	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	log("trade_query_info(): 当前持仓状态:")
	for dt in positions:
		log(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	accounts = get_trade_detail_data(T.accountid, 'stock', 'account')
	log("trade_query_info(): 当前账户状态:")
	for dt in accounts:
		log(f'trade_query_info(): 总资产: {dt.m_dBalance:.2f}, 净资产: {dt.m_dAssureAsset:.2f}, 总市值: {dt.m_dInstrumentValue:.2f}',
		f'总负债: {dt.m_dTotalDebit:.2f}, 可用金额: {dt.m_dAvailable:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	return orders, deals, positions, accounts
	
def trade_sell_stock(contextInfo, code, comment):
	volume = 0
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	for dt in positions:
		if f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}" != code:
			continue
		log(f'trade_sell_stock(): 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')
		volume = dt.m_nCanUseVolume  # 可卖数量
		break
	if volume == 0:
		log(f'trade_sell_stock(): {code} {get_stock_name(contextInfo, code)}, {comment}, Error! volume == 0! 没有可卖的持仓，跳过卖出操作')
		return
	volume = 100  # 测试时先卖100股
	passorder(T.opType_sell, T.orderType_volume, T.accountid, code, T.prType_buy_1, T.price_invalid, volume, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_sell_stock(): {code} {get_stock_name(contextInfo, code)}, {comment}, 卖出 {volume} 股 (测试时先卖100股)')

def trade_buy_stock_at_up_stop_price_by_amount(contextInfo, code, buy_amount, comment):
	# log(f'trade_buy_stock_at_up_stop_price_by_amount(): {code} {get_stock_name(contextInfo, code)}, buy_amount={buy_amount:.2f}元')
	# 获取涨停价
	instrument_detail = contextInfo.get_instrument_detail(code)
	up_stop_price = instrument_detail.get('UpStopPrice')
	if up_stop_price is None or up_stop_price <= 0:
		log(f'trade_buy_stock_at_up_stop_price_by_amount(): Error! 无法获取{code}的涨停价!')
		return
	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'trade_buy_stock_at_up_stop_price_by_amount(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	# 计算交易费用
	commission = max(buy_amount * T.commission_rate, T.commission_minimum)
	transfer_fee = buy_amount * T.transfer_fee_rate
	total_cost = buy_amount + commission + transfer_fee
	# 检查总成本是否超过可用资金
	if total_cost > available_cash:
		log(f'trade_buy_stock_at_up_stop_price_by_amount(): Error! 买入金额{buy_amount:.2f} 元 + 佣金{commission:.2f} 元 + 过户费{transfer_fee:.2f} 元 = 总成本{total_cost:.2f} 元超过可用资金{available_cash:.2f} 元，跳过!')
		return
	volume = int(buy_amount / up_stop_price // 100) * 100
	if volume == 0:
		log(f'trade_buy_stock_at_up_stop_price_by_amount(): Error! 资金不足! 可买手数为0!')
		return
	# 使用passorder进行指定价买入，prType=11，price=up_stop_price
	passorder(T.opType_buy, T.orderType_volume, T.accountid, code, T.prType_designated, up_stop_price, volume, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_buy_stock_at_up_stop_price_by_amount(): {code} {get_stock_name(contextInfo, code)} 以涨停价{up_stop_price:.2f}买入 {volume}手金额 {buy_amount:.2f}元')

def trade_buy_stock_at_up_stop_price_by_volume(contextInfo, code, volume, comment):
	log(f'trade_buy_stock_at_up_stop_price_by_volume(): {code} {get_stock_name(contextInfo, code)}, volume={volume} 股')
	# 检查volume是否为100的倍数
	if volume % 100 != 0 or volume < 100:
		log(f'trade_buy_stock_at_up_stop_price_by_volume(): Error! 买入股数{volume} 不是100的倍数或小于100股，跳过!')
		return
	# 获取涨停价
	instrument_detail = contextInfo.get_instrument_detail(code)
	up_stop_price = instrument_detail.get('UpStopPrice')
	if up_stop_price is None or up_stop_price <= 0:
		log(f'trade_buy_stock_at_up_stop_price_by_volume(): Error! 无法获取{code}的涨停价!')
		return
	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'trade_buy_stock_at_up_stop_price_by_volume(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	log(f'trade_buy_stock_at_up_stop_price_by_volume(): 当前可用资金: {available_cash:.2f}')
	# 计算买入金额
	buy_amount = volume * up_stop_price
	# 计算交易费用
	commission = max(buy_amount * T.commission_rate, T.commission_minimum)
	transfer_fee = buy_amount * T.transfer_fee_rate
	total_cost = buy_amount + commission + transfer_fee
	# 检查总成本是否超过可用资金
	if total_cost > available_cash:
		log(f'trade_buy_stock_at_up_stop_price_by_volume(): Error! 买入金额{buy_amount:.2f} 元 + 佣金{commission:.2f} 元 + 过户费{transfer_fee:.2f} 元 = 总成本{total_cost:.2f} 元超过可用资金{available_cash:.2f} 元，跳过!')
		return
	# 使用passorder进行指定价买入，prType=11，price=up_stop_price
	passorder(T.opType_buy, T.orderType_volume, T.accountid, code, T.prType_designated, up_stop_price, volume, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_buy_stock_at_up_stop_price_by_volume(): {code} {get_stock_name(contextInfo, code)} 以涨停价{up_stop_price:.2f}买入 {volume} 股，预计成交金额 {buy_amount:.2f} 元')

def trade_buy_stock_by_amount(contextInfo, code, buy_amount, comment):
	log(f'trade_buy_stock_by_amount(): {code} {get_stock_name(contextInfo, code)}, buy_amount={buy_amount:.2f}元')
	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'trade_buy_stock_by_amount(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	log(f'trade_buy_stock_by_amount(): 当前可用资金: {available_cash:.2f}')

	# 计算交易费用
	commission = max(buy_amount * T.commission_rate, T.commission_minimum)
	transfer_fee = buy_amount * T.transfer_fee_rate
	total_cost = buy_amount + commission + transfer_fee
	# 检查总成本是否超过可用资金
	if total_cost > available_cash:
		log(f'trade_buy_stock_by_amount(): Error! 买入金额{buy_amount:.2f} 元 + 佣金{commission:.2f} 元 + 过户费{transfer_fee:.2f} 元 = 总成本{total_cost:.2f} 元超过可用资金{available_cash:.2f} 元，跳过!')
		return
	#获取当前最新股价, 计算是否能够买入大于100股股票
	# 获取当前最新股价
	market_data = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', count=1, dividend_type='front', fill_data=False, subscribe=True)
	if code not in market_data or market_data[code].empty:
		log(f'trade_buy_stock_by_amount(): Error! 无法获取{code} {get_stock_name(contextInfo, code)}的最新股价!')
		return
	last_price = market_data[code]['lastPrice'].iloc[0]
	log(f'trade_buy_stock_by_amount(): 当前最新股价: {last_price:.2f}')
	# 计算买入100股的成本
	min_volume = 100
	min_cost = min_volume * last_price
	commission_min = max(min_cost * T.commission_rate, T.commission_minimum)
	transfer_fee_min = min_cost * T.transfer_fee_rate
	total_min_cost = min_cost + commission_min + transfer_fee_min
	if total_min_cost > buy_amount:
		log(f'trade_buy_stock_by_amount(): Error! 买入金额不足! 买入最少100股需要总成本{total_min_cost:.2f} 元，超过买入金额 {buy_amount:.2f} 元，跳过!')
		return

	# 使用passorder进行市价买入
	passorder(T.opType_buy, T.orderType_amount, T.accountid, code, T.prType_latest, T.price_invalid, buy_amount, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_buy_stock_by_amount(): {code} {get_stock_name(contextInfo, code)} 市价买入金额 {buy_amount:.2f}元')

def trade_buy_stock_by_volume(contextInfo, code, volume, comment):
	log(f'trade_buy_stock_by_volume(): {code} {get_stock_name(contextInfo, code)}, volume={volume} 股')
	# 检查volume是否为100的倍数
	if volume % 100 != 0 or volume < 100:
		log(f'trade_buy_stock_by_volume(): Error! 买入股数{volume} 不是100的倍数或小于100股，跳过!')
		return
	# 查询当前账户资金余额
	account = get_trade_detail_data(T.accountid, T.accountid_type, 'account')
	if len(account) == 0:
		log(f'trade_buy_stock_by_volume(): Error! 账号未登录! 请检查!')
		return
	available_cash = float(account[0].m_dAvailable)
	log(f'trade_buy_stock_by_volume(): 当前可用资金: {available_cash:.2f}')

	# 获取当前最新股价
	market_data = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', count=1, dividend_type='front', fill_data=False, subscribe=True)
	if code not in market_data or market_data[code].empty:
		log(f'trade_buy_stock_by_volume(): Error! 无法获取{code} {get_stock_name(contextInfo, code)}的最新股价!')
		return
	last_price = market_data[code]['lastPrice'].iloc[0]
	log(f'trade_buy_stock_by_volume(): 当前最新股价: {last_price:.2f}')

	# 计算成交金额
	trade_amount = volume * last_price
	# 计算交易费用
	commission = max(trade_amount * T.commission_rate, T.commission_minimum)
	transfer_fee = trade_amount * T.transfer_fee_rate
	total_cost = trade_amount + commission + transfer_fee
	# 检查总成本是否超过可用资金
	if total_cost > available_cash:
		log(f'trade_buy_stock_by_volume(): Error! 买入金额{trade_amount:.2f} 元 + 佣金{commission:.2f} 元 + 过户费{transfer_fee:.2f} 元 = 总成本{total_cost:.2f} 元超过可用资金{available_cash:.2f} 元，跳过!')
		return

	# 使用passorder进行市价买入，按股数
	passorder(T.opType_buy, T.orderType_volume, T.accountid, code, T.prType_latest, T.price_invalid, volume, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_buy_stock_by_volume(): {code} {get_stock_name(contextInfo, code)} 市价买入 {volume} 股，预计成交金额 {trade_amount:.2f} 元')

def account_callback(contextInfo, accountInfo):
	# 输出资金账号状态
	if accountInfo.m_strStatus != '登录成功':
		log(f'account_callback(): Error! 账号状态异常! m_strStatus={accountInfo.m_strStatus}')

# 委托主推函数
def order_callback(contextInfo, orderInfo):
	code = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	name = get_stock_name(contextInfo, code)
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
	message = '\t' + message
	current_date = date.today().strftime('%Y%m%d')
	path = 'C:/a/trade/量化/中信证券/code/'
	file_name = 'QMT ' + current_date + ' log.txt'
	with open(path + file_name, 'a', encoding='utf-8') as f:
		f.write(message + '\n')

def db_init():
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS stock_status (
		code TEXT PRIMARY KEY,
		name TEXT,
		recommend_date TEXT,
		lateral_high_date TEXT,
		buy_date TEXT,
		buy_price REAL,
		sell_date TEXT,
		sell_price REAL
	)
	''')
	conn.commit()
	conn.close()

def db_save_stock_status(code, name, recommend_date, buy_date, buy_price, sell_date, sell_price):
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('''
	INSERT OR REPLACE INTO stock_status (code, name, recommend_date, buy_date, buy_price, sell_date, sell_price)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	''', (code, name, recommend_date, buy_date, buy_price, sell_date, sell_price))
	conn.commit()
	conn.close()

def db_load_all():
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	df = pd.read_sql_query("SELECT * FROM stock_status", conn)
	conn.close()
	return df

def db_load_stock_status(recommend_date):
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('SELECT code, name, recommend_date, buy_date, buy_price, sell_date, sell_price FROM stock_status WHERE recommend_date = ?', (recommend_date,))
	rows = cursor.fetchall()
	conn.close()
	stock_status_list = []
	for row in rows:
		stock_status_list.append({
			'code': row[0],
			'name': row[1],
			'recommend_date': row[2],
			'buy_date': row[3],
			'buy_price': row[4],
			'sell_date': row[5],
			'sell_price': row[6]
		})
	return stock_status_list

def db_load_stock_status(code):
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('SELECT code, name, recommend_date, buy_date, buy_price, sell_date, sell_price FROM stock_status WHERE code = ?', (code,))
	row = cursor.fetchone()
	conn.close()
	if row:
		return {
			'code': row[0],
			'name': row[1],
			'recommend_date': row[2],
			'buy_date': row[3],
			'buy_price': row[4],
			'sell_date': row[5],
			'sell_price': row[6]
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
		code TEXT PRIMARY KEY,
		name TEXT
	)
	''')

	# 创建合并的股票数据表
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS stock_data (
		code TEXT,
		date TEXT,
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
		PRIMARY KEY (code, date),
		FOREIGN KEY (code) REFERENCES stocks(code)
	)
	''')

	conn.commit()
	conn.close()

def data_save_stock_data(df):
	"""保存股票数据到数据库，按照data_init_db()的表结构"""
	if df is None or df.empty:
		print(f'data_save_stock_data(): Error! df is None or df.empty')
		return
	try:
		conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/stock_data.db')
		cursor = conn.cursor()

		# 提取股票信息
		code = df['code'].iloc[0]
		name = df['name'].iloc[0]

		# 插入stocks表（如果不存在）
		cursor.execute('INSERT OR IGNORE INTO stocks (code, name) VALUES (?, ?)', (code, name))

		# 排序数据按日期
		df_sorted = df.sort_values('date').reset_index(drop=True)

		# 插入数据到stock_data表
		for _, row in df_sorted.iterrows():
			date = row['date']
			open = row['open']
			high = row['high']
			low = row['low']
			close = row['close']
			pre_close = row['pre_close']
			volume = row['volume']
			amount = row['amount']
			turnover_rate = row['turnover_rate']
			pe = row['pe']
			circ_mv = row['circ_mv']

			# 插入stock_data
			cursor.execute('INSERT OR REPLACE INTO stock_data (code, date, open, high, low, close, pre_close, volume, amount, turnover_rate, pe, circ_mv) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (code, date, open, high, low, close, pre_close, volume, amount, turnover_rate, pe, circ_mv))

		conn.commit()
		conn.close()
		# print(f"data_save_stock_data(): 成功保存 {code} 数据，共 {len(df)} 条记录")
	except Exception as e:
		print(f"data_save_stock_data(): Error! 保存股票数据时出错: {e}")

def data_download_single_stock_data(contextInfo, code, start_date, end_date):
	"""
	使用QMT接口获取单只股票的历史行情数据。
	参数:
		contextInfo: QMT上下文
		code: 股票代码 (如 '600000.SH' 或 '000001.SZ')
		start_date: 开始日期 (YYYYMMDD)
		end_date: 结束日期 (YYYYMMDD)
	返回: DataFrame 或 None (如果出错)
	"""
	try:
		# 用down_history_data下载数据
		down_history_data(code, '1d', start_date, end_date)

		# 用get_market_data_ex获取数据，包括close和pre_close
		market_data = contextInfo.get_market_data_ex(['open', 'high', 'low', 'close', 'preClose', 'volume', 'amount'], [code], period='1d', start_time=start_date, end_time=end_date, count=-1, dividend_type='front', fill_data=False)
		if code not in market_data or market_data[code].empty:
			print(f'data_download_single_stock_data(): Error! 未获取到 {code} 的市场数据')
			return None
		# print(f'market_data=\n{market_data[code].head()}')
		df = market_data[code].reset_index()
		df['date'] = pd.to_datetime(df['stime'], format='%Y%m%d').dt.strftime('%Y%m%d')
		df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'preClose': 'pre_close', 'volume': 'volume', 'amount': 'amount'})
		# print(f'df=\n{df.head()}')
		df['code'] = code

		# 获取股票名称
		name = get_stock_name(contextInfo, code)
		df['name'] = name

		# 获取换手率
		turnover_df = contextInfo.get_turnover_rate([code], start_date, end_date)
		if not turnover_df.empty:
			turnover_df['date'] = turnover_df.index.astype(str)
			# 假设换手率数据以股票代码为列名，需要重命名为 'turnover_rate'
			if code in turnover_df.columns:
				turnover_df = turnover_df.rename(columns={code: 'turnover_rate'})
				df = df.merge(turnover_df[['date', 'turnover_rate']], on='date', how='left')
			else:
				df['turnover_rate'] = None
				print(f'data_download_single_stock_data(): Warning! {code} 的换手率数据列不存在')
		else:
			df['turnover_rate'] = None
			print(f'data_download_single_stock_data(): Error! 未获取到 {code} 的换手率数据')

		# 获取市盈率和流通市值
		try:
			pe_data = contextInfo.get_financial_data(['利润表.净利润', 'CAPITALSTRUCTURE.circulating_capital', 'CAPITALSTRUCTURE.total_capital'], [code], start_date, end_date, report_type='report_time')
			if pe_data is not None and not pe_data.empty:
				# pe_data的索引是日期，列是s_fa_eps_basic, circulating_capital
				df = df.merge(pe_data, left_on='date', right_index=True, how='left')
				# 计算市盈率
				# df['pe'] = df.apply(lambda row: row['close'] * row['total_capital'] / row['净利润'] if pd.notna(row['净利润']) and row['净利润'] != 0 else None, axis=1)
				df['pe'] = np.nan
				# 计算流通市值
				df['circ_mv'] = df['circulating_capital'] * df['close'] / 10000  # 转换为万元
			else:
				df['pe'] = None
				df['circ_mv'] = None
				print(f'data_download_single_stock_data(): Error! 未获取到 {code} 的财务数据')
		except Exception as e:
			print(f'data_download_single_stock_data(): Error! 获取 {code} 的财务数据失败: {e}')
			df['pe'] = None
			df['circ_mv'] = None

		# 选择需要的列
		df = df[['code', 'name', 'date', 'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount', 'turnover_rate', 'pe', 'circ_mv']]

		return df
	except Exception as e:
		print(f"data_download_single_stock_data(): Error! 获取 {code} 数据时出错: {e}")
		return None

def data_get_stock_list(contextInfo):
	"""
	获取A股股票代码列表，使用QMT API获取整个市场的股票列表，包括沪深两市，创业板，科创板，和北交所股票。
	返回: 股票代码列表（带市场后缀，如 '600000.SH'）
	"""
	# 尝试获取整个A股市场的股票列表
	# QMT API 支持 get_stock_list_in_sector，可以尝试使用 'A股' 或类似板块名
	try:
		all_codes = contextInfo.get_stock_list_in_sector('沪深A股')
	except:
		# 如果都不支持，使用指数成份股作为近似
		print("data_get_stock_list(): Error! QMT API 不支持直接获取完整A股列表!")
		return []

	# 筛选掉ST股票（通过名称过滤）
	filtered_codes = []
	for code in all_codes:
		try:
			name = get_stock_name(contextInfo, code)
			if name and 'ST' not in name:
				filtered_codes.append(code)
		except:
			print("data_get_stock_list(): Error! get_stock_name() exception!")
			return []

	print(f"从QMT API获取并过滤后共发现 {len(filtered_codes)} 只股票. 过滤前总数: {len(all_codes)}")
	return filtered_codes

def data_download_stock(contextInfo):
	"""
	获取所有A股（沪深京）的股票代码列表，并保存到数据库。
	使用QMT接口。
	"""
	end_date = date.today().strftime('%Y%m%d')
	start_date = (date.today() - relativedelta(weeks=2)).strftime('%Y%m%d')

	# 初始化数据库
	data_init_db()

	# 获取股票列表
	all_codes = data_get_stock_list(contextInfo)
	if not all_codes:
		print("data_download_stock(): Error! 无法获取股票列表，退出")
		return

	total_stocks = len(all_codes)
	successful_downloads = 0
	failed_downloads = 0

	for i, code in enumerate(all_codes):
		success = False
		for attempt in range(3):  # 最多重试3次
			try:
				df = data_download_single_stock_data(contextInfo, code, start_date, end_date)
				if df is not None and not df.empty:
					data_save_stock_data(df)
					success = True
					successful_downloads += 1
					break
				else:
					print(f"{code} 数据为空，重试中...")
			except Exception as e:
				print(f"data_download_stock(): Error! 获取 {code} 数据失败 (尝试 {attempt + 1}/3): {e}")
				continue

		if not success:
			print(f"data_download_stock(): Error! 获取 {code} 数据失败，已达到最大重试次数")
			failed_downloads += 1

		# 打印进度
		progress = (i + 1) / total_stocks * 100
		print(f"\r进度: {i + 1}/{total_stocks} ({progress:.1f}%) - 成功: {successful_downloads}, 失败: {failed_downloads}", end='')

	print(f"\n下载完成! 总计: {total_stocks}, 成功: {successful_downloads}, 失败: {failed_downloads}")

def data_load_stock(code, start_date='20200101'):
	"""直接从数据库加载指定股票数据"""
	# 转换 code 到 code
	if not code.endswith(('.SH', '.SZ', '.BJ')):
		print(f'data_load_stock(): Error! 股票代码 {code} 格式不正确，缺少市场后缀(.SH/.SZ/.BJ)')
		return pd.DataFrame()
	code = code
	columns = ['股票代码', '股票名称', '日期', '开盘', '收盘', '前收盘', '最高', '最低', '成交量', '成交额', '换手率', '市盈率', '流通市值']
	try:
		conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/stock_data.db')
		cursor = conn.cursor()

		# 查询指定股票数据
		cursor.execute('''
			SELECT d.code, d.date, d.open, d.high, d.low, d.close, d.pre_close, d.volume, d.amount, d.turnover_rate, d.pe, d.circ_mv, s.name
			FROM stocks s
			JOIN stock_data d ON s.code = d.code
			WHERE d.code = ? AND d.date >= ?
			ORDER BY d.date
		''', (code, start_date))
		rows = cursor.fetchall()

		if not rows:
			return pd.DataFrame(columns=columns)

		# 转换为DataFrame
		df = pd.DataFrame(rows, columns=['code', 'date', 'open', 'high', 'low', 'close', 'pre_close', 'volume', 'amount', 'turnover_rate', 'pe', 'circ_mv', 'name'])
		# 重命名列为中文
		df = df.rename(columns={
			'code': '股票代码',
			'name': '股票名称',
			'date': '日期',
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
		print(f"data_load_stock(): Error! 从数据库加载股票 {code} 数据失败: {e}")
		return pd.DataFrame(columns=columns)
	finally:
		if 'conn' in locals():
			conn.close()
