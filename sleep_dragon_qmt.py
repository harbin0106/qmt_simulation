#encoding:gbk
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
import sqlite3
import time
import os
import copy
import talib as ta
# Global trade variables
class T():
	pass
T = T()

def init(contextInfo):
	T.download_mode = False
	if T.download_mode:
		return
	init_trade_parameters(contextInfo)
	# init_clear_log_file(contextInfo)
	log('\n' + '=' * 40 + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '=' * 40)
	db_init()
	init_load_codes_in_position(contextInfo)
	# init_load_recommendations_from_excel(contextInfo)
	init_load_recommendations_from_db(contextInfo)
	T.codes_all = T.codes_recommended
	contextInfo.set_universe(list(set(T.codes_all.keys())))
	contextInfo.set_account(T.accountid)
	return
	# Start the opening call auction timer
	today = date.today()
	# log(f'today={today}')
	startTime = today.strftime('%Y-%m-%d') + ' 09:15:00'
	# For testing only
	# startTime = "2025-10-31 09:15:00"
	contextInfo.run_time("on_timer", "3nSecond", startTime)

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
		# 打印所有成员变量的内容
		# log('init_load_codes_in_position(): All attributes of deal:')
		# for attr in dir(deal):
		# 	if not attr.startswith('_'):
		# 		try:
		# 			value = getattr(deal, attr)
		# 			log(f'  {attr}: {value}')
		# 		except:
		# 			log(f'  {attr}: <无法获取>')
		if code in codes and deal.m_nDirection == 48:  # 48 表示买入
			trade_date = deal.m_strTradeDate
			if code not in buy_dates or trade_date < buy_dates[code]:
				buy_dates[code] = trade_date

	# 构建 T.codes_in_position
	for dt in positions:
		code = f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}"
		# 打印所有成员变量的内容
		# log('init_load_codes_in_position(): All attributes of dt:')
		# for attr in dir(dt):
		# 	if not attr.startswith('_'):
		# 		try:
		# 			value = getattr(dt, attr)
		# 			log(f'  {attr}: {value}')
		# 		except:
		# 			log(f'  {attr}: <无法获取>')
		if code not in T.codes_in_position:
			T.codes_in_position[code] = {}
			T.codes_in_position[code]['name'] = dt.m_strInstrumentName
			T.codes_in_position[code]['last_buy_date'] = buy_dates.get(code, '')  # 使用成交日期
	df = pd.DataFrame.from_dict(T.codes_in_position, orient='index')
	log(f'init_load_codes_in_position(): T.codes_in_position=\n{df.to_string()}')

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
		db_save_stock_status(code, name, r_date, r_date, None, None, None, None, 'Y', '', '')

def init_load_recommendations_from_db(contextInfo):
	T.codes_recommended = {}
	# 获取上一个交易日
	# recommend_date = trade_get_previous_trade_date(contextInfo)
	# 从数据库加载上一个交易日的推荐股票
	df_all = db_load_all()
	log(f'df_all=\n{df_all}')
	# 判断recommend_date是否是数据库里的最新日期
	# latest_recommend_date = df_all['recommend_date'].max()
	# if recommend_date != latest_recommend_date:
	# 	log(f'init_load_recommendations_from_db(): Warning! recommend_date {recommend_date} is not the latest in database {latest_recommend_date}!')
	df_filtered = df_all[df_all['is_valid'] == 'Y']
	# 根据df_all的表格结构, 把所有数据转换到T.codes_recommended里
	for df in df_filtered.itertuples():
		code = df.code
		if code not in T.codes_recommended:
			T.codes_recommended[code] = {
				'name': df.name,
				'recommend_date': df.recommend_date,
				'lateral_high_date': df.lateral_high_date,
				'last_buy_date': None,
				'price': None,
				'last_price': None,
				'type': None,
				'last_type': None,
				'lateral_high': None,
				'records': []
			}
			if df.name != get_stock_name(contextInfo, df.code):
				log(f'init_load_recommendations_from_db(): Warning! Invalid stock name! {df.code} {df.name} get_stock_name(contextInfo, df.code)={get_stock_name(contextInfo, df.code)}')
		record = {
			'date': df.date,
			'type': df.type,
			'price': df.price,
			'shares': df.shares,
			'profit': df.profit,
			'comment': df.comment
		}
		T.codes_recommended[code]['records'].append(record)
	# 枚举T.codes_recommended所有的code, 把它的最近日期'records'的内容复制给T.codes_rocommended[code]['type'].
	for code in T.codes_recommended:
		if T.codes_recommended[code]['records']:
			latest_record = max(T.codes_recommended[code]['records'], key=lambda r: r['date'])
			T.codes_recommended[code]['last_type'] = latest_record['type']
			T.codes_recommended[code]['last_price'] = latest_record['price']
			# 枚举T.codes_recommended所有的code, 把它最近日期'type'为'BUY_AT_LOCAL_MIN'的'records'日期复制给T.codes_recommended[code]['last_buy_date']
			buy_at_local_min_records = [r for r in T.codes_recommended[code]['records'] if r['type'] == 'BUY_AT_LOCAL_MIN']
			if buy_at_local_min_records:
				latest_buy_record = max(buy_at_local_min_records, key=lambda r: r['date'])
				T.codes_recommended[code]['last_buy_date'] = latest_buy_record['date']
	df = pd.DataFrame.from_dict(T.codes_recommended, orient='index')
	log(f'init_load_recommendations_from_db(): T.codes_recommended=\n{T.codes_recommended}')
	if len(df_filtered) == 0:
		log(f'init_load_recommendations_from_db(): Error! Number of recommendations is 0!')
	# 判断T.codes_in_postition在T.codes_recommended中是否存在
	for code in T.codes_in_position:
		if code not in T.codes_recommended:
			log(f'init_load_recommendations_from_db(): Warning! code {code} {T.codes_in_position[code]["name"]} in position but not in recommendations!')
			continue
		if T.codes_recommended[code]['last_type'] not in ['BUY_AT_LOCAL_MIN', 'BUY_AT_STEP_1', 'BUY_AT_STEP_2', 'BUY_AT_STEP_3', 'SELL_AT_STEP_1', 'SELL_AT_STEP_2', 'SELL_AT_STEP_3']:
			log(f'init_load_recommendations_from_db(): Error! code {code} {T.codes_in_position[code]["name"]} in position "last_type" is invalid! {T.codes_recommended[code]["last_type"]}')
			# T.codes_recommended[code]['last_type'] = 'BUY_AT_LOCAL_MIN'

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
	T.strategyName = 'sleep_dragon'
	# 0-非立即下单。1-实盘下单（历史K线不起作用）。2-仿真下单，不会等待k线走完再委托。可以在after_init函数、run_time函数注册的回调函数里进行委托 
	T.quickTrade = 1 	
	T.price_invalid = 0
	# 佣金
	# T.commission_rate = 0.0001
	T.commission_rate = 0.003
	T.commission_minimum = 5
	# 过户
	T.transfer_fee_rate = 0.00001
	# 印花税
	T.sale_stamp_duty_rate = 0.0005
	# 算法参数
	T.SLOPE = np.log(1.07)
	T.BUY_AMOUNT = None
	T.MARKET_OPEN_TIME = '09:30:00'
	T.CHECK_CLOSE_PRICE_TIME = '14:55:30'
	T.TRANSACTION_CLOSE_TIME = '14:55:40'
	T.MARKET_CLOSE_TIME= '15:00:00'	
	T.TARGET_DATE = '20251219'
	T.CURRENT_DATE = date.today().strftime('%Y%m%d') if T.TARGET_DATE == '' else T.TARGET_DATE
	T.last_codes_all = None
	# 用于过滤log
	T.last_current_time = {}

def open_log_file(contextInfo):
	# 打开日志文件
	path = 'C:/a/trade/量化/中信证券/code/'
	file_name = 'QMT ' + T.CURRENT_DATE + ' log.txt'
	os.startfile(path + file_name)

def init_clear_log_file(contextInfo):
	# 清除日志文件内容
	path = 'C:/a/trade/量化/中信证券/code/'
	file_name = 'QMT ' + T.CURRENT_DATE + ' log.txt'
	with open(path + file_name, 'w', encoding='utf-8') as f:
		pass  # 清空文件内容

def on_timer(contextInfo):
	if not hasattr(on_timer, 'stop_timer'):
		on_timer.stop_timer = False
	if on_timer.stop_timer:
		return
	# current_time = datetime.now().strftime("%H:%M:%S")
	# STOP_TIMER_TIME = "09:25:00"
	# CHECK_PRICE_TIME = "09:24:00"
	# BUY_STOCK_TIME = "09:24:30"
	# if current_time > STOP_TIMER_TIME:
	# 	log("on_timer(): 集合竞价结束")
	# 	on_timer.stop_timer = True
	# 	return
	# # Check prices only
	# if current_time >= CHECK_PRICE_TIME and current_time < BUY_STOCK_TIME:
	# 	log(f'\non_timer(): current_time={current_time}, check price......')
	# 	ticks = contextInfo.get_full_tick(list(set(T.codes_recommended.keys())))
	# 	# log(f'on_timer(): ticks=\n{ticks}')
	# 	for code in list(set(T.codes_recommended.keys())):
	# 		last_price = ticks[code]['lastPrice']
	# 		recommend_date = T.codes_recommended[code]['recommend_date']
	# 		to_buy = trade_is_to_buy(contextInfo, code, last_price, recommend_date)
	# 		if to_buy and T.codes_recommended[code]['type'] == '':
	# 			log(f'on_timer(BUY_AT_CALL_AUCTION): {code} {T.codes_all[code]["name"]}, current_time={current_time}, last_price={last_price:.2f}, recommend_date={recommend_date}, to_buy={to_buy}')
	# 			T.codes_recommended[code]['type'] = 'BUY_AT_CALL_AUCTION'
	# 			db_insert_record(code, status=T.codes_recommended[code]['type'])

	# log(f'on_timer(): T.codes_recommended={T.codes_recommended}')
	# # 下单买入
	# # 计算标记为'BUY_AT_CALL_AUCTION'的股票个数
	# if current_time >= BUY_STOCK_TIME and current_time <= STOP_TIMER_TIME:
	# 	buy_at_open_count = sum(1 for code in T.codes_recommended if T.codes_recommended[code].get('type') == 'BUY_AT_CALL_AUCTION')
	# 	if buy_at_open_count == 0:
	# 		log(f'on_timer(): no stocks to buy......')
	# 		return
	# 	amount_of_each_stock = (trade_get_cash(contextInfo) / buy_at_open_count - T.commission_minimum) / (1 + T.commission_rate + T.transfer_fee_rate) / 1000
	# 	for code in list(set(T.codes_recommended.keys())):
	# 		if T.codes_recommended[code]['type'] != 'BUY_AT_CALL_AUCTION':
	# 			continue
	# 		log(f'on_timer(BUY_AT_CALL_AUCTION): {code} {T.codes_all[code]["name"]}, buying at amount {amount_of_each_stock:.2f}元')
	# 		trade_buy_stock_at_up_stop_price_by_amount(contextInfo, code, amount_of_each_stock, 'BUY_AT_CALL_AUCTION')
	# 		T.codes_recommended[code]['type'] = 'BUY_AT_CALL_AUCTION'
	# 		db_insert_record(code, status=T.codes_recommended[code]['type'])
	
def after_init(contextInfo):
	if T.download_mode:
		data_download_stock(contextInfo)
	trade_query_info(contextInfo)
	# trade_buy_stock_at_up_stop_price_by_amount(contextInfo, list(T.codes_recommended.keys())[0], 10000, 'test trade_buy_stock_at_up_stop_price_by_amount()')
	# trade_buy_stock_by_amount(contextInfo, '002300.SZ', 10000000, '测试买入1千万')
	# trade_buy_stock_by_volume(contextInfo, list(T.codes_recommended.keys())[2], 100, 'test trade_buy_stock_by_volume()')
	# trade_buy_stock_at_up_stop_price_by_volume(contextInfo, list(T.codes_recommended.keys())[1], 100, 'test trade_buy_stock_at_up_stop_price_by_volume()')
	# df = pd.DataFrame.from_dict(T.codes_all, orient='index')
	# log(f'after_init(): T.codes_all=\n{df.to_string()}')
	# 计算lateral_high_date是否正确
	# trade_refine_codes_all(contextInfo)
	# trade_get_recommendations(contextInfo)
	open_log_file(contextInfo)

# def trade_get_recommendations(contextInfo):
# 	query = f"2025年12月1日 百日新高 主板股票 非ST股票 股价小于15元"
# 	df = pywencai.get(query=query, query_type='stock', sort_order='desc', loop=True)
# 	log(f'df=\n{df}')

def trade_refine_codes_all(contextInfo):
	start_date = '20240418'
	filtered_codes_all = {}
	for code in list(set(T.codes_all.keys())):
		market_data_high = contextInfo.get_market_data_ex(['high'], [code], period='1d', start_time=start_date, end_time=T.codes_all[code]['recommend_date'], count=-1, dividend_type='front', fill_data=False, subscribe=True)
		# 获取当前交易日和昨日的收盘价
		market_data_close = contextInfo.get_market_data_ex(['close'], [code], period='1d', end_time=T.CURRENT_DATE, count=2, dividend_type='front', fill_data=False, subscribe=True)
		highs = market_data_high[code]['high'].astype(float)
		closes= market_data_close[code]['close'].astype(float)
		lateral_high_date = highs.idxmax()
		lateral_high = max(highs)
		if T.codes_all[code]['lateral_high_date'] != lateral_high_date:
			log(f'trade_refine_codes_all(): code={code}, name={T.codes_all[code]["name"]}. Error! Invalid lateral_high_date! lateral_high_date={lateral_high_date}, db={T.codes_all[code]["lateral_high_date"]}')
			continue
		# 过滤掉还没有接近水平突破线且买入日期, 卖出日期为空的股票. closes[0]为昨日, closes[1]为今日
		if closes[0] < lateral_high * 0.9 and T.codes_all[code]['last_day'] is None and T.codes_all[code]['sell_date'] is None:
			log(f'trade_refine_codes_all(): {code} {T.codes_all[code]["name"]} is removed from T.codes_all. closes[0]={closes[0]:.2f}')
			continue
		# log(f'trade_refine_codes_all(): code={code}, name={T.codes_all[code]["name"]}, lateral_high={lateral_high:.2f}, closes[-1]={closes[-1]}')
		filtered_codes_all[code] = T.codes_all[code]

	T.codes_all = filtered_codes_all
	log(f'trade_refine_codes_all(): Filtered T.codes_all to {len(T.codes_all)} stocks that meet the condition.')


def handlebar(contextInfo):
	if T.download_mode:
		return
	# bar_time= timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M%S')
	# log(f"handlebar(): bar_time={timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y-%m-%d %H:%M:%S')}")
	# Validate period
	if contextInfo.period != 'tick' and False:
		log(f'handlebar(): Error! contextInfo.period != "tick"! contextInfo.period={contextInfo.period}')
		return
	# Filter by target date
	if T.TARGET_DATE != '' and T.TARGET_DATE != timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d'):
		return	
	# Skip history bars ####################################
	if not contextInfo.is_last_bar() and T.TARGET_DATE == '':
		# log(f'handlebar(): contextInfo.is_last_bar()={contextInfo.is_last_bar()}')
		return
	trade_on_handle_bar(contextInfo)

def trade_get_previous_trade_date(contextInfo):
	today = date.today()
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
	if trading_dates[1] == T.CURRENT_DATE:
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
	bar_date = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d')
	if T.CURRENT_DATE != bar_date:
		log(f'trade_on_handle_bar(): Error! T.CURRENT_DATE != bar_date! {T.CURRENT_DATE}, {bar_date}')
		return
	# log(f'bar_date={bar_date}, T.CURRENT_DATE={T.CURRENT_DATE}')
	# 获取当前时间: current_time带有前导0
	current_time = timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%H:%M:%S')
	if not T.last_current_time or T.last_current_time.get('top') != current_time[:-3]:
		T.last_current_time['top'] = current_time[:-3]
		# log(f'\t{current_time}')
	# if T.MARKET_OPEN_TIME <= current_time <= T.TRANSACTION_CLOSE_TIME:
	if current_time < T.MARKET_OPEN_TIME:
		return
	# 判断买卖条件, 并保存指令状态
	for code in list(set(T.codes_all.keys())):
		# 获取当前的最新价格
		if T.TARGET_DATE != '':
			bar_time= timetag_to_datetime(contextInfo.get_bar_timetag(contextInfo.barpos), '%Y%m%d%H%M00')
			market_data_last_price = contextInfo.get_market_data_ex(['high'], [code], period='1m', start_time=bar_time, end_time=bar_time, count=-1, dividend_type='front', fill_data=False, subscribe=True)
			# log(f'bar_time={bar_time}, market_data_last_price=\n{market_data_last_price[code].tail(100)}')
			if market_data_last_price[code].empty:
				log(f'trade_on_handle_bar(): Error! 未获取到{code} {T.codes_all[code]["name"]} 的{bar_time}分钟线数据!')
				continue
			current = market_data_last_price[code]['high'][0]
		else:
			market_data_last_price = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', end_time=T.CURRENT_DATE, count=1, dividend_type='front', fill_data=False, subscribe=True)
			current = round(market_data_last_price[code]['lastPrice'][0], 2)
		if T.codes_all[code]['lateral_high'] is None:
			lateral_high_date = T.codes_all[code]['lateral_high_date']
			if lateral_high_date is None:
				log(f'trade_on_handle_bar(): Error! 未获取到{code} {T.codes_all[code]["name"]} 的lateral_high_date {lateral_high_date}!')
				continue
			# 使用 lateral_high_date 获取lateral_high价格
			market_data_lateral_high = contextInfo.get_market_data_ex(['high'], [code], period='1d', start_time=lateral_high_date, end_time=lateral_high_date, count=1, dividend_type='front', fill_data=False, subscribe=True)
			if market_data_lateral_high[code].empty:
				log(f'trade_on_handle_bar(): Error! 未获取到{code} {T.codes_all[code]["name"]} 的推荐日{lateral_high_date}收盘价数据!')
				continue
			lateral_high = market_data_lateral_high[code]['high'][0]
			T.codes_all[code]['lateral_high'] = lateral_high
		else:
			lateral_high = T.codes_all[code]['lateral_high']
		# 获取120日的成交额
		market_data_120 = contextInfo.get_market_data_ex(['amount', 'close', 'low', 'open', 'high'], [code], period='1d', end_time=T.CURRENT_DATE, count=120, dividend_type='front', fill_data=False, subscribe=True)
		# 转换成单位亿
		amounts = market_data_120[code]['amount'] / 100000000
		closes = market_data_120[code]['close']
		# 获取今日开盘价, 昨日收盘价和昨日最低价
		lows = market_data_120[code]['low']			
		opens = market_data_120[code]['open']
		highs = market_data_120[code]['high']
		avg_amount_120 = amounts.mean()
		# local_min是从T.codes_all[code]['last_buy_date']到当日lows值的最低值
		buy_date = T.codes_all[code].get('last_buy_date')
		if buy_date and buy_date in highs.index:
			idx = highs.index.get_loc(buy_date)
			local_max = max(highs[idx - 2: idx])
		else:
			local_max = max(highs[-3 : -1])
		if buy_date and buy_date in lows.index:
			idx = lows.index.get_loc(buy_date)
			local_min = min(lows[idx : idx + 1])
		else:
			local_min = 0
		# 计算成交量相对量比
		rolling_max = pd.Series(amounts).rolling(window=20).max().values
		rolling_min = pd.Series(amounts).rolling(window=20).min().values
		diff = rolling_max - rolling_min
		amount_ratios = np.where(diff == 0, 1, (amounts - rolling_min) / diff)
		amount_ratios = np.nan_to_num(amount_ratios, nan=0)
		# 计算120日的rates
		rates = closes.pct_change().dropna() * 100
		# log(f'len(closes)={len(closes)}, len(rates)={len(rates)}')
		# 计算5日均线的数值
		# closes_ma5 = closes.rolling(window=5).mean()
		# closes_ma5_derivative = closes_ma5.diff(1).dropna()
		# ma5_derivative_normalized = closes_ma5_derivative / closes_ma5.shift(1)
		# if len(ma5_derivative_normalized) == 0:
		# 	log(f'trade_on_handle_bar(): Error! {code} {T.codes_all[code]["name"]} 的len(ma5_derivative_normalized) == 0!')
		# 	continue
		macd, macdsignal, macdhist = ta.MACD(np.array(closes), fastperiod=12, slowperiod=26, signalperiod=9)
		if T.BUY_AMOUNT is None:
			cash = trade_get_cash(contextInfo)
			if cash is None: 
				log(f'trade_on_handle_bar(): Error! cash is None!')
				cash = 100000
			T.BUY_AMOUNT = cash / 10
			log(f'T.BUY_AMOUNT={T.BUY_AMOUNT:.2f}')
		# 每分钟打印一次数据值
		if not T.last_current_time or T.last_current_time.get(code) != current_time[:-3] and False:
			T.last_current_time[code] = current_time[:-3]
			log(f'{code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')

		# 买入: 低于0.86倍的local_max. 全新推荐股票, 或者上次已经全部卖出的股票. 'type'为空, 当日无其它操作.
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in [None, 'SELL_AT_LOCAL_MAX', 'SELL_AT_TIMEOUT'] and current <= 0.86 * local_max and macd[-1] > 0:
			T.codes_all[code]['type'] = 'BUY_AT_LOCAL_MIN'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_buy_stock_by_amount(contextInfo, code, T.BUY_AMOUNT, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		# 卖出：最高价大于1.16倍的local_min (从buy_date到当日)
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_LOCAL_MIN', 'BUY_AT_STEP_1', 'BUY_AT_STEP_2', 'BUY_AT_STEP_3'] and local_min != 0 and current >= 1.16 * local_min:
			T.codes_all[code]['type'] = 'SELL_AT_LOCAL_MAX'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		# 卖出: 持仓超过3天. 从T.codes_all[code]['last_buy_date']到T.CURRENT_DATE超过3个交易日
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_LOCAL_MIN', 'BUY_AT_STEP_1', 'BUY_AT_STEP_2', 'BUY_AT_STEP_3'] and T.codes_all[code]['last_buy_date'] is not None and len(contextInfo.get_trading_dates(code, T.codes_all[code]['last_buy_date'], T.CURRENT_DATE, -1, '1d')) > 4:
			T.codes_all[code]['type'] = 'SELL_AT_TIMEOUT'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		# 买入: 多次台阶买入, 价格每下降0.1倍local_max就买入1次, 最多3次. 台阶是0.79倍, 0.70倍, 0.61倍.
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_LOCAL_MIN'] and current < 0.79 * local_max:
			T.codes_all[code]['type'] = 'BUY_AT_STEP_1'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_buy_stock_by_amount(contextInfo, code, T.BUY_AMOUNT, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_STEP_1'] and current < 0.70 * local_max:
			T.codes_all[code]['type'] = 'BUY_AT_STEP_2'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_buy_stock_by_amount(contextInfo, code, T.BUY_AMOUNT, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_STEP_2'] and current < 0.61 * local_max:
			T.codes_all[code]['type'] = 'BUY_AT_STEP_3'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_buy_stock_by_amount(contextInfo, code, T.BUY_AMOUNT, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		# 卖出: 当日出现高于BUY_AT_STEP_x买入价的1.15倍时, 卖出此份股票. buy_price要从T.codes_all[code]['records']里枚举, 还包括当日买入的T.codes_all[code]['price']. 卖出时, 用SELL_AT_1.15_STEP_x标记对应step的x
		if (T.codes_all[code]['type'] in ['BUY_AT_LOCAL_MIN'] and current >= 1.15 * T.codes_all[code]['price']) or (T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_LOCAL_MIN'] and current >= 1.15 * T.codes_all[code]['last_price']):
			T.codes_all[code]['type'] = 'SELL_AT_STEP_0'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if (T.codes_all[code]['type'] in ['BUY_AT_STEP_1'] and current >= 1.15 * T.codes_all[code]['price']) or (T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_STEP_1'] and current >= 1.15 * T.codes_all[code]['last_price']):
			T.codes_all[code]['type'] = 'SELL_AT_STEP_1'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_STEP_2'] and current >= 1.15 * T.codes_all[code]['last_price']:
			T.codes_all[code]['type'] = 'SELL_AT_STEP_2'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if T.codes_all[code]['type'] in [None] and T.codes_all[code]['last_type'] in ['BUY_AT_STEP_3'] and current >= 1.15 * T.codes_all[code]['last_price']:
			T.codes_all[code]['type'] = 'SELL_AT_STEP_3'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if T.codes_all[code]['type'] in ['BUY_AT_STEP_2'] and current >= 1.15 * T.codes_all[code]['price']:
			T.codes_all[code]['type'] = 'SELL_AT_STEP_2'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
		if T.codes_all[code]['type'] in ['BUY_AT_STEP_3'] and current >= 1.15 * T.codes_all[code]['price']:
			T.codes_all[code]['type'] = 'SELL_AT_STEP_3'
			T.codes_all[code]['price'] = current
			log(f'{current_time} {T.codes_all[code]["type"]}: {code} {T.codes_all[code]["name"]}, current={current:.2f}, opens[-1]={opens[-1]:.2f}, lateral_high={lateral_high:.2f}, amounts[-1]={amounts[-1]:.1f}, avg_amount_120={avg_amount_120:.1f}, rates[-1]={rates[-1]:.2f}, rates[-2]={rates[-2]:.2f}, rates[-3]={rates[-3]:.2f}, amount_ratios[-1]={amount_ratios[-1]:.2f}, amount_ratios[-2]={amount_ratios[-2]:.2f}, amount_ratios[-3]={amount_ratios[-3]:.2f}, closes[-2]={closes[-2]:.2f}, closes[-3]={closes[-3]:.2f}, lows[-2]={lows[-2]:.2f}, lows[-3]={lows[-3]:.2f}, macd[-1]={macd[-1]:.2f}, local_max={local_max:.2f}, local_min={local_min:.2f}')
			trade_sell_stock(contextInfo, code, T.codes_all[code]['type'])
			db_insert_record(code, name=T.codes_all[code]['name'], date=T.CURRENT_DATE, type=T.codes_all[code]['type'], price=T.codes_all[code]['price'])
			continue
	# 打印变化的表格内容
	if T.last_codes_all is None or T.last_codes_all != T.codes_all:
		# df = pd.DataFrame.from_dict(T.codes_all, orient='index')
		log(f'T.codes_all=\n{T.codes_all}')
		T.last_codes_all = copy.deepcopy(T.codes_all)

def trade_query_info(contextInfo):
	N_days_ago = datetime.strptime(T.CURRENT_DATE, '%Y%m%d').date() - timedelta(days=7)
	orders = get_trade_detail_data(T.accountid, 'stock', 'order')
	log("trade_query_info(): 最近7天的委托记录:")
	for o in orders:
		order_date = datetime.strptime(o.m_strInsertDate, '%Y%m%d').date()
		if order_date >= N_days_ago:
			log(f'trade_query_info(): {o.m_strInstrumentID}.{o.m_strExchangeID} {o.m_strInstrumentName}, 买卖方向: {o.m_nOffsetFlag}',
			f'委托数量: {o.m_nVolumeTotalOriginal}, 成交均价: {o.m_dTradedPrice:.2f} 元, 成交数量: {o.m_nVolumeTraded}, 成交金额: {o.m_dTradeAmount:.2f} 元, 委托时间: {o.m_strInsertDate} T {o.m_strInsertTime}')

	deals = get_trade_detail_data(T.accountid, 'stock', 'deal')
	log("trade_query_info(): 最近7天的成交记录:")
	for dt in deals:
		deal_date = datetime.strptime(dt.m_strTradeDate, '%Y%m%d').date()
		if deal_date >= N_days_ago:
			log(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 买卖方向: {dt.m_nOffsetFlag}',
			f'成交价格: {dt.m_dPrice:.2f}, 成交数量: {dt.m_nVolume}, 成交金额: {dt.m_dTradeAmount:.2f} 元, 成交时间: {dt.m_strTradeDate} T {dt.m_strTradeTime}')

	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	log("trade_query_info(): 当前持仓状态:")
	for dt in positions:
		log(f'trade_query_info(): {dt.m_strInstrumentID}.{dt.m_strExchangeID} {dt.m_strInstrumentName}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')

	accounts = get_trade_detail_data(T.accountid, 'stock', 'account')
	log("trade_query_info(): 当前账户状态:")
	for dt in accounts:
		log(f'trade_query_info(): 总资产: {dt.m_dBalance:.2f}, 净资产: {dt.m_dAssureAsset:.2f}, 总市值: {dt.m_dInstrumentValue:.2f}',
		f'总负债: {dt.m_dTotalDebit:.2f}, 可用金额: {dt.m_dAvailable:.2f} 元, 盈亏: {dt.m_dPositionProfit:.2f}')

	return orders, deals, positions, accounts
	
def trade_sell_stock(contextInfo, code, comment):
	volume = 0
	positions = get_trade_detail_data(T.accountid, 'stock', 'position')
	for dt in positions:
		if f"{dt.m_strInstrumentID}.{dt.m_strExchangeID}" != code:
			continue
		log(f'trade_sell_stock():  {code} {T.codes_all[code]["name"]}, 持仓量: {dt.m_nVolume}, 可用数量: {dt.m_nCanUseVolume}',
		f'成本价: {dt.m_dOpenPrice:.2f}, 市值: {dt.m_dInstrumentValue:.2f}, 持仓成本: {dt.m_dPositionCost:.2f}, 盈亏: {dt.m_dPositionProfit:.2f}')
		volume = dt.m_nCanUseVolume  # 可卖数量
		break
	if volume == 0:
		log(f'trade_sell_stock(): {code} {T.codes_all[code]["name"]}, {comment}, Error! volume == 0! 没有可卖的持仓，跳过卖出操作')
		return
	# volume = 100  # 测试时先卖100股
	# 通过指定价格卖出
	market_data = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', count=1, dividend_type='front', fill_data=False, subscribe=True)
	if code not in market_data or market_data[code].empty:
		log(f'trade_sell_stock(): Error! 无法获取{code} {T.codes_all[code]["name"]}的最新股价!')
		return
	last_price = market_data[code]['lastPrice'][0]
	passorder(T.opType_sell, T.orderType_volume, T.accountid, code, T.prType_designated, last_price, volume, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_sell_stock(): {code} {T.codes_all[code]["name"]}, {comment}, 以 {last_price:.2f} 元卖出 {volume} 股')

def trade_buy_stock_at_up_stop_price_by_amount(contextInfo, code, buy_amount, comment):
	# log(f'trade_buy_stock_at_up_stop_price_by_amount(): {code} {T.codes_all[code]["name"]}, buy_amount={buy_amount:.2f}元')
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
	log(f'trade_buy_stock_at_up_stop_price_by_amount(): {code} {T.codes_all[code]["name"]} 以涨停价{up_stop_price:.2f}买入 {volume}手金额 {buy_amount:.2f} 元')

def trade_buy_stock_at_up_stop_price_by_volume(contextInfo, code, volume, comment):
	log(f'trade_buy_stock_at_up_stop_price_by_volume(): {code} {T.codes_all[code]["name"]}, volume={volume} 股')
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
	log(f'trade_buy_stock_at_up_stop_price_by_volume(): {code} {T.codes_all[code]["name"]} 以涨停价{up_stop_price:.2f}买入 {volume} 股，预计成交金额 {buy_amount:.2f} 元')

def trade_get_fee(contextInfo, buy_amount):
	# 计算交易费用
	commission = max(buy_amount * T.commission_rate, T.commission_minimum)
	transfer_fee = buy_amount * T.transfer_fee_rate
	return commission + transfer_fee

def trade_buy_stock_by_amount(contextInfo, code, buy_amount, comment):
	log(f'trade_buy_stock_by_amount(): {code} {T.codes_all[code]["name"]}, buy_amount={buy_amount:.2f}元')
	#获取当前最新股价, 计算是否能够买入大于100股股票
	# 获取当前最新股价
	market_data = contextInfo.get_market_data_ex(['lastPrice'], [code], period='tick', count=1, dividend_type='front', fill_data=False, subscribe=True)
	if code not in market_data or market_data[code].empty:
		log(f'trade_buy_stock_by_amount(): Error! 无法获取{code} {T.codes_all[code]["name"]}的最新股价!')
		return
	last_price = market_data[code]['lastPrice'][0]
	# log(f'trade_buy_stock_by_amount(): 当前最新股价: {last_price:.2f}')
	# 计算买入股数
	volume = int(buy_amount / last_price // 100) * 100
	if volume < 100:
		log(f'trade_buy_stock_by_amount(): Error! 买入股数不足! 计算得买入 {volume} 股，少于100股，跳过!')
		return
	# 计算买入金额
	actual_buy_amount = volume * last_price
	total_cost = actual_buy_amount + trade_get_fee(contextInfo, actual_buy_amount)
	# 检查总成本是否超过可用资金，如果不足则减少股数
	available_cash = trade_get_cash(contextInfo)
	while total_cost > available_cash and volume >= 200:
		volume -= 100
		actual_buy_amount = volume * last_price
		total_cost = actual_buy_amount + trade_get_fee(contextInfo, actual_buy_amount)
	if volume < 100 or total_cost > available_cash:
		log(f'trade_buy_stock_by_amount(): Error! 可用资金不足! 买入 {volume} 股需要总成本{total_cost:.2f} 元，超过可用资金 {available_cash:.2f} 元，跳过!')
		return

	# 使用passorder进行指定价格last_price买入，按股数
	passorder(T.opType_buy, T.orderType_volume, T.accountid, code, T.prType_designated, last_price, volume, T.strategyName, T.quickTrade, comment, contextInfo)
	log(f'trade_buy_stock_by_amount(): {code} {T.codes_all[code]["name"]} 指定价格{last_price:.2f} 买入 {volume} 股，预计成交金额 {actual_buy_amount:.2f} 元')

def trade_buy_stock_by_volume(contextInfo, code, volume, comment):
	log(f'trade_buy_stock_by_volume(): {code} {T.codes_all[code]["name"]}, volume={volume} 股')
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
		log(f'trade_buy_stock_by_volume(): Error! 无法获取{code} {T.codes_all[code]["name"]}的最新股价!')
		return
	last_price = market_data[code]['lastPrice'][0]
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
	log(f'trade_buy_stock_by_volume(): {code} {T.codes_all[code]["name"]} 市价买入 {volume} 股，预计成交金额 {trade_amount:.2f} 元')

def account_callback(contextInfo, accountInfo):
	# 输出资金账号状态
	if accountInfo.m_strStatus != '登录成功':
		log(f'account_callback(): Error! 账号状态异常! m_strStatus={accountInfo.m_strStatus}')

# 委托主推函数
def order_callback(contextInfo, orderInfo):
	code = f"{orderInfo.m_strInstrumentID}.{orderInfo.m_strExchangeID}"
	if code in T.codes_all:
		name = T.codes_all[code]["name"]
	else:
		name = orderInfo.m_strInstrumentName
		log(f'order_callback(): Warning! {code} {name} is not in T.codes_all! T.codes_all=\n{T.codes_all}')
	# log(f'order_callback(): {code} {name}, m_nOrderStatus={orderInfo.m_nOrderStatus}, m_dLimitPrice={orderInfo.m_dLimitPrice}, m_nOpType={orderInfo.m_nOpType}, m_nVolumeTotalOriginal={orderInfo.m_nVolumeTotalOriginal}, m_nVolumeTraded={orderInfo.m_nVolumeTraded}')
	# 检查委托状态并记录成交结果
	if orderInfo.m_nOrderStatus == 56:  # 已成
		log(f'order_callback(): 委托已全部成交 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 成交数量: {orderInfo.m_nVolumeTraded}, 成交均价: {orderInfo.m_dTradedPrice:.2f}, 成交金额: {orderInfo.m_dTradeAmount:.2f} 元, m_nDirection={orderInfo.m_nDirection}, m_strOptName={orderInfo.m_strOptName}')
		# if '买' in orderInfo.m_strOptName:
		# 	db_insert_record(code, price=orderInfo.m_dTradedPrice)
		# elif '卖' in orderInfo.m_strOptName:
		# 	db_insert_record(code, price=orderInfo.m_dTradedPrice)
	elif orderInfo.m_nOrderStatus == 55:  # 部成
		log(f'order_callback(): 委托部分成交 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 已成交数量: {orderInfo.m_nVolumeTraded}, 剩余数量: {orderInfo.m_nVolumeTotal}, 成交金额: {orderInfo.m_dTradeAmount:.2f} 元, m_nDirection={orderInfo.m_nDirection}, m_strOptName={orderInfo.m_strOptName}')
	elif orderInfo.m_nOrderStatus == 54:  # 已撤
		log(f'order_callback(): 委托已撤销 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, m_nDirection={orderInfo.m_nDirection}, m_strOptName={orderInfo.m_strOptName}')
	else:
		return
		# log(f'order_callback(): 委托状态更新 - 股票: {code} {name}, 委托ID: {orderInfo.m_strOrderSysID}, 状态: {orderInfo.m_nOrderStatus}')

# 成交主推函数
def deal_callback(contextInfo, dealInfo):
	code = f"{dealInfo.m_strInstrumentID}.{dealInfo.m_strExchangeID}"
	if code in T.codes_all:
		name = T.codes_all[code]["name"]
	else:
		name = dealInfo.m_strInstrumentName
		log(f'deal_callback(): Warning! {code} {name} is not in T.codes_all! T.codes_all=\n{T.codes_all}')
	# log(f'deal_callback(): {code} {name}, m_dPrice={dealInfo.m_dPrice}, m_dPrice={dealInfo.m_dPrice}, m_nVolume={dealInfo.m_nVolume}')
	# 检查成交结果并记录
	# log(f'deal_callback(): 成交确认 - 股票: {code} {name}, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f}, 买卖方向: {dealInfo.m_nDirection}')
	# 可以在这里添加更多逻辑，如更新全局变量、发送通知等
	# 例如，检查是否为买入或卖出，并更新持仓统计
	log(f'deal_callback(): {code} {name}, 成交ID: {dealInfo.m_strTradeID}, 成交价格: {dealInfo.m_dPrice:.2f}, 成交数量: {dealInfo.m_nVolume}, 成交金额: {dealInfo.m_dTradeAmount:.2f} 元, m_strOptName={dealInfo.m_strOptName}')

# 持仓主推函数
def position_callback(contextInfo, positionInfo):
	code = f"{positionInfo.m_strInstrumentID}.{positionInfo.m_strExchangeID}"
	if code in T.codes_all:
		name = T.codes_all[code]["name"]
	else:
		name = positionInfo.m_strInstrumentName
		log(f'position_callback(): Warning! {code} {name} is not in T.codes_all! T.codes_all=\n{T.codes_all}')
	# log(f'position_callback(): {code} {name}, m_nVolume={positionInfo.m_nVolume}, m_nFrozenVolume={positionInfo.m_nFrozenVolume}')
	# 检查持仓变化并记录
	log(f'position_callback(): 持仓更新 - 股票: {code} {name}, 总持仓量: {positionInfo.m_nVolume}, 可用数量: {positionInfo.m_nCanUseVolume}, 冻结数量: {positionInfo.m_nFrozenVolume}, 成本价: {positionInfo.m_dOpenPrice:.2f}, 持仓盈亏: {positionInfo.m_dPositionProfit:.2f}, m_nDirection={positionInfo.m_nDirection}')
	# 可以在这里添加逻辑，如检查持仓是否为0，触发卖出信号等
	# if positionInfo.m_nVolume == 0:
	# 	log(f'position_callback(): 持仓清空 - 股票: {code} {name}')
	
#下单出错回调函数
def orderError_callback(contextInfo, passOrderInfo, msg):
	code = f"{passOrderInfo.orderCode}"
	if code in T.codes_all:
		name = T.codes_all[code]["name"]
	else:
		name = "未知股票"
		log(f'orderError_callback(): Warning! {code} {name} is not in T.codes_all! T.codes_all=\n{T.codes_all}')
	log(f'\norderError_callback(): 下单错误 - 股票: {code} {name}, 错误信息: {msg}')
	# 可以在这里添加逻辑，如重试下单或发送警报

def get_stock_name(contextInfo, code):
	try:
		instrument = contextInfo.get_instrument_detail(code)
		return instrument.get('InstrumentName')
	except:
		return "get_stock_name(): Error! 未知"

def log(*args):
	message = ''.join(str(arg) for arg in args) + '\n'
	file_path = 'C:/a/trade/量化/中信证券/code/' + 'QMT ' + T.CURRENT_DATE + ' log.txt'
	with open(file_path, 'a', encoding='utf-8') as f:
		f.write(message)

# recommends表格存放推荐股票数据, 包括股票代码, 股票名字, 是否有效, 推荐日期, 历史高点日期. records表格存放操作记录, 包括自增ID, 股票代码, 股票名字, 交易日期, 交易类型, 交易价格, 股数, 利润,和备注. recommends表上的股票代码对应records表的多条记录, 通过股票代码关联在一起. 
def db_init():
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()

	cursor.execute('''
	CREATE TABLE IF NOT EXISTS recommends (
		code TEXT,
		name TEXT,
		is_valid TEXT,
		recommend_date TEXT,
		lateral_high_date TEXT,
		PRIMARY KEY (code, recommend_date)
	)
	''')

	cursor.execute('''
	CREATE TABLE IF NOT EXISTS records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		code TEXT,
		name TEXT,
		date TEXT,
		type TEXT,
		price REAL,
		shares REAL,
		profit REAL,
		comment TEXT,
		FOREIGN KEY (code) REFERENCES recommends(code)
	)
	''')
	conn.commit()
	conn.close()

def db_load_all():
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	df = pd.read_sql_query("""
SELECT r.code, r.name, r.is_valid, r.recommend_date, r.lateral_high_date, rec.date, rec.type, rec.price, rec.shares, rec.profit, rec.comment
FROM recommends r
LEFT JOIN records rec ON r.code = rec.code
""", conn)
	conn.close()
	return df

def db_insert_record(code, name, date=None, type=None, price=None, shares=None, profit=None, comment=None):
	# 参数校验
	if not code or not name or not date or not type or not price:
		log(f'db_insert_record(): 参数校验失败 - code={code}, name={name}, date={date}, type={type}, price={price}')
		return
	# 插入数据库
	conn = sqlite3.connect('C:/a/trade/量化/中信证券/code/qmt.db')
	cursor = conn.cursor()
	cursor.execute('INSERT INTO records (code, name, date, type, price, shares, profit, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
				   (code, name, date, type, round(price, 2), shares, profit, comment))
	conn.commit()
	conn.close()
	log(f'db_insert_record(): code={code}, name={name}, date={date}, type={type}, price={price:.2f}, shares={shares}, profit={profit}, comment={comment}')
	
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
		code = df['code'][0]
		name = df['name'][0]

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
