from queue import Queue
import threading
import sf_api
import json
import time
import os
import re
import numpy as np
import pandas as pd
import datetime as dt
import pickle
import csv

quote_queue = Queue(maxsize=0)
account_queue = Queue(maxsize=0)
acquired_account_queue = Queue(maxsize=0)


class SnoopingAccount(object):

    def __init__(self, api, venue, stock, account_name):
        self.account_name = account_name
        fill_columns = ['direction', 'filled', 'price', 'filledAt']
        self.account_fills = pd.DataFrame(columns=fill_columns)
        self.account_fill_status = 'running'
        self.api = api
        self.venue = venue
        self.stock = stock
        self.fill_queue = Queue(maxsize=0)
        self.collect_account_fills()
        self.start_write_to_file()

    def collect_account_fills(self):

        self.account_executions_thread = threading.Thread(target=self.account_executions, args=())
        self.account_executions_thread.setDaemon(True)
        self.account_executions_thread.start()

    def start_write_to_file(self):

        self.write_to_file_thread = threading.Thread(target=self.write_to_file, args=())
        self.write_to_file_thread.setDaemon(True)
        self.write_to_file_thread.start()


    def account_executions(self):

        #launch stock ticker with custom callback
        self.executions_socket = self.api.stock_execution_socket(self.venue, self.stock, self.account_name, callback=self.received_account_fill)

        while True and (self.account_fill_status == 'running'):
            if self.executions_socket.socket.status == 'closed':
                #self.executions_socket.close()
                print('*********')
                print('re-starting the account executions socket')
                print('*********')
                self.executions_socket = self.api.stock_execution_socket(self.venue, self.stock, self.account_name, callback=self.received_account_fill)

    def received_account_fill(self, m):
        try:
            if m.is_text:
                msg = json.loads(m.data.decode('utf-8'))
                self.fill_queue.put(msg)

        except ValueError:
            pass

    def write_to_file(self):

        while True:
            msg = self.fill_queue.get()
            ord_ts = msg['order']['ts']
            ord_ts = dt.datetime.strptime(ord_ts[0:-4], '%Y-%m-%dT%H:%M:%S.%f')
            fill_ts = msg['filledAt']
            fill_ts = dt.datetime.strptime(fill_ts[0:-4], '%Y-%m-%dT%H:%M:%S.%f')

            fill_str = str(fill_ts), str(ord_ts), msg['account'], msg['venue'], msg['symbol'], msg['order']['direction'], \
                       str(msg['order']['originalQty']), msg['order']['orderType'], \
                       str(msg['price']), str(msg['filled'])

            fill_str = ",".join(fill_str)
            #f = open('C:/accounts/' + str(self.account_name) + '.txt', 'a')
            f = open(os.path.expanduser("~/stockfighter/accounts/" + self.account_name + ".txt"), 'a')
            f.write(fill_str+'\n')
            f.close()


    def return_account_fills(self):

        return self.account_fills


def account_check_distribute(api, venue, stock, instance):
    accounts = []
    dict_of_account_threads = {}
    trading_day = 0

    while trading_day < 200:


        tmp_acc = acquired_account_queue.get()
        print('this is the next account: ', tmp_acc)
        if tmp_acc in accounts:
             pass
        else:
            accounts.append(tmp_acc)
            account_queue.put(tmp_acc)
            dict_of_account_threads[tmp_acc] = threading.Thread(target=SnoopingAccount, args=(api, venue, stock, tmp_acc,))
            dict_of_account_threads[tmp_acc].setDaemon(True)
            dict_of_account_threads[tmp_acc].start()
            gm_msg = api.gm_status(instance)
            if 'details' in gm_msg:
                trading_day = gm_msg['details']['tradingDay']
            else:
                trading_day = 0


def thread_account_aqcuire(api, venue, stock, instance):
    lowest_order_id = 1
    highest_order_id = 200

    def account_extract(ord_id, venue, stock):
        cxl_msg = api.stock_order_cancel(venue, stock, ord_id)

        if 'error' in cxl_msg:
            tmp_acc = cxl_msg['error'].split()[-1][0:-1]
            acquired_account_queue.put(tmp_acc)
        else:
            pass

    trading_day = 0

    while trading_day < 200:
        gm_msg = api.gm_status(instance)
        if 'details' in gm_msg:
            trading_day = gm_msg['details']['tradingDay']
        else:
            trading_day = 0

        if trading_day == 0:
            pass
        else:
            gm_msg = api.gm_status(instance)

            if 'details' in gm_msg:
                trading_day = gm_msg['details']['tradingDay']
            else:
                trading_day = 0

            for check in range(lowest_order_id, highest_order_id):
                print('starting thread: ', check)
                t = threading.Thread(target=account_extract, args=(check, venue, stock, ))
                t.daemon = True
                t.start()


            lowest_order_id = highest_order_id
            highest_order_id += 50
            time.sleep(2)


def analysis(api, venue, stock, account_id, instance):

    trading_day = 0

    while trading_day < 200:
        gm_msg = api.gm_status(instance)

        if 'details' in gm_msg:
            trading_day = gm_msg['details']['tradingDay']
            print('##########################################', str(trading_day), '#############################')
        else:
            trading_day = 0

        time.sleep(6)

    time.sleep(5)

    order_book = read_order_book('c:/order_book/orderBook.csv')

    summ_table = []

    while not account_queue.empty():
        acc_name = account_queue.get()
        acc_to_scrutinise = read_account_fills('c:/accounts/' + acc_name + '.txt')
        acc_to_scrutinise = prepare_account_cols(acc_to_scrutinise)
        acc_to_scrutinise, acc_summary = analyse_account(acc_to_scrutinise, order_book)
        tmp_list = [acc_name] + acc_summary
        summ_table.append(tmp_list)

    df = pd.DataFrame(res, columns = ['account_num', 'pnl', 'num_trades', 'one_day', 'three_day'])

    rogue_acc = get_rogue_acc(df)

    gm_judge(instance, rogue_acc, link, summary)

    return 1

def get_rogue_acc(df):

    sorted_df = df.sort('one_day', ascending=False).reset_index()

    for account in range(len(sorted_df)):
        if sorted_df['one_day'][account] < 1:
            if sorted_df['num_trades'][account] > 10:
                if sorted_df['pnl'][account] > 0:
                    rogue = sorted_df['account_num'][account]
                    break

    return rogue

def read_account_fills(filepath):
    names = ['ordTime', 'tbl_account', 'tbl_venue', 'tbl_stock', 'side', 'origQty', 'ordType', 'ordFillPrice', 'ordFillQty']
    account_fills = pd.read_table(filepath, sep=',', index_col=0, parse_dates=True, names=names)
    #hack to drop duplicate timestamp fills
    account_fills = account_fills.reset_index().drop_duplicates(subset='index', keep='first').set_index('index')
    account_fills = account_fills.sort_index()

    return account_fills

def read_order_book(filepath):
    order_book = pd.read_table(filepath, sep=',', index_col=0, parse_dates=True)
    order_book = order_book.sort_index()

    return order_book

def prepare_account_cols(account_fills):

    def direction(row):
        if row['side'] == 'sell':
            val = -1
        else:
            val = 1

        return val

    account_fills['direction'] = account_fills.apply(direction, axis=1)
    account_fills['shares_dir'] = account_fills['direction'] * account_fills['ordFillQty']
    account_fills['inventory'] = account_fills['shares_dir'].cumsum()
    account_fills['cash_dir'] = -account_fills['shares_dir'] * 0.01 * account_fills['ordFillPrice']
    account_fills['cash'] = account_fills['cash_dir'].cumsum()
    account_fills['nav'] = account_fills['cash'] + (account_fills['inventory']*0.01*account_fills['ordFillPrice'])

    return account_fills

def analyse_account(account_fills, order_book):

    one_day_onside = []
    three_day_onside = []

    for tick in account_fills.index:
        print(tick)
        tick_offset = tick + dt.timedelta(seconds=6)
        ob_equivalent_tick = order_book.index.asof(tick_offset)

        if (order_book.bid[ob_equivalent_tick] == 0.) or (order_book.ask[ob_equivalent_tick] == 0.):
            offset_mid = order_book.lastPrice[ob_equivalent_tick]
        else:
            offset_mid = (order_book.bid[ob_equivalent_tick] + order_book.ask[ob_equivalent_tick]) / 2.

        if account_fills.side[tick] == 'buy':
            if offset_mid > account_fills.ordFillPrice[tick]:
                one_day_onside.append(1)
            else:
                one_day_onside.append(0)

        if account_fills.side[tick] == 'sell':
            if offset_mid < account_fills.ordFillPrice[tick]:
                one_day_onside.append(1)
            else:
                one_day_onside.append(0)

    account_fills['onside_1d'] = one_day_onside


    for tick in account_fills.index:
        print(tick)
        tick_offset = tick + dt.timedelta(seconds=16)
        ob_equivalent_tick = order_book.index.asof(tick_offset)

        if (order_book.bid[ob_equivalent_tick] == 0.) or (order_book.ask[ob_equivalent_tick] == 0.):
            offset_mid = order_book.lastPrice[ob_equivalent_tick]
        else:
            offset_mid = (order_book.bid[ob_equivalent_tick] + order_book.ask[ob_equivalent_tick]) / 2.

        if account_fills.side[tick] == 'buy':
            if offset_mid > account_fills.ordFillPrice[tick]:
                three_day_onside.append(1)
            else:
                three_day_onside.append(0)

        if account_fills.side[tick] == 'sell':
            if offset_mid < account_fills.ordFillPrice[tick]:
                three_day_onside.append(1)
            else:
                three_day_onside.append(0)

    account_fills['onside_3d'] = three_day_onside

    pnl = account_fills.nav[-1]
    num_trades = len(account_fills.index)
    pc_onside_1d = sum(account_fills['onside_1d']) / num_trades
    pc_onside_3d = sum(account_fills['onside_3d']) / num_trades
    acc_summary = [pnl, num_trades, pc_onside_1d, pc_onside_3d]

    return account_fills, acc_summary


def main():

    api_key = '774f753e8b0c99df206f6017ba821beb2744b994'
    #create API object
    api = sf_api.StockFighterApi(api_key)

    #details of account, venue, stock
    #pkl_file = open('c:/gm_details/gm_details.pkl', 'rb')
    pkl_file = open(os.path.expanduser("~/stockfighter/gm_details/gm_details.pkl"), 'rb')
    gm_details = pickle.load(pkl_file)
    pkl_file.close()

    #get GM details
    account_id = gm_details['account']
    venue = gm_details['venues'][0]
    stock = gm_details['tickers'][0]
    instance = gm_details['instanceId']
    print(str(instance))

    analysis_thread = threading.Thread(target=analysis, args=(api, venue, stock, account_id, instance, ))
    analysis_thread.setDaemon(True)
    analysis_thread.start()

    #set up threads for various processes that need to work in parallel
    acc_acquire_thread = threading.Thread(target=thread_account_aqcuire, args=(api, venue, stock, instance))
    acc_acquire_thread.setDaemon(True)
    acc_acquire_thread.start()

    distribute_accounts_thread = threading.Thread(target=account_check_distribute, args=(api, venue, stock, instance,))
    distribute_accounts_thread.setDaemon(True)
    distribute_accounts_thread.start()


if __name__ == '__main__':
    main()
