from queue import Queue
from threading import Thread
import sf_api
import json
import time
import os
import re
import numpy as np
import pandas as pd
from datetime import datetime
import pickle


class OrderBook(object):

    def __init__(self, api, venue, stock, account_id, instance):
        self.ob_columns = ['bidDepth', 'bidSize', 'bid', 'ask', 'askSize', 'askDepth', 'lastPrice', 'lastSize', 'lastTime']
        self.msg_queue = Queue(maxsize=0)
        #self.order_book = pd.DataFrame(columns=ob_columns)
        self.order_book_status = 'running'
        self.qts = []
        self.bidDepth = []
        self.bidSize = []
        self.bid = []
        self.ask = []
        self.askSize = []
        self.askDepth = []
        self.lastPrice = []
        self.lastSize = []
        self.lastTime = []
        self.api = api
        self.venue = venue
        self.stock = stock
        self.trading_day = 0
        self.account_id = account_id
        self.instance = instance
        self.start_days_monitor()
        self.start_order_book()
        self.start_ticker_collect()

    def start_days_monitor(self):

        self.days_monitor_thread = Thread(target=self.days_monitor, args=())
        self.days_monitor_thread.setDaemon(True)
        self.days_monitor_thread.start()

    def start_order_book(self):

        self.order_book_thread = Thread(target=self.order_book_build, args=())
        self.order_book_thread.setDaemon(True)
        self.order_book_thread.start()

    def start_ticker_collect(self):

        self.stk_ticker_thread = Thread(target=self.stk_ticker, args=())
        self.stk_ticker_thread.setDaemon(True)
        self.stk_ticker_thread.start()

    def start_send_orders(self):

        self.send_orders_thread = Thread(target=self.send_orders, args=())
        self.send_orders_thread.setDaemon(True)
        self.send_orders_thread.start()

    def days_monitor(self):

        while self.trading_day < 200:
            gm_msg = self.api.gm_status(self.instance)

            if 'details' in gm_msg:
                self.trading_day = gm_msg['details']['tradingDay']
                print('##########################################', str(self.trading_day), '#############################')
            else:
                self.trading_day = 0

            time.sleep(6)

        time.sleep(10)

        ob_to_save = self.return_order_book()
        #ob_to_save.to_csv('c:/order_book/orderBook.csv')
        ob_to_save.to_csv(os.path.expanduser("~/stockfighter/order_book/orderBook.csv"))


    def order_book_build(self):

        while self.trading_day < 200:
            if self.msg_queue.empty():
                pass
            else:
                new_tick = self.msg_queue.get()
                qts = new_tick['quote']['quoteTime']
                qts = datetime.strptime(qts[0:-4], '%Y-%m-%dT%H:%M:%S.%f')
                if 'bid' in new_tick['quote']:
                    pass
                else:
                    new_tick['quote']['bid'] = 0
                if 'ask' in new_tick['quote']:
                    pass
                else:
                    new_tick['quote']['ask'] = 0

                if 'last' in new_tick['quote']:
                    pass
                else:
                    new_tick['quote']['last'] = 0

                if 'lastSize' in new_tick['quote']:
                    pass
                else:
                    new_tick['quote']['lastSize'] = 0

                if 'lastTrade' in new_tick['quote']:
                    pass
                else:
                    new_tick['quote']['lastTrade'] = 0

                self.qts.append(qts)
                self.bidDepth.append(int(new_tick['quote']['bidDepth']))
                self.bidSize.append(int(new_tick['quote']['bidSize']))
                self.bid.append(int(new_tick['quote']['bid']))
                self.ask.append(int(new_tick['quote']['ask']))
                self.askSize.append(int(new_tick['quote']['askSize']))
                self.askDepth.append(int(new_tick['quote']['askDepth']))
                self.lastPrice.append(int(new_tick['quote']['last']))
                self.lastSize.append(int(new_tick['quote']['lastSize']))
                self.lastTime.append(new_tick['quote']['lastTrade'])

                print('******************* order book length: ', len(self.qts))

    def stk_ticker(self):
        print('beginning ticker')
        #launch stock ticker with custom callback
        self.stk_tk_socket = self.api.stock_ticker_socket(self.venue, self.stock, self.account_id, callback=self.received_message)

        while True and (self.order_book_status == 'running'):
            if self.stk_tk_socket.socket.status == 'closed':
                #self.stk_tk_socket.close()
                print('*********')
                print('re-starting the order book socket')
                print('*********')
                self.stk_tk_socket = self.api.stock_ticker_socket(self.venue, self.stock, self.account_id, callback=self.received_message)

    def received_message(self, m):
        try:
            if m.is_text:
                msg = json.loads(m.data.decode('utf-8'))
                self.msg_queue.put(msg)
                print('quote queue size is: ', self.msg_queue.qsize())

        except ValueError:
            pass


    def send_orders(self):
        last_order_time = time.time()

        while True:
            if time.time() > (last_order_time + 180):
                buy_msg = self.api.stock_order(self.venue, self.account_id, self.stock, 1, 1, 'buy', 'market')
                last_order_time = time.time()
            else:
                pass


    def return_order_book(self):
        # stop the order book socket
        self.order_book_status = 'self_closed'
        idx = self.qts
        data = [self.bidDepth, self.bidSize, self.bid, self.ask, self.askSize, self.askDepth, self.lastPrice, self.lastSize, self.lastTime]
        data = list(map(list, zip(*data)))
        self.order_book = pd.DataFrame(data=data, index=idx, columns=self.ob_columns)

        # return the order book
        return self.order_book


def main():

    api_key = '774f753e8b0c99df206f6017ba821beb2744b994'
    #create API object
    api = sf_api.StockFighterApi(api_key)

    #details of account, venue, stock
    gm_details = api.gm_start('making_amends')
    #output = open('c:/gm_details/gm_details.pkl', 'wb')
    output = open(os.path.expanduser("~/stockfighter/gm_details/gm_details.pkl"), 'wb')
    pickle.dump(gm_details, output)
    output.close

    #get GM details
    account_id = gm_details['account']
    venue = gm_details['venues'][0]
    stock = gm_details['tickers'][0]
    instance = gm_details['instanceId']
    print(str(instance))

    #create order book collection instance
    order_book_start = OrderBook(api, venue, stock, account_id, instance)

if __name__ == '__main__':
    main()
