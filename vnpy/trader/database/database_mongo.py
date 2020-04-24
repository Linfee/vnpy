import atexit
from datetime import datetime, date
from typing import Optional, Sequence, List, Dict

from pymongo import MongoClient
from pymongo.database import Database

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from .database import BaseDatabaseManager, Driver


def init(_: Driver, settings: dict):
    database = settings.get("database")
    config = {
        'host': settings.get('host'),
        'port': settings.get('port'),
        'username': settings.get('user'),
        'password': settings.get('password'),
        'authSource': settings.get('authentication_source'),
    }
    if not config.get('username'):
        del config['username']
        del config['password']
        del config['authSource']

    mongo = MongoClient(**{k: v for k, v in config.items() if v is not None})

    return MongoManager(mongo, database)


def from_bar(ex: str, s: str, data: BarData) -> Dict:
    return {
        'datetime': data.datetime,
        'open': data.open_price,
        'close': data.close_price,
        'high': data.high_price,
        'low': data.low_price,
        'volume': data.volume,
        'open_interest': data.open_interest,
        'period': data.interval.value,
        'symbol': s,
        'ex': ex
    }


def to_bar(ex: str, s: str, o: Dict) -> BarData:
    return BarData(
        symbol=s,
        exchange=Exchange(ex.upper()),
        datetime=o['datetime'],
        interval=Interval(o['period']),
        volume=o['volume'],
        open_interest=o['open_interest'],
        open_price=o['open'],
        high_price=o['high'],
        low_price=o['low'],
        close_price=o['close'],
        gateway_name="DB",
    )


def from_tick(ex: str, s: str, t: TickData) -> Dict:
    res = {
        'symbol': s,
        'ex': ex,
        'datetime': t.datetime,
        'name': t.name,
        'volume': t.volume,
        'open_interest': t.open_interest,
        'last_price': t.last_price,
        'last_volume': t.last_volume,
        'limit_up': t.limit_up,
        'limit_down': t.limit_down,
        'open_price': t.open_price,
        'high_price': t.high_price,
        'low_price': t.low_price,
        'pre_close': t.pre_close,

        'bid_price_1': t.bid_price_1,
        'ask_price_1': t.ask_price_1,
        'bid_volume_1': t.bid_volume_1,
        'ask_volume_1': t.ask_volume_1,
    }

    if t.bid_price_2:
        res['bid_price_2'] = t.bid_price_2,
        res['bid_price_3'] = t.bid_price_3
        res['bid_price_4'] = t.bid_price_4
        res['bid_price_5'] = t.bid_price_5

        res['ask_price_2'] = t.ask_price_2
        res['ask_price_3'] = t.ask_price_3
        res['ask_price_4'] = t.ask_price_4
        res['ask_price_5'] = t.ask_price_5

        res['bid_volume_2'] = t.bid_volume_2
        res['bid_volume_3'] = t.bid_volume_3
        res['bid_volume_4'] = t.bid_volume_4
        res['bid_volume_5'] = t.bid_volume_5

        res['ask_volume_2'] = t.ask_volume_2
        res['ask_volume_3'] = t.ask_volume_3
        res['ask_volume_4'] = t.ask_volume_4
        res['ask_volume_5'] = t.ask_volume_5

    return res


def to_tick(ex: str, s: str, o: Dict) -> TickData:
    res = TickData(
        symbol=s,
        exchange=Exchange(ex),
        datetime=o.get('datetime'),
        name=o.get('name'),
        volume=o.get('volume'),
        open_interest=o.get('open_interest'),
        last_price=o.get('last_price'),
        last_volume=o.get('last_volume'),
        limit_up=o.get('limit_up'),
        limit_down=o.get('limit_down'),
        open_price=o.get('open_price'),
        high_price=o.get('high_price'),
        low_price=o.get('low_price'),
        pre_close=o.get('pre_close'),
        bid_price_1=o.get('bid_price_1'),
        ask_price_1=o.get('ask_price_1'),
        bid_volume_1=o.get('bid_volume_1'),
        ask_volume_1=o.get('ask_volume_1'),
        gateway_name="DB",
    )

    if o.get('bid_price_2') is not None:
        res.bid_price_2 = o.get('bid_price_2')
        res.bid_price_3 = o.get('bid_price_3')
        res.bid_price_4 = o.get('bid_price_4')
        res.bid_price_5 = o.get('bid_price_5')

        res.ask_price_2 = o.get('ask_price_2')
        res.ask_price_3 = o.get('ask_price_3')
        res.ask_price_4 = o.get('ask_price_4')
        res.ask_price_5 = o.get('ask_price_5')

        res.bid_volume_2 = o.get('bid_volume_2')
        res.bid_volume_3 = o.get('bid_volume_3')
        res.bid_volume_4 = o.get('bid_volume_4')
        res.bid_volume_5 = o.get('bid_volume_5')

        res.ask_volume_2 = o.get('ask_volume_2')
        res.ask_volume_3 = o.get('ask_volume_3')
        res.ask_volume_4 = o.get('ask_volume_4')
        res.ask_volume_5 = o.get('ask_volume_5')
    return res


class MongoManager(BaseDatabaseManager):

    def __init__(self, client: MongoClient, database):
        self.mongo = client
        self.db: Database = self.mongo[database]

        def on_exit():
            if self.mongo is not None:
                self.mongo.close()

        atexit.register(on_exit)

    def load_bar_data(self, symbol: str,
                      exchange: Exchange,
                      interval: Interval,
                      start: datetime,
                      end: datetime, ) -> Sequence[BarData]:
        if isinstance(start, date):
            start = datetime(start.year, start.month, start.day)
        if isinstance(end, date):
            end = datetime(end.year, end.month, end.day)
        ex, s = exchange.value.lower(), symbol.lower()
        col = self.db[f'kline:{ex}:{s}']
        res = col.find({'period': interval.value, 'datetime': {'$gte': start, '$lte': end}}).sort('datetime', 1)
        return [to_bar(ex, s, i) for i in res]

    def load_tick_data(self, symbol: str, exchange: Exchange, start: datetime, end: datetime) -> Sequence[TickData]:
        if isinstance(start, date):
            start = datetime(start.year, start.month, start.day)
        if isinstance(end, date):
            end = datetime(end.year, end.month, end.day)
        ex, s = exchange.value.lower(), symbol.lower()
        col = self.db[f'tick:{ex}:{s}']
        res = col.find({'datetime': {'$gte': start, '$let': end}}).sort('datetime', 1)
        return [to_tick(ex, s, i) for i in res]

    def save_bar_data(self, data_list: Sequence[BarData]):
        if len(data_list) == 0:
            return
        min_time = min(data_list, key=lambda x: x.datetime).datetime
        max_time = max(data_list, key=lambda x: x.datetime).datetime
        ex, s, p = data_list[0].exchange.lower(), data_list[0].symbol.lower(), data_list[0].interval.value
        col = self.db[f'kline:{ex}:{s}']
        col.delete_many({'period': p, 'datetime': {'$gt': min_time, '$lt': max_time}})
        ds = (from_bar(ex, s, i) for i in data_list)
        col.insert_many(ds)

    def save_tick_data(self, data_list: Sequence[TickData]):
        if len(data_list) == 0:
            return
        min_time = min(data_list, key=lambda x: x.datetime).datetime
        max_time = max(data_list, key=lambda x: x.datetime).datetime
        ex, s = data_list[0].exchange.lower(), data_list[0].symbol.lower()
        col = self.db[f'tick:{ex}:{s}']
        col.delete_many({'datetime': {'$gt': min_time, '$lt': max_time}})
        ds = (from_tick(ex, s, i) for i in data_list)
        col.insert_many(ds)

    def get_newest_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> Optional["BarData"]:
        ex, s = exchange.value.lower(), symbol.lower()
        col = self.db[f'kline:{ex}:{s}']
        res = list(col.find({'period': interval.value}).sort('datetime', -1).limit(1))
        return to_bar(ex, s, res[0]) if res else None

    def get_oldest_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> Optional["BarData"]:
        ex, s = exchange.value.lower(), symbol.lower()
        col = self.db[f'kline:{ex}:{s}']
        res = list(col.find({'period': interval.value}).sort('datetime', 1).limit(1))
        return to_bar(ex, s, res[0]) if res else None

    def get_newest_tick_data(self, symbol: str, exchange: "Exchange") -> Optional["TickData"]:
        ex, s = exchange.value.lower(), symbol.lower()
        col = self.db[f'tick:{ex}:{s}']
        res = list(col.find({}).sort('datetime', -1).limit(1))
        return to_tick(ex, s, res[0]) if res else None

    def get_bar_data_statistics(self) -> List:
        res = []
        for col_name in (i for i in self.db.list_collection_names() if i.startswith('kline:')):
            ex, s = col_name.split(':')[1:]
            for p in self.db[col_name].distinct('period'):
                count = self.db['col_name'].count_documents({'period': p})
                res.append({'symbol': s, 'exchange': ex.upper(), 'interval': p, 'count': count})
        return res

    def delete_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> int:
        """
        Delete all bar data with given symbol + exchange + interval.
        """

        ex = exchange.value.lower()
        s = symbol.lower()
        col = self.db[f'kline:{ex}:{s}']
        count = col.estimated_document_count()
        col.drop()
        return count

    def clean(self, symbol: str):
        for col_name in (i for i in self.db.list_collection_names() if i.startswith('kline:') or i.startswith('tick:')):
            ex, s = col_name.split(':')[1:]
            if symbol.lower() == s:
                self.db[col_name].drop()
