import atexit
from datetime import datetime
from enum import Enum
from typing import Optional, Sequence, List

from pymongo import MongoClient
from pymongo.database import Database
from mongoengine import DateTimeField, Document, FloatField, StringField, connect

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from .database import BaseDatabaseManager, Driver

mongo: Optional[MongoClient] = None
db: Optional[Database] = None


def on_exit():
    if mongo is not None:
        mongo.close()


atexit.register(on_exit)


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

    global mongo
    mongo = MongoClient(**{k: v for k, v in config.items() if not v is None})
    global db
    db = mongo[database]

    return MongoManager()


class DbBarData(Document):
    """
    Candlestick bar data for database storage.

    Index is defined unique with datetime, interval, symbol
    """

    symbol: str = StringField()
    exchange: str = StringField()
    datetime: datetime = DateTimeField()
    interval: str = StringField()

    volume: float = FloatField()
    open_interest: float = FloatField()
    open_price: float = FloatField()
    high_price: float = FloatField()
    low_price: float = FloatField()
    close_price: float = FloatField()

    meta = {
        "indexes": [
            {
                "fields": ("symbol", "exchange", "interval", "datetime"),
                "unique": True,
            }
        ]
    }

    @staticmethod
    def from_bar(bar: BarData):
        """
        Generate DbBarData object from BarData.
        """
        db_bar = DbBarData()

        db_bar.symbol = bar.symbol
        db_bar.exchange = bar.exchange.value
        db_bar.datetime = bar.datetime
        db_bar.interval = bar.interval.value
        db_bar.volume = bar.volume
        db_bar.open_interest = bar.open_interest
        db_bar.open_price = bar.open_price
        db_bar.high_price = bar.high_price
        db_bar.low_price = bar.low_price
        db_bar.close_price = bar.close_price

        return db_bar

    def to_bar(self):
        """
        Generate BarData object from DbBarData.
        """
        bar = BarData(
            symbol=self.symbol,
            exchange=Exchange(self.exchange),
            datetime=self.datetime,
            interval=Interval(self.interval),
            volume=self.volume,
            open_interest=self.open_interest,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            gateway_name="DB",
        )
        return bar


class DbTickData(Document):
    """
    Tick data for database storage.

    Index is defined unique with (datetime, symbol)
    """

    symbol: str = StringField()
    exchange: str = StringField()
    datetime: datetime = DateTimeField()

    name: str = StringField()
    volume: float = FloatField()
    open_interest: float = FloatField()
    last_price: float = FloatField()
    last_volume: float = FloatField()
    limit_up: float = FloatField()
    limit_down: float = FloatField()

    open_price: float = FloatField()
    high_price: float = FloatField()
    low_price: float = FloatField()
    close_price: float = FloatField()
    pre_close: float = FloatField()

    bid_price_1: float = FloatField()
    bid_price_2: float = FloatField()
    bid_price_3: float = FloatField()
    bid_price_4: float = FloatField()
    bid_price_5: float = FloatField()

    ask_price_1: float = FloatField()
    ask_price_2: float = FloatField()
    ask_price_3: float = FloatField()
    ask_price_4: float = FloatField()
    ask_price_5: float = FloatField()

    bid_volume_1: float = FloatField()
    bid_volume_2: float = FloatField()
    bid_volume_3: float = FloatField()
    bid_volume_4: float = FloatField()
    bid_volume_5: float = FloatField()

    ask_volume_1: float = FloatField()
    ask_volume_2: float = FloatField()
    ask_volume_3: float = FloatField()
    ask_volume_4: float = FloatField()
    ask_volume_5: float = FloatField()

    meta = {
        "indexes": [
            {
                "fields": ("symbol", "exchange", "datetime"),
                "unique": True,
            }
        ],
    }

    @staticmethod
    def from_tick(tick: TickData):
        """
        Generate DbTickData object from TickData.
        """
        db_tick = DbTickData()

        db_tick.symbol = tick.symbol
        db_tick.exchange = tick.exchange.value
        db_tick.datetime = tick.datetime
        db_tick.name = tick.name
        db_tick.volume = tick.volume
        db_tick.open_interest = tick.open_interest
        db_tick.last_price = tick.last_price
        db_tick.last_volume = tick.last_volume
        db_tick.limit_up = tick.limit_up
        db_tick.limit_down = tick.limit_down
        db_tick.open_price = tick.open_price
        db_tick.high_price = tick.high_price
        db_tick.low_price = tick.low_price
        db_tick.pre_close = tick.pre_close

        db_tick.bid_price_1 = tick.bid_price_1
        db_tick.ask_price_1 = tick.ask_price_1
        db_tick.bid_volume_1 = tick.bid_volume_1
        db_tick.ask_volume_1 = tick.ask_volume_1

        if tick.bid_price_2:
            db_tick.bid_price_2 = tick.bid_price_2
            db_tick.bid_price_3 = tick.bid_price_3
            db_tick.bid_price_4 = tick.bid_price_4
            db_tick.bid_price_5 = tick.bid_price_5

            db_tick.ask_price_2 = tick.ask_price_2
            db_tick.ask_price_3 = tick.ask_price_3
            db_tick.ask_price_4 = tick.ask_price_4
            db_tick.ask_price_5 = tick.ask_price_5

            db_tick.bid_volume_2 = tick.bid_volume_2
            db_tick.bid_volume_3 = tick.bid_volume_3
            db_tick.bid_volume_4 = tick.bid_volume_4
            db_tick.bid_volume_5 = tick.bid_volume_5

            db_tick.ask_volume_2 = tick.ask_volume_2
            db_tick.ask_volume_3 = tick.ask_volume_3
            db_tick.ask_volume_4 = tick.ask_volume_4
            db_tick.ask_volume_5 = tick.ask_volume_5

        return db_tick

    def to_tick(self):
        """
        Generate TickData object from DbTickData.
        """
        tick = TickData(
            symbol=self.symbol,
            exchange=Exchange(self.exchange),
            datetime=self.datetime,
            name=self.name,
            volume=self.volume,
            open_interest=self.open_interest,
            last_price=self.last_price,
            last_volume=self.last_volume,
            limit_up=self.limit_up,
            limit_down=self.limit_down,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            pre_close=self.pre_close,
            bid_price_1=self.bid_price_1,
            ask_price_1=self.ask_price_1,
            bid_volume_1=self.bid_volume_1,
            ask_volume_1=self.ask_volume_1,
            gateway_name="DB",
        )

        if self.bid_price_2:
            tick.bid_price_2 = self.bid_price_2
            tick.bid_price_3 = self.bid_price_3
            tick.bid_price_4 = self.bid_price_4
            tick.bid_price_5 = self.bid_price_5

            tick.ask_price_2 = self.ask_price_2
            tick.ask_price_3 = self.ask_price_3
            tick.ask_price_4 = self.ask_price_4
            tick.ask_price_5 = self.ask_price_5

            tick.bid_volume_2 = self.bid_volume_2
            tick.bid_volume_3 = self.bid_volume_3
            tick.bid_volume_4 = self.bid_volume_4
            tick.bid_volume_5 = self.bid_volume_5

            tick.ask_volume_2 = self.ask_volume_2
            tick.ask_volume_3 = self.ask_volume_3
            tick.ask_volume_4 = self.ask_volume_4
            tick.ask_volume_5 = self.ask_volume_5

        return tick


def to_bar(ex: str, s: str, o: dict):
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


def from_bar(ex: str, s: str, data: BarData):
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


class MongoManager(BaseDatabaseManager):

    def load_bar_data(self, symbol: str,
                      exchange: Exchange,
                      interval: Interval,
                      start: datetime,
                      end: datetime, ) -> Sequence[BarData]:
        ex, s = exchange.value.lower(), symbol.lower()
        col = db[f'kline:{ex}:{s}']
        res = col.find({'period': interval.value, 'datetime': {'$gte': start, '$let': end}}).sort('datetime', 1)
        return [to_bar(ex, s, i) for i in res]

    def load_tick_data(self, symbol: str, exchange: Exchange, start: datetime, end: datetime) -> Sequence[TickData]:
        # todo:
        s = DbTickData.objects(
            symbol=symbol,
            exchange=exchange.value,
            datetime__gte=start,
            datetime__lte=end,
        )
        data = [db_tick.to_tick() for db_tick in s]
        return data

    @staticmethod
    def to_update_param(d):
        return {
            "set__" + k: v.value if isinstance(v, Enum) else v
            for k, v in d.__dict__.items()
        }

    def save_bar_data(self, data_list: Sequence[BarData]):
        if len(data_list) == 0:
            return
        min_time = min(data_list, key=lambda x: x.datetime).datetime
        max_time = max(data_list, key=lambda x: x.datetime).datetime
        ex, s, p = data_list[0].exchange.lower(), data_list[0].symbol.lower(), data_list[0].interval.value
        col = db[f'kline:{ex}:{s}']
        col.delete_many({'period': p, 'datetime': {'$gt': min_time, '$lt': max_time}})
        ds = (from_bar(ex, s, i) for i in data_list)
        col.insert_many(ds)

    def save_tick_data(self, datas: Sequence[TickData]):
        # todo:
        for d in datas:
            updates = self.to_update_param(d)
            updates.pop("set__gateway_name")
            updates.pop("set__vt_symbol")
            (
                DbTickData.objects(
                    symbol=d.symbol, exchange=d.exchange.value, datetime=d.datetime
                ).update_one(upsert=True, **updates)
            )

    def get_newest_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> Optional["BarData"]:
        ex, s = exchange.value.lower(), symbol.lower()
        col = db[f'kline:{ex}:{s}']
        res = col.find({'period': interval.value}).sort('datetime', -1).limit(1)
        if res:
            return to_bar(ex, s, res[0])
        else:
            return None

    def get_oldest_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> Optional["BarData"]:
        ex, s = exchange.value.lower(), symbol.lower()
        col = db[f'kline:{ex}:{s}']
        res = col.find({'period': interval.value}).sort('datetime', 1).limit(1)
        if res:
            return to_bar(ex, s, res[0])
        else:
            return None

    def get_newest_tick_data(self, symbol: str, exchange: "Exchange") -> Optional["TickData"]:
        # todo:
        s = (
            DbTickData.objects(symbol=symbol, exchange=exchange.value)
                .order_by("-datetime")
                .first()
        )
        if s:
            return s.to_tick()
        return None

    def get_bar_data_statistics(self) -> List:
        res = []
        for col_name in (i for i in db.list_collection_names() if i.startswith('kline:')):
            ex, s = col_name.split(':')[1:]
            for p in db[col_name].distinct('period'):
                count = db['col_name'].count_documents({'period': p})
                res.append({'symbol': s, 'exchange': ex.upper(), 'interval': p, 'count': count})
        return res

    def delete_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> int:
        """
        Delete all bar data with given symbol + exchange + interval.
        """

        ex = exchange.value.lower()
        s = symbol.lower()
        col = db[f'kline:{ex}:{s}']
        count = col.estimated_document_count()
        col.drop()
        return count

    def clean(self, symbol: str):
        for col_name in (i for i in db.list_collection_names() if i.startswith('kline:') or i.startswith('tick:')):
            ex, s = col_name.split(':')[1:]
            if symbol.lower() == s:
                db[col_name].drop()
