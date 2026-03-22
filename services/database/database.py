import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()


class Database:
    def __init__(self) -> None:
        self.connection_string: str = os.getenv("DATABASE_URL", "")
        self.connection = self.connect()

    def connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(self.connection_string)

    def disconnect(self):
        self.connection.close()

    def insert_candle(self, candle):
        pass

    def get_candles(self, ticker, start_time, end_time):
        pass

    # TODO: Implement method to aggregate historical data for indicators
    def aggregate_historical_data(self):
        pass
