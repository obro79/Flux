import psycopg2
from dotenv import load_dotenv
import os
from services.utils import retry_policy


load_dotenv()


class Database:
    def __init__(self) -> None:
        self.connection_string: str = os.getenv("DATABASE_URL", "")
        self.connection = self.connect()

    @retry_policy
    def connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(self.connection_string)

    def disconnect(self):
        self.connection.close()

    def insert_candle(self, candle):
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO candles_1m (product_id, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id, timestamp) DO NOTHING
            """,
            (
                candle["product_id"],
                candle["timestamp"],
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                candle["volume"],
            ),
        )
        self.connection.commit()
        cursor.close()

    def get_candles(self, product_id: str, limit: int = 100) -> list[dict]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT product_id, timestamp, open, high, low, close, volume
            FROM candles_1m
            WHERE product_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (product_id, limit),
        )
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows

    def rollup_candles(self):
        pass

    def delete_old_candles(self):
        pass


if __name__ == "__main__":
    db = Database()
    print("Connected to database successfully.")
    db.disconnect()
