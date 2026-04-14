import psycopg2
from dotenv import load_dotenv
import os
from services.utils import retry_policy


load_dotenv()


class Database:
    def __init__(self) -> None:
        self.connection_string: str = os.getenv("DATABASE_URL", "")
        self.connection = self.connect()
        self.ensure_schema()

    @retry_policy
    def connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(self.connection_string)

    def disconnect(self):
        self.connection.close()

    def ensure_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS candles_1m (
                exchange TEXT NOT NULL DEFAULT 'coinbase',
                product_id TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (exchange, product_id, timestamp)
            );

            ALTER TABLE candles_1m
            ADD COLUMN IF NOT EXISTS exchange TEXT;

            UPDATE candles_1m
            SET exchange = 'coinbase'
            WHERE exchange IS NULL;

            ALTER TABLE candles_1m
            ALTER COLUMN exchange SET DEFAULT 'coinbase';

            ALTER TABLE candles_1m
            ALTER COLUMN exchange SET NOT NULL;

            ALTER TABLE candles_1m
            DROP CONSTRAINT IF EXISTS candles_1m_pkey;

            ALTER TABLE candles_1m
            ADD PRIMARY KEY (exchange, product_id, timestamp);
            """
        )
        self.connection.commit()
        cursor.close()

    def insert_candle(self, candle):
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO candles_1m (exchange, product_id, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange, product_id, timestamp) DO NOTHING
            """,
            (
                candle["exchange"],
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

    def get_candles(
        self,
        product_id: str,
        limit: int = 100,
        exchange: str = "coinbase",
    ) -> list[dict]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT exchange, product_id, timestamp, open, high, low, close, volume
            FROM candles_1m
            WHERE exchange = %s AND product_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (exchange, product_id, limit),
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
