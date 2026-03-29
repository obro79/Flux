brew services start zookeeper && brew services start kafka && docker compose up kafka-ui -d


rest test:

curl http://localhost:8000/candles/BTC-USD/1m


websocket test:

websocat ws://localhost:8000/indicators/BTC-USD