class TickerConsumer:
    def __init__(self):
        pass

    def run(self):
        pass


if __name__ == "__main__":
    import asyncio

    ticket_consumer = TickerConsumer()

    asyncio.run(ticket_consumer.run())
