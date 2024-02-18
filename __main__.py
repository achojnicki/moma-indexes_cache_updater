from adisconfig import adisconfig
from log import Log

from json import loads
from redis import Redis
from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from pprint import pprint, pformat



class cache_updater:
    project_name = "moma-cache_updater"

    def __init__(self):
        self.active=True
        self.websocket_conn=None

        self.config = adisconfig('/opt/adistools/configs/moma-indexes_cache_updater.yaml')
        self.log = Log(
            parent=self,
            rabbitmq_host=self.config.rabbitmq.host,
            rabbitmq_port=self.config.rabbitmq.port,
            rabbitmq_user=self.config.rabbitmq.user,
            rabbitmq_passwd=self.config.rabbitmq.password,
            debug=self.config.log.debug,
        )

        self.rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                host=self.config.rabbitmq.host,
                port=self.config.rabbitmq.port,
                credentials=PlainCredentials(
                    self.config.rabbitmq.user,
                    self.config.rabbitmq.password
                )
            )
        )

        self.rabbitmq_channel = self.rabbitmq_conn.channel()

        self.rabbitmq_channel.basic_consume(
            queue='moma-indexes_fetched',
            auto_ack=True,
            on_message_callback=self.incoming_events
        )
    
        self.redis_conn=Redis(
            host=self.config.redis.host,
            port=self.config.redis.port
        )

    def start(self):
        self.rabbitmq_channel.start_consuming()

    def event(self, event):
        redis_conn=self.redis_conn
        if event['type'] == 'heartbeat':
            pass

        if event['type'] == 'candles_1m_updates':
            for change in event['changes']:
                redis_conn.hset(
                    f"moma:{event['symbol']}:{change[0]}",
                    mapping={
                        "timestamp": change[0], 
                        "open": change[1],
                        "high": change[2],
                        "low": change[3],
                        "close": change[4],
                        "volume": change[5]
                        }
                    )


    def incoming_events(self, channel, method, properties, body):
        print(body)
        self.event(loads(body.decode('utf-8')))




if __name__ == "__main__":
    worker = cache_updater()
    worker.start()
