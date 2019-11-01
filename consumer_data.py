import logging
from pykafka import KafkaClient
from pykafka.simpleconsumer import OffsetType

logging.getLogger("pykafka.broker").setLevel('ERROR')

consumer_client = KafkaClient(hosts="localhost:9092")

cons_topic = consumer_client.topics['demo']

data_consumer = cons_topic.get_balanced_consumer(
    consumer_group = b'pytkafka-demo-2',
    auto_commit_enable = False,
    auto_offset_reset = OffsetType.EARLIEST,
    zookeeper_connect = 'localhost:2181'
)

for mes in data_consumer:
    if mes is not None:
        print("Message",mes.offset, mes.value)