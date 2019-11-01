import pykafka
import time
import pathlib

import json
import logging


dataset = 'police-department-calls-for-service.json'

logger = logging.getLogger(__name__)


def read_file() -> json:
    with open(dataset, 'r') as f:
        data_file = json.load(f)
    return data_file


def generate_data() -> None:
    data = read_file()
    for i in data:
        message = dict_to_binary(i)
        producer.produce(message)
        time.sleep(2)


# TODO complete this function
def dict_to_binary(json_dict: dict) -> bytes:
    return json.dumps(json_dict).encode('utf-8')


# TODO set up kafka client
if __name__ == "__main__":
    Kclient = pykafka.KafkaClient("localhost:9092")
    print("topics", Kclient.topics)
    producer = Kclient.topics[b'demo'].get_producer()

    generate_data()
