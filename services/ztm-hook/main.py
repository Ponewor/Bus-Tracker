import requests
import json
import time
import logging
from kafka import KafkaProducer

token = 'd561bd20-5bcf-4ce6-be8a-abcbb700ffdd'
url = 'https://api.um.warszawa.pl/api/action/busestrams_get/'
resource_id = 'f2e5503e927d-4ad3-9500-4ab9e55deb59'

bus_params = {
    'apikey': token,
    'type': 1,
    'resource_id': resource_id
}
tram_params = {
    'apikey': token,
    'type': 2,
    'resource_id': resource_id
}

producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         key_serializer=lambda x: x
                         )


def send_data(data):
    for record in data['result']:
        future = producer.send('ztm-input', value=record, key=record["VehicleNumber"].encode('utf-8'))
        future.get(timeout=60)
    logging.info(f'{len(data["result"])} results sent')


if __name__ == '__main__':
    while True:
        for params in (bus_params, tram_params):
            try:
                r = requests.get(url=url, params=params)
                send_data(r.json())
            except:
                logging.error("Error")
        time.sleep(30)
