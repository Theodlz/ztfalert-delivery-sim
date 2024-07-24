import io
import uuid
from ast import literal_eval

import fastavro
from confluent_kafka import Consumer, KafkaException, KafkaError


from ztfalertsim.log import log
from ztfalertsim.config import load_config

config = load_config(config_files=["config.yaml"])

class KafkaConsumer:
    def __init__(self, date, config):
        self.topic = f"ztf_{date}_programid1"
        self.config = config

    @staticmethod
    def read_schema_data(bytes_io):
        """Read data that already has an Avro schema.

        :param bytes_io: `_io.BytesIO` Data to be decoded.
        :return: `dict` Decoded data.
        """
        bytes_io.seek(0)
        message = fastavro.reader(bytes_io)
        return message
    
    @classmethod
    def decode_message(cls, msg):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """
        message = msg.value()
        decoded_msg = message

        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = cls.read_schema_data(bytes_io)
        except AssertionError:
            decoded_msg = None
        except IndexError:
            literal_msg = literal_eval(
                str(message, encoding="utf-8")
            )  # works to give bytes
            bytes_io = io.BytesIO(literal_msg)  # works to give <class '_io.BytesIO'>
            decoded_msg = cls.read_schema_data(bytes_io)  # yields reader
        except Exception:
            decoded_msg = message
        finally:
            return decoded_msg

    def consume(self):
        # load configuration

        # create consumer
        consumer = Consumer({
            'bootstrap.servers': self.config["kafka"]["bootstrap.servers"],
            'group.id': str(uuid.uuid1()), # generate a unique group id
            'auto.offset.reset': 'earliest'
        })

        # subscribe to topic
        consumer.subscribe([self.topic])

        # consume messages
        i = 0
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        log('%% %s [%d] reached end at offset %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    partition = msg.partition()
                    msg = self.decode_message(msg)
                    for record in msg:
                        log(f"{i}: objectId: {record['objectId']}, candid: {record['candid']}, jd: {record['candidate']['jd']}, magpsf: {record['candidate']['magpsf']}, filter: {record['candidate']['fid']} (partition {partition})")
                        i += 1
                        break
        except KeyboardInterrupt:
            pass
        finally:
            # close consumer
            consumer.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Stream ZTF alerts to Kafka")
    parser.add_argument(
        "--date",
        type=str,
        help="Date for which to fetch alerts in YYYYMMDD format",
        required=True,
    )
    args = parser.parse_args()
    
    date = args.date

    kafka_consumer = KafkaConsumer(date, config)
    kafka_consumer.consume()
