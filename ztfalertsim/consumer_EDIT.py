import io
import uuid
import time
import numpy as np
import pathlib

from fastavro import reader


from ztfalertsim.log import log
from ztfalertsim.config import load_config
from ztfalertsim.ml import ACAI_H_AlertClassifier

config = load_config(config_files=["config.yaml"])

# TO ADD: import you classifier here



class FakeKafkaConsumer:
    def __init__(self, date, config):
        self.topic = f"ztf_{date}_programid1"
        self.config = config
        self.path = pathlib.Path("data") / date

        # grab the full list of alert files and create an iterator
        self.alert_files = sorted(list(self.path.glob("*.avro")))
        self.alert_files_iter = iter(self.alert_files)

    @classmethod
    def decode_message(cls, msg):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """
        message = msg
        decoded_msg = message

        try:
            bytes_io = io.BytesIO(message)
            bytes_io.seek(0)
            decoded_msg = reader(bytes_io)
        except Exception:
            decoded_msg = message
        finally:
            return decoded_msg
        
    def fetch_alert(self):
        try:
            alert_file = next(self.alert_files_iter)
        except StopIteration:
            return None
        with open(alert_file, "rb") as f:
            alert = f.read()
        return alert

    def consume(self, max_alerts=None):

        start = time.time()

        # consume messages
        count = 0
        try:
            while True:
                msg = self.fetch_alert()
                if msg is None:
                    break
                else:
                    msg = self.decode_message(msg)
                    count += 1
                    if not msg:
                        continue

                    # TO ADD: get any other alert fields that you will want in addition to objectId and drb as variables here
                    for record in msg:
                        objectId = record['objectId']
                        drb = record['candidate']['drb']

                        #TO ADD: put all of youre filter here. We give you an example of a filter on drb, but you may choose
                        # to change the way you filter out bogus alerts

                        if drb < 0.5:
                            continue
                       















                        break

                if max_alerts and count >= max_alerts:
                    print(f"Reached maximum number of alerts: {max_alerts}")
                    break
        except KeyboardInterrupt:
            pass

        print(f"Processed {count} alerts in {time.time() - start} seconds")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Stream ZTF alerts to Kafka")
    parser.add_argument(
        "--date",
        type=str,
        help="Date for which to fetch alerts in YYYYMMDD format",
        required=True,
    )
    parser.add_argument(
        "--max-alerts",
        type=int,
        help="Maximum number of alerts to fetch",
        default=None,
    )
    args = parser.parse_args()
    
    date = args.date

    print(f"Creating Kafka consumer for date {date}")
    kafka_consumer = FakeKafkaConsumer(date, config)
    kafka_consumer.consume(args.max_alerts)