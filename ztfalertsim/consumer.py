import io
import uuid
import time
import numpy as np
from ast import literal_eval

from fastavro import reader
from confluent_kafka import Consumer, KafkaException, KafkaError


from ztfalertsim.log import log
from ztfalertsim.config import load_config
from ztfalertsim.ml import ACAI_H_AlertClassifier

config = load_config(config_files=["config.yaml"])

acai_h = ACAI_H_AlertClassifier("data/models/acai_h.d1_dnn_20201130.h5")

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
        return reader(bytes_io)
    
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

    def consume(self, max_alerts=None):
        # load configuration

        # create consumer
        consumer = Consumer({
            'bootstrap.servers': self.config["kafka"]["bootstrap.servers"],
            'group.id': str(uuid.uuid1()), # generate a unique group id
            'auto.offset.reset': 'earliest'
        })

        start = time.time()

        # subscribe to topic
        consumer.subscribe([self.topic])

        # consume messages
        count = 0
        try:
            while True:
                msg = consumer.poll(timeout=5.0)
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        log('%% %s [%d] reached end at offset %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    msg = self.decode_message(msg)
                    count += 1
                    if not msg:
                        continue
                    for record in msg:
                        objectId = record['objectId']
                        jd = record['candidate']['jd']
                        drb = record['candidate']['drb']
                        ssdistnr = record['candidate']['ssdistnr']
                        ssmagnr = record['candidate']['ssmagnr']
                        isdiffpos = record['candidate']['isdiffpos']
                        distnr = record['candidate']['distnr']
                        sgscore1 = record['candidate']['sgscore1']
                        age = record['candidate']['jd'] - record['candidate']['jdstarthist']
                        sigmapsf = record['candidate']['sigmapsf']
                        fid = record['candidate']['fid']

                        # filter out bogus alerts
                        if drb < 0.7:
                            continue

                        # we are looking for young objects, so filter on age
                        if age > 30.0:
                            continue

                        # filter out objects with large magnitude errors
                        if sigmapsf > 0.5:
                            continue

                        # remove known stars (not likely to be interesting, we are looking for new things in galaxies other than our own)
                        # 0 > sgcore1 > 1, 0 means galaxy, 1 means star
                        # if distnr < 5.0 and sgscore1 > 0.5:
                        #     continue

                        # filter out solar system objects
                        if (
                            ssdistnr >= 0.0
                            and ssdistnr < 10.0
                            and ssmagnr < 20.0
                            and ssmagnr > -20.0
                        ):
                            continue

                        # only look at positive subtractions (helps reject variable stars)
                        if isdiffpos != 't':
                            continue

                        acai_h_score = acai_h.predict(record)
                        if acai_h_score < 0.8:
                            continue

                        # get the detections from the `prv_candidates` field:
                        # - magpsf < 99
                        # - fid: same as the alert
                        # - isdiffpos: 't'
                        prv_candidates = record.get('prv_candidates', [])
                        if not prv_candidates:
                            print(f"WARNING: No prv_candidates for objectId: {objectId}")
                            continue
                        detections = [
                            prv for prv in prv_candidates
                            if (
                                prv['magpsf'] is not None
                                and prv['magpsf'] < 99
                                and prv['fid'] == fid
                                # and prv['isdiffpos'] == 't'
                            )
                        ]
                        # only keep detections that are more than 0.015 jd away from the alert
                        detections = [
                            prv for prv in detections
                            if jd - prv['jd'] > 0.015
                        ]
                        # keep only the last 14 days of data at most
                        detections = [
                            prv for prv in detections
                            if jd - prv['jd'] <= 14
                        ]
                        # append the alert itself
                        detections.append(record['candidate'])
                        # run a simple linear fit to get the slope of the light curve
                        if len(detections) < 2:
                            continue

                        # sort detections by jd ascending
                        detections = sorted(detections, key=lambda x: x['jd'])
                        first_jd = detections[0]['jd']
                        max_magpsf = max([d['magpsf'] for d in detections])
                        m, _ = np.polyfit(
                            [d['jd'] - first_jd for d in detections],
                            [max_magpsf - d['magpsf'] for d in detections],
                            1
                        )
                        if m > -0.15 and m < 0.15:
                            continue

                        print(f"Light curve slope: {m} for objectId: {objectId}") 
                        print(f"{count} | ALERT: objectId: {record['objectId']}, candid: {record['candid']}, score: {acai_h_score}")

                        break
                if max_alerts and count >= max_alerts:
                    print(f"Reached maximum number of alerts: {max_alerts}")
                    break
        except KeyboardInterrupt:
            pass
        finally:
            # close consumer
            consumer.close()

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
    kafka_consumer = KafkaConsumer(date, config)
    kafka_consumer.consume(args.max_alerts)
