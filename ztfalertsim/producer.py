import os
import pathlib
import subprocess
import time
from confluent_kafka import Producer

from ztfalertsim.log import log
from ztfalertsim.config import load_config
from ztfalertsim.fetch_data import fetch_alerts
from ztfalertsim.init_kafka import init_kafka

config = load_config(config_files=["config.yaml"])

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        log(f"Message delivery failed: {err}")
    else:
        log(f"Message delivered to {msg.topic()} - partition(s) [{msg.partition()}]")

class KafkaStream:
    def __init__(self, topic, path_alerts, config, **kwargs):
        self.config = config
        self.topic = topic
        self.path_alerts = path_alerts

        if kwargs.get("max_alerts") is not None:
            try:
                int(kwargs.get("max_alerts"))
            except ValueError:
                raise ValueError("max_alerts must be an integer")
            self.max_alerts = int(kwargs.get("max_alerts"))
        else:
            self.max_alerts = None

    def start(self):
        # create a kafka topic and start a producer to stream the alerts
        path_logs = pathlib.Path("logs/")
        if not path_logs.exists():
            path_logs.mkdir(parents=True, exist_ok=True)
        # clean up old Kafka logs
        log("Cleaning up Kafka logs")
        subprocess.run(["rm", "-rf", path_logs / "kafka-logs", "/tmp/zookeeper"])

        log("Starting up ZooKeeper at localhost:2181")

        # start ZooKeeper in the background
        cmd_zookeeper = [
            os.path.join(
                self.config["kafka"]["path"], "bin", "zookeeper-server-start.sh"
            ),
            "-daemon",
            os.path.join(
                self.config["kafka"]["path"], "config", "zookeeper.properties"
            ),
        ]

        with open(path_logs / "zookeeper.stdout", "w") as stdout_zookeeper:
            # p_zookeeper =
            subprocess.run(
                cmd_zookeeper, stdout=stdout_zookeeper, stderr=subprocess.STDOUT
            )

        # take a nap while it fires up
        time.sleep(3)

        log("Starting up Kafka Server at localhost:9092")

        # start the Kafka server:
        cmd_kafka_server = [
            os.path.join(self.config["kafka"]["path"], "bin", "kafka-server-start.sh"),
            "-daemon",
            os.path.join(self.config["kafka"]["path"], "config", "server.properties"),
        ]

        with open(os.path.join("logs/", "kafka_server.stdout"), "w"):
            subprocess.run(cmd_kafka_server)

        # take a nap while it fires up
        time.sleep(3)

        # get kafka topic names with kafka-topics command
        cmd_topics = [
            os.path.join(self.config["kafka"]["path"], "bin", "kafka-topics.sh"),
            "--bootstrap-server",
            self.config["kafka"]["bootstrap.servers"],
            "-list",
        ]

        topics = (
            subprocess.run(cmd_topics, stdout=subprocess.PIPE)
            .stdout.decode("utf-8")
            .split("\n")[:-1]
        )
        log(f"Found topics: {topics}")

        if self.topic in topics:
            # topic previously created? remove first
            cmd_remove_topic = [
                os.path.join(self.config["kafka"]["path"], "bin", "kafka-topics.sh"),
                "--bootstrap-server",
                self.config["kafka"]["bootstrap.servers"],
                "--delete",
                "--topic",
                self.topic,
            ]

            remove_topic = (
                subprocess.run(cmd_remove_topic, stdout=subprocess.PIPE)
                .stdout.decode("utf-8")
                .split("\n")[:-1]
            )
            log(f"{remove_topic}")
            log(f"Removed topic: {self.topic}")
            time.sleep(1)

        if self.topic not in topics:
            log(f"Creating topic {self.topic}")

            cmd_create_topic = [
                os.path.join(self.config["kafka"]["path"], "bin", "kafka-topics.sh"),
                "--create",
                "--bootstrap-server",
                self.config["kafka"]["bootstrap.servers"],
                "--replication-factor",
                "1",
                "--partitions",
                "1",
                "--topic",
                self.topic,
            ]
            with open(
                os.path.join("logs/", "create_topic.stdout"), "w"
            ) as stdout_create_topic:
                # p_create_topic = \
                subprocess.run(
                    cmd_create_topic,
                    stdout=stdout_create_topic,
                    stderr=subprocess.STDOUT,
                )
            time.sleep(1)

    def stop(self):
        # shut down Kafka server and ZooKeeper

        log("Shutting down Kafka Server at localhost:9092")
        # start the Kafka server:
        cmd_kafka_server_stop = [
            os.path.join(self.config["kafka"]["path"], "bin", "kafka-server-stop.sh"),
            os.path.join(self.config["kafka"]["path"], "config", "server.properties"),
        ]

        with open(
            os.path.join("logs/", "kafka_server.stdout"), "w"
        ) as stdout_kafka_server:
            # p_kafka_server_stop = \
            subprocess.run(
                cmd_kafka_server_stop,
                stdout=stdout_kafka_server,
                stderr=subprocess.STDOUT,
            )

        log("Shutting down ZooKeeper at localhost:2181")
        cmd_zookeeper_stop = [
            os.path.join(
                self.config["kafka"]["path"], "bin", "zookeeper-server-stop.sh"
            ),
            os.path.join(
                self.config["kafka"]["path"], "config", "zookeeper.properties"
            ),
        ]

        with open(os.path.join("logs/", "zookeeper.stdout"), "w") as stdout_zookeeper:
            # p_zookeeper_stop = \
            subprocess.run(
                cmd_zookeeper_stop, stdout=stdout_zookeeper, stderr=subprocess.STDOUT
            )

        # delete the content of meta.properties in logs/kafka-logs
        # otherwise, the next time you start Kafka, it will complain that a cluster with a different ID already exists
        # (because the ID is stored in meta.properties)
        meta_properties = os.path.join("logs", "kafka-logs", "meta.properties")
        if os.path.exists(meta_properties):
            os.remove(meta_properties)

    def produce(self):
        log("Starting up Kafka Producer")

        # spin up Kafka producer
        producer = Producer(
            {
                "bootstrap.servers": self.config["kafka"]["bootstrap.servers"]
            }
        )

        alerts = list(self.path_alerts.glob("*.avro"))
        if self.max_alerts is not None:
            alerts = alerts[: self.max_alerts]
        log(f"Streaming {len(alerts)} alerts")
        for p in alerts:
            with open(str(p), "rb") as data:
                # Trigger any available delivery report callbacks from previous produce() calls
                producer.poll(0)

                log(f"Pushing {p}")

                # Asynchronously produce a message, the delivery report callback
                # will be triggered from poll() above, or flush() below, when the message has
                # been successfully delivered or failed permanently.
                while True:
                    try:
                        producer.produce(
                            self.topic, data.read(), callback=delivery_report
                        )
                        break
                    except BufferError:
                        print(
                            "Local producer queue is full (%d messages awaiting delivery): try again\n"
                            % len(producer)
                        )
                        time.sleep(1)

                # Wait for any outstanding messages to be delivered and delivery report
                # callbacks to be triggered.
        producer.flush()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Stream ZTF alerts to Kafka")
    parser.add_argument(
        "--date",
        type=str,
        help="Date for which to fetch alerts in YYYYMMDD format",
        required=False,
    )
    parser.add_argument(
        "--max-alerts",
        type=int,
        help="Maximum number of alerts to stream, optional",
        required=False,
    )
    parser.add_argument(
        "--wait",
        type=int,
        help="Time to wait between between starting kafka and producing alerts",
        required=False,
    )

    args = parser.parse_args()

    date = args.date
    max_alerts = args.max_alerts
    wait = args.wait
    if date in [None, ""]:
        # try readting it from the env as ALERTS_DATE
        date = os.getenv("ALERTS_DATE")
    if wait in [None, ""]:
        # try reading it from the env as WAIT
        wait = os.getenv("WAIT")
    if date in [None, ""]:
        raise ValueError("date must be provided")
    if max_alerts not in [None, ""]:
        max_alerts = int(max_alerts)

    init_kafka()

    print(f"Streaming ZTF alerts for {date}")

    fetch_alerts(date, f"data/{date}", skip_existing=True)

    kafka_stream = KafkaStream(
        topic=f"ztf_{date}_programid1",
        path_alerts=pathlib.Path(f"data/{date}"),
        config=config,
        max_alerts=max_alerts,
    )

    kafka_stream.start()
    
    if wait not in [None, ""]:
        time.sleep(int(wait))
    kafka_stream.produce()
    try:
        while True:
            time.sleep(15)
            log("Heartbeat. Press Ctrl-C to stop.")
    except KeyboardInterrupt:
        kafka_stream.stop()
        print("Stopped by user.")
    except Exception as e:
        kafka_stream.stop()
        print(f"Stopped by exception: {e}")