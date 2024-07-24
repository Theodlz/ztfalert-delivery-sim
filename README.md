### ZTF ALERT SIMULATOR

The code in this repo allows you to simulate streaming ZTF alerts using Kafka. Given a UT date in YYYYMMDD format, it will fetch public alerts for that data, create a kafka topic with the "official" `ztf_YYYYMMDD_programid1` name, and start streaming the alerts to that topic. We also provide a basic consumer with all the code needed to read off the alerts from the topic, and serialize them a python dictionary.

System requirements:
- Python 3.10 or later
- Java 8 or later

To install the required Python packages, run:
```
pip install -r requirements.txt
```
**Note**: Feel free to use a virtual environment to install the required packages, virtualenv is a good option, but conda should work as well.

### Development:
To start streaming alerts, run:
```
PYTHONPATH=. python ztfalertsim/producer.py --date=YYYYMMDD
```

**Note**: You can also specify a `--max-alerts` argument to limit the number of alerts that are streamed, and a `--wait` argument to specify the time in seconds to wait between starting kafka and producing the first alert. Waiting is needed on some systems to make sure that the kafka server is up and running before the producer starts sending alerts. Also,`--date` and `--wait` can alternatively be set as environment variables `ALERTS_DATE` and `WAIT`, which is for example what we use with the docker-compose setup.

To consume alerts, run:
```
PYTHONPATH=. python ztfalertsim/consumer.py --date=YYYYMMDD
```

### Docker:
To start the kafka server and stream alerts, edit the `docker-compose.yml` file to set the `ALERTS_DATE` environment variable to the desired date, and run:
```
docker-compose up
```

you can alternatively run it in detached mode (in the background) by adding the `-d` flag:
```
docker-compose up -d
```

**Note**: If you wish to see the full logs, just enter the container and use `tail -f` on the log file in the `app/logs` directory.

To stop the kafka server and stream alerts, run:
```
docker-compose down
```

##### Troubleshooting:
- The `network_mode` set to `host` in the docker-compose.yml file allows us to read the kafka topic created in the container without havint to mess with the kafka server configuration. This could be changed in the future to avoid potential conflicts with other services running on the host machine.