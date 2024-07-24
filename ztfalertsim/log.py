import datetime
import os

LOG_DIR = "./logs"
USING_DOCKER = os.environ.get("USING_DOCKER", False)
if USING_DOCKER:
    LOG_DIR = "/data/logs"

if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)


def time_stamp():
    """

    :return: UTC time -> string
    """
    return datetime.datetime.utcnow().strftime("%Y%m%d_%H:%M:%S")


def log(message):
    timestamp = time_stamp()
    print(f"{timestamp}: {message}")

    date = timestamp.split("_")[0]
    try:
        with open(os.path.join(LOG_DIR, f"{date}.log"), "a") as logfile:
            logfile.write(f"{timestamp}: {message}\n")
            logfile.flush()
    # if the file was not found, try to create it
    except FileNotFoundError as e:
        with open(os.path.join(LOG_DIR, f"{date}.log"), "w") as logfile:
            logfile.write(f"{timestamp}: {message}\n")
            logfile.flush()
            print(e)
    return
