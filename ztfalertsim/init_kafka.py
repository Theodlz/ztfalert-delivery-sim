from pathlib import Path
import tarfile

import requests

from ztfalertsim.config import load_config

config = load_config(config_files=["config.yaml"])


def init_kafka():
    print("Checking Kafka installation:")
    kafka_path = config["kafka"]["path"]

    path_kafka = Path(kafka_path)
    if not path_kafka.exists():
        print("Kafka not found, downloading and installing...")
        scala_version = config["kafka"]["scala_version"]
        kafka_version = config["kafka"]["kafka_version"]

        # check if by any chance the .tar.gz file is already there
        if not Path(f"kafka_{scala_version}-{kafka_version}.tgz").exists():
            kafka_url = f"https://archive.apache.org/dist/kafka/{kafka_version}/kafka_{scala_version}-{kafka_version}.tgz"
            print(f"Downloading Kafka from {kafka_url}")

            r = requests.get(kafka_url)
            with open(f"kafka_{scala_version}-{kafka_version}.tgz", "wb") as f:
                f.write(r.content)
        else:
            print("Kafka tarball already exists, skipping download...")

        print("Unpacking Kafka...")

        tar = tarfile.open(f"kafka_{scala_version}-{kafka_version}.tgz", "r:gz")
        tar.extractall(path=path_kafka.parent)
    else:
        print("Kafka found!")

    # we copy the server.properties file to the kafka config directory
    # there is an existing one in the kafka directory, so we need to overwrite it
    print("Copying server.properties to Kafka config directory...")
    path_server_properties = Path("server.properties")
    path_kafka_config = Path(kafka_path) / "config"
    path_kafka_config_server_properties = path_kafka_config / "server.properties"

    with open(path_server_properties, "r") as f:
        server_properties = f.read()

    public_host = config["kafka"].get("public_host")
    if public_host not in ["", None, "localhost"]:
        server_properties += f"listeners=PLAINTEXT://0.0.0.0:9092\nadvertised.listeners=PLAINTEXT://{public_host}:9092"
    
    with open(path_kafka_config_server_properties, "w") as f:
        f.write(server_properties)

    print("Done!")


if __name__ == "__main__":
    init_kafka()
    print()
    print("-" * 20)
    print()
