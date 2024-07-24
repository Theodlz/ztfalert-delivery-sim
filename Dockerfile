# dockerfile with python 3.12
FROM python:3.12

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install system requirements: java
RUN apt-get update && apt install default-jre -y

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install kafka
RUN python init_kafka.py

# Make port 9092 available to the world outside this container
EXPOSE 9092

# Run app.py when the container launches, the ALERTS_DATE environment variable will be used
# as the --date argument for the producer.py script, read directly from the environment in the script
CMD ["python", "ztfalertsim/producer.py"]
