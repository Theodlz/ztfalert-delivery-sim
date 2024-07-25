# dockerfile with python 3.12
FROM python:3.12

# Install system requirements: java
RUN apt-get update && apt install default-jre -y

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install kafka
RUN PYTHONPATH=. python ztfalertsim/init_kafka.py

# Make port 9092 available to the world outside this container
EXPOSE 9092

# set a PYTHONPATH environment variable with value ".", so that the producer.py script can be run
# from the root directory of the project, and find all the imports as expected
ENV PYTHONPATH=.

# Run app.py when the container launches, the ALERTS_DATE environment variable will be used
# as the --date argument for the producer.py script, read directly from the environment in the script
CMD ["python", "ztfalertsim/producer.py"]
