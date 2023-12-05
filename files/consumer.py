from kafka import KafkaConsumer, TopicPartition
import json
import sys
import os
from collections import defaultdict
from report_pb2 import Report
from datetime import datetime

def write_atomic(data, path):
    path2 = path + ".tmp"
    with open(path2, "w") as f:
        json.dump(data, f)
    os.replace(path2, path)

partitions = [int(partition) for partition in sys.argv[1:]]
partitions.sort()

broker = "localhost:9092"

consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.assign([TopicPartition("temperatures", partition) for partition in partitions])
# consumer.seek_to_beginning()

try:
    partition_data = {partition: {"partition": partition, "offset": 0} for partition in partitions}
    for partition in partitions:
        # Check if partition-N.json exists
        filename = f'partition-{partition}.json'
        try:
            with open(filename, 'r') as f:
                partition_data[partition] = json.load(f)
        except FileNotFoundError:
            # Initialize partition-N.json if it does not exist
            partition_data[partition] = {"partition": partition, "offset": 0}
            offset = 0
            # with open(filename, 'w') as f:
            #     json.dump(partition_data[partition], f)
            write_atomic(partition_data[partition], filename)

        # Seek to the specified offset
        consumer.seek(TopicPartition("temperatures", partition), partition_data[partition]['offset'])

    while True:
        batch = consumer.poll(1000)
        for partition, messages in batch.items():
            filename = f'partition-{partition}.json'
            # partition_data = {"partition": partition, "offset": partition.position().offset}
            # with open(filename, 'w') as f:
            #     json.dump(partition_data, f)
            partition_data[TopicPartition("temperatures", partition)].update({"partition": partition, "offset": messages[-1].offset + 1})
            for msg in messages:
                value = msg.value
                month = str(msg.key, "utf-8")
                report = Report()
                report.ParseFromString(value)
                date = report.date
                year = date[:4]
                degrees = int(report.degrees)

                curr_date = datetime.strptime(str(date), "%Y-%m-%d")
                latest_date = datetime.strptime(str(partition_data[partition][month][year]['end']), "%Y-%m-%d")
                if curr_date <= latest_date:
                    continue  # Suppress duplicate dates
                
                partition_data[partition][month][year]['end'] = date
                partition_data[partition][month][year]['count'] += 1
                partition_data[partition][month][year]['sum'] += degrees
                partition_data[partition][month][year]['avg'] = (
                    partition_data[partition][month][year]['sum'] /
                    partition_data[partition][month][year]['count']
                )

                if partition_data[partition][month][year]['start'] is None:
                    partition_data[partition][month][year]['start'] = date

            write_atomic(partition_data[partition], filename)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()



