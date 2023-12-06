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

    for curr_partition in partitions:
        # Check if partition-N.json exists
        # print(curr_partition)
        filename = f'/files/partition-{curr_partition}.json'
        try:
            with open(filename, 'r') as f:
                partition_data[curr_partition] = json.load(f)
        except FileNotFoundError:
            # Initialize partition-N.json if it does not exist
            partition_data[curr_partition] = {"partition": curr_partition, "offset": 0}

            write_atomic(partition_data[curr_partition], filename)

        # Seek to the specified offset
        consumer.seek(TopicPartition("temperatures", curr_partition), partition_data[curr_partition]['offset'])

    while True:
        batch = consumer.poll(1000)
        for curr_partition, messages in batch.items():
            filename = f'/files/partition-{curr_partition}.json'

            pos = consumer.position(curr_partition)
            partition_data[curr_partition.partition]["offset"] = pos

            for msg in messages:
                value = msg.value
                month = str(msg.key, "utf-8")
                report = Report()
                report.ParseFromString(value)
                date = report.date
                year = date[:4]
                degrees = int(report.degrees)

                # print(partition_data[curr_partition.partition])
                if month not in partition_data[curr_partition.partition]:
                    partition_data[curr_partition.partition][month] = {}
                    # print(partition_data[curr_partition.partition][month])
                if year not in partition_data[curr_partition.partition][month]:
                    partition_data[curr_partition.partition][month][year] = {
                        "count": 1,
                        "sum": degrees,
                        "avg": degrees,
                        "end": date,
                        "start": date
                    }

                print(partition_data)
                
                # convert dates into datetime objects to make them comparable
                curr_date = datetime.strptime(str(date), "%Y-%m-%d")
              
                latest_date = datetime.strptime(str(partition_data[curr_partition.partition][month][year]["end"]), "%Y-%m-%d")
                if curr_date <= latest_date:
                    continue  # Skip rest of loop to suppress duplicate dates
                
                partition_data[curr_partition.partition][month][year]["end"] = date
                partition_data[curr_partition.partition][month][year]['count'] += 1
                partition_data[curr_partition.partition][month][year]['sum'] += degrees
                partition_data[curr_partition.partition][month][year]['avg'] = (
                    partition_data[curr_partition.partition][month][year]['sum'] /
                    partition_data[curr_partition.partition][month][year]['count']
                )

                # print (partition_data[curr_partition])
                write_atomic(partition_data[curr_partition.partition], filename)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()



