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
        filename = f'/files/partition-{curr_partition}.json'
        try:
            with open(filename, 'r') as f:
                partition_data[curr_partition] = json.load(f)
        except FileNotFoundError:
            # Initialize partition-N.json if it does not exist
            partition_data[curr_partition] = {"partition": curr_partition, "offset": 0}
            offset = 0
            # with open(filename, 'w') as f:
            #     json.dump(partition_data[partition], f)
            write_atomic(partition_data[curr_partition], filename)

        # Seek to the specified offset
        consumer.seek(TopicPartition("temperatures", curr_partition), partition_data[curr_partition]['offset'])

    while True:
        batch = consumer.poll(1000)
        for curr_partition, messages in batch.items():
            filename = f'/files/partition-{curr_partition}.json'
            # partition_data = {"partition": partition, "offset": partition.position().offset}
            # with open(filename, 'w') as f:
            #     json.dump(partition_data, f)
            pos = consumer.position(curr_partition)
            partition_data[partition_data[curr_partition.partition]["partition"]].update({"partition": curr_partition.partition, "offset": pos})
            # print(partition_data)
            # partition_data[curr_partition] = {"partition": curr_partition, "offset": partition_data[curr_partition]['offset']}
            for msg in messages:
                value = msg.value
                month = str(msg.key, "utf-8")
                report = Report()
                report.ParseFromString(value)
                date = report.date
                year = date[:4]
                degrees = int(report.degrees)

                print(partition_data)
                if month not in partition_data:
                    partition_data[month] = {}
                if year not in partition_data[month]:
                    partition_data[month][year] = {
                        "count": 1,
                        "sum": degrees,
                        "avg": degrees,
                        "end": date,
                        "start": date
                    }
                # partition_data[partition_data[curr_partition.partition]["partition"]].update({month: {year:{"count": None, "sum": None,
                #     "avg": None, "end": None, "start": None}}})
                print(partition_data)
                print(date)

                # # if there is no start date, assign the first date to start
                # if partition_data[partition_data[curr_partition.partition]["partition"]][month][year]["start"] is None:
                #     partition_data[partition_data[curr_partition.partition]["partition"]][month][year]["start"] = date
                # print(partition_data)

                # # initialize end date
                # if partition_data[partition_data[curr_partition.partition]["partition"]][month][year]['end'] is None:
                #     partition_data[partition_data[curr_partition.partition]["partition"]][month][year]['end'] = date
                
                # convert dates into datetime objects to make them comparable
                curr_date = datetime.strptime(str(date), "%Y-%m-%d")
                # print(partition_data[curr_partition.partition]["partition"])
                # print(list(partition_data[month].keys())[0])
                # print(partition_data[month][year]["end"])
                latest_date = datetime.strptime(str(partition_data[month][year]["end"]), "%Y-%m-%d")
                if curr_date <= latest_date:
                    continue  # Skip rest of loop to suppress duplicate dates
                
                partition_data[month][year]["end"] = date
                partition_data[month][year]['count'] += 1
                partition_data[month][year]['sum'] += degrees
                partition_data[month][year]['avg'] = (
                    partition_data[month][year]['sum'] /
                    partition_data[month][year]['count']
                )

            write_atomic(partition_data, filename)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()



