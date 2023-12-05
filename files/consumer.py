from kafka import KafkaConsumer, TopicPartition
import json
import sys

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
        path = f'partition-{partition}.json'
        try:
            with open(filename, 'r') as f:
                partition_data[partition] = json.load(f)
        except FileNotFoundError:
            # Initialize partition-N.json if it does not exist
            partition_data = {"partition": partition, "offset": 0}
            offset = 0
            # with open(filename, 'w') as f:
            #     json.dump(partition_data[partition], f)
            path2 = path + ".tmp"
            with open(path2, "w") as f:
                json.dump(partition_data[partition], f)
            os.replace(path2, path)
        # Seek to the specified offset
        consumer.seek(TopicPartition(topic, partition), partition_data[partition]['offset'])

    while True:
        batch = consumer.poll(1000)
        for partition, messages in batch.items():
            path = f'partition-{partition}.json'
            # partition_data = {"partition": partition, "offset": partition.position().offset}
            # with open(filename, 'w') as f:
            #     json.dump(partition_data, f)
            partition_data[partition] = {"partition": partition, "offset": messages[-1].offset + 1}
            path2 = path + ".tmp"
            with open(path2, "w") as f:
                json.dump(partition_data[partition], f)
            os.replace(path2, path)
except: KeyboardInterrupt:
    pass
finally:
    consumer.close()



