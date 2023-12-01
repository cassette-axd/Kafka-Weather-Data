from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from kafka import KafkaConsumer
from report_pb2 import Report

broker = "localhost:9092"
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.subscribe(["temperatures"])

while True:
    batch = consumer.poll(1000)
    for topic_partition, messages in batch.items():
        for msg in messages:
            dict = {"partition": topic_partition.partition,
                    "key": str(msg.key, "utf-8"),
                    "date": Report.FromString(msg.value).date,
                    "degrees": Report.FromString(msg.value).degrees}
            print(dict)