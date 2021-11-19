import uuid

from kafka.admin import KafkaAdminClient, NewTopic
import pyarrow as pa  # hdfs api #https://medium.com/analytics-vidhya/hadoop-single-node-cluster-on-docker-e88c3d09a256

import xml_to_csv.transformer


class KafkaConnect:
    BOOTSTRAP_SERVERS = ['localhost:9092']

    def __init__(self):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id='test'
        )

    def create_topic(self, topic_name):
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def register_listener(self, topic):
        # Poll kafka
        def poll():
            # Initialize consumer Instance
            consumer = KafkaConsumer(topic, bootstrap_servers=self.BOOTSTRAP_SERVERS)

            print("About to start polling for topic:", topic)
            consumer.poll(timeout_ms=6000)
            print("Started Polling for topic:", topic)
            for msg in consumer:
                print("Entered the loop\nKey: ", msg.key, " Value:", msg.value)
                kafka_listener(msg)

        print("About to register listener to topic:", topic)
        t1 = threading.Thread(target=poll)
        t1.start()
        print("started a background thread")

    def kafka_listener(data):
        print("Image Ratings:\n", data.value.decode("utf-8"))

    def list_topics(self, group_id):
        consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=self.BOOTSTRAP_SERVERS)
        return consumer.topics()

class ConnectHive:
    pass
    #todo subscribe to topic return hive "select * from topic"

class ConnectHdfs:
    def __init__(self, host, port, user):
        self.host=host
        self.port=port
        self.user=user

    def connect(self):
        return pa.HadoopFileSystem("hdfs:{}:{}?user={}".format(self.host,self.port,self.user)) # pa.hdfs.connect(host=self.host,port=self.port, user=self.user) #"172.17.0.2", port=9870, user="hduser")

    def find_path(self, query):
        return query in self.connect().ls("/")

    def get_csv(self, query):
            return


def main():
    q = 'proton'
    host = "http://export.arxiv.org/"
    api_v = "api"
    query_word = "proton";
    max_res = 100
    kafka = KafkaConnect()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))
    connector_hdfs=ConnectHdfs("nodename", 8020, "hduser")
    if not (q in kafka.list_topics("testID")):
        kafka.create_topic("proton")
        kafka.register_kafka_listener('topic1', kafka.kafka_listener("created"))

        http_connect = xml_to_csv.transformer.HttpConnector(host, api_v,query_word, max_res)
        r = http_connect.request_data()
    else:
        producer.send(q, value=uuid.UUID[0, 6])



if __name__ == "__main__":
    main()
