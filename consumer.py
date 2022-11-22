from confluent_kafka import Consumer
import pymongo
import json
import datetime
#Intiating Mongo connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["kafkaDb"]
mycol = mydb["Collection"]

#Initiating confluent Kafka cluster configuration

config ={
	'bootstrap.servers': 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
	'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'PLAIN',
	'sasl.username': 'ZMASLRRUXJABOM3A',
	'sasl.password': '/X1fxOXRPaDM9Jsx0moQFabt+eTj+nt6fgZVwViMn0pdl8f4ihapCT/3uJEXQL9B',
	'group.id': 'group_1',
	'auto.offset.reset': 'earliest'
}

c = Consumer(**config)
print('Kafka Consumer has been initiated...')
#Listing topics in the cluster
print('Available topics to consume: ', c.list_topics().topics)
c.subscribe(['topic_3'])


def main():
    while True:
        #Receiving messages from topic
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        # messageReceivedTime=str(datetime.datetime.now())
        data = msg.value()
        print('consumed message on topic {} with value of {} \n'.format(msg.topic(), msg.value()))
        print("data::::", json.loads(data))
        #Inserting data to Mongo
        x = mycol.insert_one(json.loads(data))
    c.close()

if __name__ == '__main__':
    main()

