from confluent_kafka import Producer
import json
import time
import logging
import random
import uuid
import datetime
from flask import Flask


print( "Started" )
app = Flask(__name__)

#Confluent Kafka cluster configuration
conf = {
    'bootstrap.servers': 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'ZMASLRRUXJABOM3A',
    'sasl.password': '/X1fxOXRPaDM9Jsx0moQFabt+eTj+nt6fgZVwViMn0pdl8f4ihapCT/3uJEXQL9B',
    # any other config you like ..
}
#Inititatin producer
p = Producer(**conf)
print('Kafka Producer has been initiated...')



def receipt(topic, msg):
    if topic is not None:
        print('Error from: {}'.format(topic))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value())
        print(message)


@app.route("/producemessage")
def main():
    for i in range(10):
        #JSON data preparation.
        data = {
            "EVALTN_INPUT_ID": "1001",
            "EVALTN_INPUT_TYP_ID": "Infrastructure",
            "EVALTN_INPUT_NM": "Configuration Drift",
            "EVALTN_INPUT_DESC": "Asset must be scanned for configuration drift",
            "EVALTN_RQRMNT_DESC" "Review TCDB report to determine if TCDB is installed and scanning on Windows and Unix servers.', 'ITCR-202-05 Configuration Management"
            "CNTRL_RQRMNT_DESC": "ISCR-618-01 Configuring Information Resources Based on Security Baselines",
            "CLOUD_RQRMNT_DESC": "No",
            "messageSendTime": str(datetime.datetime.now())
        }
        m = json.dumps(data)
        p.poll(1)
        #Sending message to topic topic_3
        p.produce('topic_3', m, callback=receipt)

        p.flush()
        time.sleep(3)
    return "Messages are publishing to topic ........."


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3002)

