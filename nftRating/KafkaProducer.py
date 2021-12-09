from kafka import KafkaProducer

# Kafka settings
topic_name = 'kafka-nft'
brokers = ['cnt7-naya-cdh63:9092']

producer = KafkaProducer(bootstrap_servers=brokers)


# The send() method creates the topic
def send_to_kafka(uuid, item_name, twitter, url, rating):
    send_value = uuid + '|' + item_name + '|' + twitter + '|' + url + '|' + str(rating)
    producer.send(topic_name, value=send_value.encode())
    producer.flush()
