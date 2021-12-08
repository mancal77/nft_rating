from kafka import KafkaProducer

# Kafka settings
topic_name = 'kafka-nft'
brokers = ['cnt7-naya-cdh63:9092']

producer = KafkaProducer(bootstrap_servers=brokers)

# The send() method creates the topic
uuid, item_name, twitter, url, rating = \
    "09dbd245-da77-42f3-a31f-027e3388965c", "Fairy Tales", "FairyTalesNTFs", "https://ftales.io/", 8
# producer.send(topic_name, value=uuid + '|' + item_name + '|' + twitter + '|' + url + '|' + str(rating))
send_value = uuid + '|' + item_name + '|' + twitter + '|' + url + '|' + str(rating)
producer.send(topic_name, value=send_value.encode())
producer.flush()


# One more example
