# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from confluent_kafka.avro import AvroConsumer


# Import sys module
import sys

def consume_record():
    default_group_name = "default-consumer-group"
    consumer_config = {"bootstrap.servers": '192.168.100.14:9092',
                       "schema.registry.url": 'http://192.168.100.14:8081',
                       "group.id": default_group_name,}
    
    consumer = AvroConsumer(consumer_config)
    
    consumer.subscribe(['postgres.public.tiktokcomment'])
    
    running = True
    
    while running:
        message = consumer.poll()
        if message:
            print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
            consumer.commit()
        else:
            print(message.error())
            consumer.close()
            running = False
    
    # try:
    #     message = consumer.poll()
    # except Exception as e:
    #     print(f"Exception while trying to poll messages - {e}")
    # else:
    #     if message:
    #         print(f"Successfully poll a record from "
    #               f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
    #               f"message key: {message.key()} || message value: {message.value()}")
    #         consumer.commit()
    #     else:
    #         print("No new messages at this point. Try again later.")

    # consumer.close()
    
if __name__ == "__main__":
    consume_record()
