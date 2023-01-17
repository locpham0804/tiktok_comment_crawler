# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer, TopicPartition
import json
from datetime import datetime, timezone


# Import sys module
import sys

def consume_debezium_record():
    # prepare consumer
    topic = 'postgres.public.tiktokcomment'
    tp = TopicPartition(topic,0)
    consumer = KafkaConsumer(group_id = 'consumer-group_monitor',
                             bootstrap_servers = '192.168.100.14:9092',
                             auto_offset_reset='latest')
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    
    # obtain the last offset value
    lastOffset = consumer.end_offsets([tp])[tp]
    
    print('start check postgres topic tiktokcomment')
    
    insert_count = 0
    
    for message in consumer:
    ## Filter "insert-type" change from table
        data = message.value.decode('utf-8')
        if json.loads(data)['payload']['before'] is None and json.loads(data)['payload']['after'] is not None:
            monitor_row = json.loads(data)['payload']['after']
            print(monitor_row)
            insert_count += 1
            if message.offset == lastOffset - 1:
                break
            else: continue
        else:
            if message.offset == lastOffset - 1:
                break
            else: continue
    
    return insert_count

def consume_kafka_topic_record():
    # prepare consumer
    topic = 'raw_comment_tiktok'
    tp = TopicPartition(topic,0)
    consumer_postgres = KafkaConsumer(group_id = 'tiktok_consumer_group_monitor',
                        bootstrap_servers = '192.168.100.14:9092',
                        auto_offset_reset='latest')
    consumer_postgres.assign([tp])
    consumer_postgres.seek_to_beginning(tp)
    
    print('start check kafka topic raw_comment_tiktok')
    
    # obtain the last offset value
    lastOffset = consumer_postgres.end_offsets([tp])[tp]
    
    kafka_topic_messages_count = 0
    
    for message in consumer_postgres:
        data = message.value.decode('utf-8')
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value.decode('utf-8')))
        print('start checking')
        try:
            time_check = json.loads(data)['time_created']
            print(time_check)
            kafka_topic_messages_count += 1
            if message.offset == lastOffset - 1:
                break
            else: continue
        except:
            if message.offset == lastOffset - 1:
                break
            else: continue
    
    return kafka_topic_messages_count

def consume_record():
                
    check_row = consume_debezium_record()
    check_message = consume_kafka_topic_record()
    print(f'row_insert: {check_row}')
    print(f'message_count: {check_message}')
    if check_row == check_message:
        print('Matched')
    else:
        print('Unmatched')
    
if __name__ == "__main__":
    consume_record()
