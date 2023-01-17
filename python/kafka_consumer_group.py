from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine
from sqlalchemy import text

def consume():
    # Consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('raw_comment_tiktok',
                            group_id = 'tiktok_consumer_group',
                            bootstrap_servers = '192.168.100.14:9092',
                            auto_offset_reset='latest')
    print('Kafka consumer connected')
    
    # Create engine connect to postgresql
    engine = create_engine('postgresql+psycopg2://postgres:postgres@192.168.100.14:5432/tiktokrawdata')    
    print('Connected with postgres')
    
    # Create table if not exist
    with engine.connect() as con:
        statement = text("""
                        CREATE TABLE IF NOT EXISTS tiktokcomment (
                            user_Id bigint,
                            unique_Id text,
                            nickname text,
                            follow_Role int,
                            "comment" text,
                            live_User_unique_Id text,
                            time_created timestamp
                        )
                        """)
        con.execute(statement)
        print("Created table tiktokcomment")

    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        data = message.value.decode('utf-8')
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value.decode('utf-8')))
        try:
            with engine.connect() as con:
                statement = text("""
                                INSERT INTO tiktokcomment(user_Id, unique_Id, nickname, follow_Role, comment, live_User_unique_Id, time_created) 
                                VALUES(:user_Id, :unique_Id, :nickname, :follow_Role, :comment, :live_User_unique_Id, :time_created)
                                """)
                con.execute(statement, json.loads(data))
                con.close()
        except:
            continue
    
if __name__ == '__main__':
    # Run the client and block the main thread
    # await client.start() to run non-blocking
    consume()