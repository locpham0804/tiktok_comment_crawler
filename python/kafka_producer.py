from kafka import KafkaProducer
from datetime import datetime
from datetime import timezone
import json
import sys

from TikTokLive import TikTokLiveClient
from TikTokLive.types.events import CommentEvent, ConnectEvent, LiveEndEvent

def main(id_url):
    
    print(id_url)
    # Instantiate the client with the user's username
    client = TikTokLiveClient(unique_id=id_url)
    
    # Define how you want to handle specific events via decorator
    @client.on("connect")
    async def on_connect(_: ConnectEvent):
        print("Connected to Room ID:", client.room_id)
        
    # Connect to kafka
    producer = KafkaProducer(bootstrap_servers = '192.168.100.14:9092')
    print("Kafka producer has been initiated")
        
        
    # Notice no decorator?
    async def on_comment(event: CommentEvent):
        # print(event)
        userId = event.user.userId
        uniqueId = event.user.uniqueId
        nickname = event.user.nickname
        followRole = event.user.extraAttributes.followRole
        comment = event.comment
        tz = timezone.utc
        data = { 
                "user_Id": userId, 
                "unique_Id": uniqueId, 
                "nickname": nickname, 
                "follow_Role": followRole, 
                "comment" : comment, 
                "live_User_unique_Id": id_url, 
                "time_created": datetime.now(tz=tz).strftime('%Y-%m-%dT%H:%M:%S%z')
                }
        
        message = json.dumps(data)
        print(message)
        producer.send('raw_comment_tiktok', message.encode('utf-8'))
        
        print(f"{event.user.nickname} -> {event.comment}")
        

    # Define handling an event via "callback"
    client.add_listener("comment", on_comment)
    client.run()
    
if __name__ == '__main__':
    # Run the client and block the main thread
    # await client.start() to run non-blocking
    main(id_url = sys.argv[1])