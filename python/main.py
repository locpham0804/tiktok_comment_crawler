from TikTokLive import TikTokLiveClient
from TikTokLive.types.events import CommentEvent, ConnectEvent, LiveEndEvent

from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime as dt
import sys
import json

def main(id_url):

    # Instantiate the client with the user's username
    client = TikTokLiveClient(unique_id=id_url)
    
    # Create engine connect to postgresql
    # engine = create_engine('postgresql+psycopg2://postgres:postgres@192.168.100.14:5432/tiktokrawdata')
    
    # Create table if not exist
    # with engine.connect() as con:
    #     statement = text("""
    #                     CREATE TABLE IF NOT EXISTS tiktokcomment (
    #                         user_Id bigint,
    #                         unique_Id text,
    #                         nickname text,
    #                         follow_Role int,
    #                         "comment" text,
    #                         live_User_unique_Id text,
    #                         time_created timestamp
    #                     )
    #                     """)
    #     con.execute(statement)

    # Define how you want to handle specific events via decorator
    @client.on("connect")
    async def on_connect(_: ConnectEvent):
        print("Connected to Room ID:", client.room_id)


    # Notice no decorator?
    async def on_comment(event: CommentEvent):
        # print(event)
        userId = event.user.userId
        uniqueId = event.user.uniqueId
        nickname = event.user.nickname
        followRole = event.user.extraAttributes.followRole
        comment = event.comment
        data = ({ "user_Id": userId, "unique_Id": uniqueId, "nickname": nickname, "follow_Role": followRole, "comment" : comment, "live_User_unique_Id": id_url, "time_created": dt.now()})
        
        # with engine.connect() as con:
        #     statement = text("""
        #                     INSERT INTO tiktokcomment(user_Id, unique_Id, nickname, follow_Role, comment, live_User_unique_Id, time_created) 
        #                     VALUES(:user_Id, :unique_Id, :nickname, :follow_Role, :comment, :live_User_unique_Id, :time_created)
        #                     """)
        #     con.execute(statement, data)
        #     con.close()
        print(f"{event.user.nickname} -> {event.comment}")
        
    # async def on_end_live(event: LiveEndEvent):
    #     engine.dispose()
        

    # Define handling an event via "callback"
    client.add_listener("comment", on_comment)
    # client.add_listener("live_end", on_end_live)
    client.run()

if __name__ == '__main__':
    # Run the client and block the main thread
    # await client.start() to run non-blocking
    main(sys.argv[1])