from motor.motor_asyncio import AsyncIOMotorClient
from abstract import SongRepo

class MongoDB(SongRepo):
    def __init__(self, uri='mongodb://mongo:27017/', db_name='music_db', collection_name='songs'):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        print("Connected to MongoDB successfully!")



    async def get_100_trending_songs(self):
        cursor = self.collection.find({},{"_id": 0,"rating_count": 0,"rating_sum": 0} ).sort("trending_score", -1).limit(100)
        return await cursor.to_list(length=100)

    async def get_trending_songs_by_genre(self, genre, limit=20):
        cursor = self.collection.find({"genre": genre},{"_id": 0,"rating_count": 0,"rating_sum": 0,"geographic_popularity":0,"social_media_shares":0}).sort("trending_score", -1).limit(limit)
        return await cursor.to_list(length=limit)


    

db = MongoDB()
collection = db.collection      