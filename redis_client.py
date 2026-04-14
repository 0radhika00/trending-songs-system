import redis.asyncio as redis
import json
from abstract import SongRepo

class RedisCache(SongRepo):
    def __init__(self, host='redis', port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        
        self.trending_zset_key = "trending_songs_key"
        self.song_prefix = "song:"
        self.genre_zset_key = "trending_genres"          
        self.genre_songs_key = "trending_genre_songs:"
        self.genre_song_prefix = "song:"
        self.max_genres_cached = 5
        self.max_songs_per_genre = 20

        

    async def store_100_trending_songs(self, song_data: list):

        pipe = self.client.pipeline()

        for song in song_data:
            song_id = song['song_id']
            score = song['trending_score']
            #zset ranking
            pipe.zadd(self.trending_zset_key, {song_id: score})
            #full data storage
            pipe.set(f"{self.song_prefix}{song_id}", json.dumps(song))
            
       
        await pipe.execute()


    
    async def get_100_trending_songs(self,limit=100):
        song_ids =  await self.client.zrevrange(
            self.trending_zset_key, 0, limit-1)

        if not song_ids:
            return None

        pipe = self.client.pipeline()

        for song_id in song_ids:
            pipe.get(f"{self.song_prefix}{song_id}")

       
        return await pipe.execute()

    async def get_lowest_score(self):
        result = await self.client.zrange(self.trending_zset_key, 0, 0, withscores=True)
        return result[0][1] if result else -1

    async def store_trending_songs_by_genre(self, genre, songs):
    # Calculate popularity score for the genre
        popularity_score = sum(song['trending_score'] for song in songs) / len(songs)

        # Check lowest genre score in trending_genres ZSet
        lowest_popularity = await self.client.zrange(self.genre_zset_key, 0, 0, withscores=True)
        lowest_score = lowest_popularity[0][1] if lowest_popularity else -1

        if popularity_score > lowest_score:
            genre_key = self.genre_songs_key + genre  # "trending_genre_songs:pop"

            # Storing full data → "song:song_1" = {full song JSON}
            for song in songs:
                await self.client.set(self.genre_song_prefix + song['song_id'], json.dumps(song))

            # Storing song_ids with trendingscore in ZSet for ordering
            song_scores = {song['song_id']: song['trending_score'] for song in songs}
            await self.client.zadd(genre_key, song_scores)

            # Storing genre with popularityscore in ZSet
            await self.client.zadd(self.genre_zset_key, {genre: popularity_score})

            # Get lowest genre before removing 
            lowest_genre = await self.client.zrange(self.genre_zset_key, 0, 0)

            # Keep only top 5 genres remove lowest
            await self.client.zremrangebyrank(self.genre_zset_key, 0, -(self.max_genres_cached + 1))

            # Clean up removed genre data
            if lowest_genre:
                removed_genre = lowest_genre[0]
                removed_genre_key = self.genre_songs_key + removed_genre  # "trending_genre_songs:jazz"

                # Get all song_ids of removed genre
                removed_song_ids = await self.client.zrange(removed_genre_key, 0, -1)

                # Delete each song's full data
                for song_id in removed_song_ids:
                    await self.client.delete(self.genre_song_prefix + song_id)

                # Delete the genre songs ZSet
                await self.client.delete(removed_genre_key)

       


    async def get_trending_songs_by_genre(self, genre):
        genre_key = await self.client.zscore(self.genre_zset_key, genre)
        if genre_key is None:
            return None

        raw = await self.client.get(self.genre_songs_key + genre)
        if not raw:
            return None

        return json.loads(raw)

        



cache = RedisCache()