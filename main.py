from fastapi import FastAPI
from redis_client import cache
from mongodb import db
import json

app = FastAPI()


@app.get("/trending-songs")
async def get_trending_songs(genre: str = None,artist: str = None):
    songs = await cache.get_100_trending_songs()
  
    if songs is not None:
        print("Cache hit")
        songs = [json.loads(song) for song in songs]
        if genre:
            songs = [
                song for song in songs
                if song.get("genre") and song["genre"].lower() == genre.lower()
            ]

        # Filter by artist
        if artist:
            songs = [
                song for song in songs
                if song.get("artist") and song["artist"].lower() == artist.lower()
            ]
        return {"source": "cache", "songs": songs}
    print("Cache miss")
    songs = await db.get_100_trending_songs()
    if songs:
        print("Storing trending songs in cache...")
        await cache.store_100_trending_songs(songs)
        songs = [json.loads(song) for song in songs]
        if genre:
            songs = [
                song for song in songs
                if song.get("genre") and song["genre"].lower() == genre.lower()
            ]

        # Filter by artist
        if artist:
            songs = [
                song for song in songs
                if song.get("artist") and song["artist"].lower() == artist.lower()
            ]
        return {"source": "database", "songs": songs}
    return {"message": "No trending songs found."}




@app.get("/trending-songs/genre/{genre_name}")
async def get_trending_songs_by_genre(genre_name: str):
    genre_songs = await cache.get_trending_songs_by_genre(genre_name)

    if genre_songs:
        print("Cache hit")
        return {"source": "cache", "songs": genre_songs}

    print("Cache miss")
    songs = await db.get_trending_songs_by_genre(genre_name)
    if songs:
        print("Storing trending songs in cache...")
        await cache.store_trending_songs_by_genre(genre_name, songs)
        return {"source": "database", "songs": songs}
    return {"message": f"No trending songs found for genre {genre_name}."}


