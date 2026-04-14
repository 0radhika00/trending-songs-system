import asyncio
import json
import aio_pika
from redis_client import cache
from mongodb import db   # use your MongoDB class instance


# --- Redis update ---
async def update_cache(event):
    song_id = event['song_id']
    key = cache.song_prefix + song_id # for trending song update
    genre = event['genre']

    genre_in_redis = cache.client.zscore(cache.genre_zset_key, genre)
    score = event['trending_score']
    #For trending song List

    lowest_score = await cache.get_lowest_score()

    if score > lowest_score:
        await cache.client.set(
            key,
            json.dumps(event)
        )

        #leaderboard update
        await cache.client.zadd(
            cache.trending_zset_key,
            {song_id: score}
        )

        # Remove extra 101
        await cache.client.zremrangebyrank(
            cache.trending_zset_key,
            0, -101
        )
    else:

        exists = await cache.client.exists(key)
        if exists:
            await cache.client.set(
                key,
                json.dumps(event)
            )

        
            await cache.client.zadd(
                cache.trending_zset_key,
                {song_id: event['trending_score']}
            )
    #For genre Check
    if genre_in_redis:
        genre_key = cache.genre_songs_key + genre  
        raw = await cache.client.zscore(genre_key,song_id)

        if raw:
            await cache.client.set(
            cache.genre_song_prefix + song_id,
            json.dumps(event))

            await cache.client.zadd(genre_key, {song_id: score})
            #recalculate score
            all_scores = await cache.client.zrange(genre_key, 0, -1, withscores=True)

            if all_scores:
                avg_score = sum(score for _, score in all_scores) / len(all_scores)

                # 6. Update genre score
                await cache.client.zadd(cache.genre_zset_key, {genre: avg_score})


# --- Mongo update ---
async def update_db(event):
    await db.collection.update_one(
        {"song_id": event['song_id']},
        {"$set": event}
    )


# --- Message handler ---
async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        event = json.loads(message.body)
        print(f"Processed: {event['song_id']}")

        await update_cache(event)
        await update_db(event)

        print(f"Processed: {event['song_id']}")


# --- Main consumer ---
async def main():
    print("Consumer started...")

    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@rabbitmq/"
    )

    channel = await connection.channel()

    queue = await channel.declare_queue("song_events")

    await queue.consume(process_message)

    await asyncio.Future()  # keep running forever


if __name__ == "__main__":
    asyncio.run(main())