import asyncio
import json
import random
import aio_pika
from data_generate import calculate_trending_score
from mongodb import collection

Changed_song_size = 10
BATCH_SIZE = 10
UPDATE_INTERVAL = 10  # 10 minutes


# --- Async publish ---
async def publish_event(channel, song_id, data):
    event = {
        'song_id': song_id,
        'play_count': data['play_count'],
        'user_rating': data['user_rating'],
        'social_media_shares': data['social_media_shares'],
        'trending_score': data['trending_score'],
        'title': data['title'],
        'artist': data['artist'],
        'album': data['album'],
        'genre': data['genre'],
        'timestamp_last_play': data['timestamp_last_play']
    }
    print(f"Produced: {event}")
    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(event).encode()),
        routing_key='song_events'
    )


# --- Update logic ---
async def random_generator(channel, song_id):
    new_play_count = random.randint(0, 1000)
    new_social_media_shares = random.randint(0, 1000)
    new_votes = random.randint(1, 20)
    new_votes_geographich_popularity = random.randint(1, 100)
    new_rating_sum = round(
        sum(random.uniform(1, 5) for _ in range(new_votes)), 1
    )
   
    updated_data = await collection.find_one({'song_id': song_id})

    if not updated_data:
        return  

    updated_data['rating_count'] += new_votes
    updated_data['rating_sum'] += new_rating_sum
    updated_data['play_count'] += new_play_count
    updated_data['social_media_shares'] += new_social_media_shares

    updated_data['user_rating'] = round(
        updated_data['rating_sum'] / updated_data['rating_count'], 1
    )

    updated_data['trending_score'] = calculate_trending_score(updated_data)

    await publish_event(channel, song_id, updated_data)

    return updated_data


# --- Get random songs ---
async def get_random_song_ids(size):
    cursor = collection.aggregate([
        {"$sample": {"size": size}},
        {"$project": {"song_id": 1, "_id": 0}}
    ])
    docs = await cursor.to_list(length=size)
    return [d["song_id"] for d in docs]


# --- Stream simulator ---
async def stream_simulator(channel):
    while True:
        num = random.randint(1, Changed_song_size)
        song_ids = await get_random_song_ids(Changed_song_size)

        for i in range(0, num, BATCH_SIZE):
            batch = song_ids[i:i+BATCH_SIZE]

            tasks = [random_generator(channel, sid) for sid in batch]
            await asyncio.gather(*tasks)

            print(f"Updated {len(batch)} songs.")

        await asyncio.sleep(UPDATE_INTERVAL)


# --- Main ---
async def main():
    print("Producer started...")

    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@rabbitmq/"
    )

    channel = await connection.channel()

    await channel.declare_queue("song_events")

    await stream_simulator(channel)


if __name__ == "__main__":
    asyncio.run(main())