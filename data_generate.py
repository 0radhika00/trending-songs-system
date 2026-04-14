
import asyncio
from mongodb import collection
from faker import Faker
import random
from datetime import datetime, timedelta





fake = Faker()
size_of_collection = 10000


def normalize_data(value,min_val,max_val):
    return (value - min_val) / (max_val - min_val) 

def calculate_trending_score(song_data):
    norm_play_count = normalize_data(song_data['play_count'], 0, 1000000)
    norm_user_rating = normalize_data(song_data['user_rating'], 1, 5)
    norm_social_shares = normalize_data(song_data['social_media_shares'], 0, 100000)
    norm_recency = 1 - normalize_data((datetime.now() - datetime.fromisoformat(song_data['timestamp_last_play'])).days, 0, 365)

    trending_score = (norm_play_count * 0.3) + (norm_user_rating * 0.2) + (norm_social_shares * 0.2) + (norm_recency * 0.3) # geographical location additional factor can be added here
    return round(trending_score, 2)



def generate_data():
    genres = ['rock', 'pop', 'hiphop', 'jazz', 'country', 'electronic', 'rb', 'indie']
    
    data = {
        'song_id': str(fake.uuid4()),
        'title':  fake.catch_phrase(),
        'artist': fake.name(),
        'album': fake.sentence(nb_words=2),
        'genre': random.choice(genres),
        'play_count': random.randint(0, 1000000),
        'rating_count': random.randint(10, 500),
        
        'social_media_shares': random.randint(0, 100000),
        'geographic_popularity': {f'country_{i}': random.randint(1, 100) for i in range(5)},
        'timestamp_last_play': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
    }
    data['rating_sum'] = round(sum(random.uniform(1, 5) for _ in range(data['rating_count'])), 1)
    data['user_rating'] = round(data['rating_sum'] / data['rating_count'], 1) if data['rating_count'] > 0 else 0
    data['trending_score'] = calculate_trending_score(data)
    
    
    return data



async def main():

    await collection.delete_many({})
    await collection.create_index("song_id", unique=True)
    await collection.create_index("trending_score")
    await collection.create_index([("genre",1),("trending_score",-1)])
    await collection.create_index([("artist",1),("trending_score",-1)])

    songs = [generate_data() for _ in range(size_of_collection)]

    await collection.insert_many(songs)

    print(f"Inserted {len(songs)} songs")
    print("Total:", await collection.count_documents({}))


if __name__ == "__main__":
    asyncio.run(main())

    
