from abc import ABC, abstractmethod

class SongRepo(ABC):
    @abstractmethod
    async def get_trending_songs_by_genre(self, genre:str):
        pass


    @abstractmethod
    async def get_100_trending_songs(self):
        pass
