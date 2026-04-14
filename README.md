# 🎵 Real-Time Trending Songs System

A scalable system to track and serve **top trending songs** using **FastAPI, MongoDB, Redis, and RabbitMQ**.

---

## 📌 Features

- Real-time song updates via event-driven architecture
- Fast retrieval using Redis caching
- Top 100 trending songs leaderboard
- Genre-based filtering
- Asynchronous processing for scalability

---

## 🛠️ Tech Stack

- FastAPI
- MongoDB (Motor - async)
- Redis
- RabbitMQ (aio-pika)
- Docker & Docker Compose

---

---

## 🚀 Setup Instructions

Clone the Repository

```bash
git clone git@github.com:0radhika00/trending-songs-system.git
cd trending-songs-system


Run the Project (Docker Compose)

To build and start the project using Docker:
```bash
docker compose up --build

Horizontal Scaling (For Large Load / Database Stress Testing)
To run multiple instances of services for better scalability:

```bash
docker compose up --build --scale api=3 --scale consumer=5

Note : As of now hardcoded 1000 fake data is generated

To check the api
http://localhost:8080/trending-songs/genre/indie
http://127.0.0.1:8080/trending-songs
http://127.0.0.1:8080/trending-songs?artist=Shelby%20Harper