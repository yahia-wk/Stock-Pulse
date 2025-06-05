import os, json, asyncio
from polygon import WebSocketClient
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Kafka producer and Polygon WebSocket client
p = Producer({"bootstrap.servers": "localhost:9092"})

# Create a WebSocket client for Polygon.io
ws = WebSocketClient(os.getenv("POLYGON_API_KEY"), "stocks")

async def main():
    async for msg in ws.subscribe("AM.AAPL"):  
           # minute bars for AAPL
        p.produce("stock_ticks", json.dumps(msg).encode())
        p.poll(0)                                 # flush async
asyncio.run(main())