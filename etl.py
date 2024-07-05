import asyncio
import aiohttp
import asyncpg
import hashlib

async def fetch_message(session, queue_url):
    async with session.get(queue_url) as response:
        return await response.json()

async def process_message(message):
    masked_ip = hashlib.sha256(message['ip'].encode()).hexdigest()
    masked_device_id = hashlib.sha256(message['device_id'].encode()).hexdigest()
    return {
        'user_id': message['user_id'],
        'device_type': message['device_type'],
        'masked_ip': masked_ip,
        'masked_device_id': masked_device_id,
        'locale': message['locale'],
        'app_version': message['app_version'],
        'create_date': message['create_date']
    }

async def write_to_db(pool, records):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.executemany("""
                INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, records)

async def main():
    queue_url = "http://localhost:4566/000000000000/login-queue"
    batch_size = 10
    async with aiohttp.ClientSession() as session:
        pool = await asyncpg.create_pool(dsn="postgresql://postgres:postgres@localhost/postgres")
        while True:
            tasks = [fetch_message(session, queue_url) for _ in range(batch_size)]
            messages_batch = await asyncio.gather(*tasks)
            if not any(message.get('Messages') for message in messages_batch):
                break
            processed_records = [await process_message(msg) for message in messages_batch for msg in message['Messages']]
            await write_to_db(pool, processed_records)

if __name__ == "__main__":
    asyncio.run(main())
