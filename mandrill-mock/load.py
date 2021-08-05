import aiohttp
import asyncio
import random
import secrets
import uuid


async def send(session):
    recipient_count = random.randint(1, 300)
    data = dict(
        subject_template='Testing ' + secrets.token_hex(5),
        company_code='test',
        from_address='testing@example.com',
        method='email-mandrill',
        recipients=[{'address': f'testing-{i}@other.com'} for i in range(recipient_count)],
        uid=str(uuid.uuid4()),
    )
    print(f'sending to {recipient_count} recipients...')
    headers = {'Authorization': 'testing'}
    async with session.post('http://localhost:5000/send/email/', json=data, headers=headers) as r:
        assert r.status_code == 201, r.status


async def main():
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.shield(send(session))
            await asyncio.sleep(random.random())


if __name__ == '__main__':
    asyncio.run(main())
