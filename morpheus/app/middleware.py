import asyncio
from time import time

from aiohttp.web import middleware
from aiohttp.web_exceptions import HTTPException


async def _save_request(time_taken, request, response_status):
    if request.match_info.route.name == 'request-stats':
        return
    key = f'{request.method}:{int(response_status/100)}'
    with await request.app['redis'] as redis:
        redis_list_key = request.app['stats_request_list']

        _, _, list_len = await asyncio.gather(
            redis.hincrby(request.app['stats_request_count'], key),
            redis.lpush(redis_list_key, key + f':{int(time_taken * 1000)}'),
            redis.llen(redis_list_key),
        )
        if list_len > request.app['settings'].max_request_stats:
            # to avoid filling up redis we nuke this data if it exceeds the max length
            await redis.delete(redis_list_key)


def get_request_start(request):
    try:
        return float(request.headers.get('X-Request-Start', '.'))
    except ValueError:
        return time()


@middleware
async def stats_middleware(request, handler):
    start = get_request_start(request)
    try:
        r = await handler(request)
    except HTTPException as e:
        request.app.loop.create_task(_save_request(time() - start, request, e.status))
        raise
    else:
        request.app.loop.create_task(_save_request(time() - start, request, r.status))
        return r
