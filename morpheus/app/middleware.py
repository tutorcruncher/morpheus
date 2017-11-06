import asyncio
import logging
from time import time

from aiohttp.web import middleware
from aiohttp.web_exceptions import HTTPException, HTTPInternalServerError


async def _save_request(time_taken, request, response_status):
    if request.match_info.route.name == 'request-stats':
        return
    key = f'{request.method}:{int(response_status/100)}'
    async with await request.app['sender'].get_redis_conn() as redis:
        redis_list_key = request.app['stats_request_list']

        _, _, list_len = await asyncio.gather(
            redis.hincrby(request.app['stats_request_count'], key),
            redis.lpush(redis_list_key, key + f':{int(time_taken * 1000)}'),
            redis.llen(redis_list_key),
        )
        if list_len > request.app['settings'].max_request_stats:
            # to avoid filling up redis we nuke this data if it exceeds the max length
            await redis.delete(redis_list_key)


@middleware
async def stats_middleware(request, handler):
    try:
        start = float(request.headers.get('X-Request-Start', '.'))
    except ValueError:
        start = time()
    http_exception = getattr(request.match_info, 'http_exception', None)
    try:
        if http_exception:
            raise http_exception
        else:
            r = await handler(request)
    except HTTPException as e:
        request.app.loop.create_task(_save_request(time() - start, request, e.status))
        raise
    else:
        request.app.loop.create_task(_save_request(time() - start, request, r.status))
        return r


class ErrorLoggingMiddleware:
    """
    Middleware for logging exceptions occurring while processing requests,
    also capable of logging warnings - eg. for responses with status >= 400.

    This is setup to play nicely with sentry (https://sentry.io) but just uses
    vanilla python logging so could be used to report exceptions and warnings
    with any logging setup you like.
    """

    def __init__(self, log_name='morpheus.request', log_warnings=True):
        self.logger = logging.getLogger(log_name)
        self.should_log_warnings = log_warnings

    async def log_extra_data(self, request, response=None):
        return dict(
            request_url=str(request.rel_url),
            request_method=request.method,
            request_host=request.host,
            request_headers=dict(request.headers),
            request_text=response and await request.text(),
            response_status=response and response.status,
            response_headers=response and dict(response.headers),
            response_text=response and response.text,
        )

    async def log_warning(self, request, response):
        self.logger.warning('%s %d', request.rel_url, response.status, extra={
            'fingerprint': [str(request.rel_url), str(response.status)],
            'data': await self.log_extra_data(request, response)
        })

    async def log_exception(self, exc, request):
        self.logger.exception('%s: %s', exc.__class__.__name__, exc, extra={
            'data': await self.log_extra_data(request)
        })

    def should_warning(self, r):
        return (
            self.should_log_warnings and
            r.status >= 400 and
            not (r.status == 401 and 'WWW-Authenticate' in r.headers) and
            not r.status == 409 and
            '127.0.0.1' not in r.headers.get('Origin', '') and
            'localhost' not in r.headers.get('Origin', '')
        )

    async def __call__(self, app, handler):
        async def _handler(request):
            try:
                http_exception = getattr(
                    request.match_info, 'http_exception', None
                )
                if http_exception:
                    raise http_exception
                else:
                    r = await handler(request)
            except HTTPException as e:
                if self.should_warning(e):
                    await self.log_warning(request, e)
                raise
            except BaseException as e:
                await self.log_exception(e, request)
                raise HTTPInternalServerError()
            else:
                if self.should_warning(r):
                    await self.log_warning(request, r)
                return r

        return _handler
