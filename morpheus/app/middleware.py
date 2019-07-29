import asyncio
import logging
from time import time

from aiohttp.web import middleware
from aiohttp.web_exceptions import HTTPException

from .utils import OkCancelError


async def _save_request(time_taken, request, response_status):
    if request.match_info.route.name == 'request-stats':
        return
    key = f'{request.method}:{int(response_status/100)}'
    redis_pool = await request.app['sender'].get_redis()
    with await redis_pool as redis:
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

    async def log_extra_data(self, request, duration, response=None):
        return dict(
            request_url=str(request.rel_url),
            request_method=request.method,
            request_host=request.host,
            request_headers=dict(request.headers),
            request_text=None if response is None else await request.text(),
            response_status=None if response is None else response.status,
            response_headers=None if response is None else dict(response.headers),
            response_text=None if response is None else response.text,
            request_duration=duration,
            route=self.get_route_name(request),
        )

    async def log_warning(self, request, response, start):
        self.logger.warning(
            '%d %s',
            response.status,
            request.rel_url,
            extra={
                'fingerprint': [str(request.rel_url), str(response.status)],
                'data': await self.log_extra_data(request, time() - start, response),
            },
        )

    async def log_exception(self, exc, request, start):
        self.logger.exception(
            '%s: %s', exc.__class__.__name__, exc, extra={'data': await self.log_extra_data(request, time() - start)}
        )

    def should_warning(self, req, resp):
        return (
            self.should_log_warnings
            and resp.status >= 400
            and not (resp.status == 401 and 'WWW-Authenticate' in resp.headers)
            and not resp.status == 409
            and not self.is_local(resp)
            and not (resp.status == 403 and self.get_route_name(req) == 'user-messages' and 'company' in req.query)
        )

    @staticmethod
    def get_route_name(r):
        try:
            return r.match_info.route.name
        except AttributeError:
            return None

    @staticmethod
    def is_local(r):
        origin = r.headers.get('Origin', '')
        return '127.0.0.1' in origin or 'localhost' in origin

    @middleware
    async def middleware(self, request, handler):
        start = get_request_start(request)
        try:
            http_exception = getattr(request.match_info, 'http_exception', None)
            if http_exception:
                raise http_exception
            else:
                r = await handler(request)
        except OkCancelError:
            raise
        except HTTPException as e:
            if self.should_warning(request, e):
                await self.log_warning(request, e, start)
            raise
        except BaseException as e:
            await self.log_exception(e, request, start)
            raise
        else:
            if self.should_warning(request, r):
                await self.log_warning(request, r, start)
            return r
