import asyncio
import logging
from time import time

from aiohttp.web_exceptions import HTTPException, HTTPInternalServerError


async def stats_middleware(app, handler):
    async def _save_request(time_taken, request, response_status):
        sender = app['sender']
        route = request.match_info.route.name or request.path
        status = f'{int(response_status/100)}XX'
        redis_key = app['stats_list_key']
        async with await sender.get_redis_conn() as redis:
            # we put route_name last as it could have colons in it
            _, list_len = await asyncio.gather(
                redis.lpush(redis_key, f'{time_taken:0.4f}:{status}:{request.method}:{route}'),
                redis.llen(redis_key),
            )
            if list_len > app['settings'].max_request_stats:
                # to avoid filling up redis we nuke this data if it exceeds the max length
                tr = redis.multi_exec()
                tr.delete(redis_key)
                tr.set(app['stats_start_key'], time())
                await tr.execute()

    async def _handler(request):
        try:
            start = float(request.headers.get('X-Request-Start', '.'))
        except ValueError:
            start = request.time_service.time()
        http_exception = getattr(request.match_info, 'http_exception', None)
        try:
            if http_exception:
                raise http_exception
            else:
                r = await handler(request)
        except HTTPException as e:
            app.loop.create_task(_save_request(time() - start, request, e.status))
            raise
        else:
            app.loop.create_task(_save_request(time() - start, request, r.status))
            return r

    return _handler


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
            'fingerprint': [request.rel_url, str(response.status)],
            'data': await self.log_extra_data(request, response)
        })

    async def log_exception(self, exc, request):
        self.logger.exception('%s: %s', exc.__class__.__name__, exc, extra={
            'data': await self.log_extra_data(request)
        })

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
                if self.should_log_warnings and e.status >= 400:
                    await self.log_warning(request, e)
                raise
            except BaseException as e:
                await self.log_exception(e, request)
                raise HTTPInternalServerError()
            else:
                if self.should_log_warnings and r.status >= 400:
                    await self.log_warning(request, r)
                return r

        return _handler
