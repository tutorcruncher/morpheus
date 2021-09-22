import base64
import logging
from fastapi import APIRouter, Depends, Header
from foxglove import glove
from foxglove.route_class import KeepBodyAPIRoute
from html import escape
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from time import time
from typing import Optional

from src.db import get_session
from src.models import Link

logger = logging.getLogger('views.common')
app = APIRouter(route_class=KeepBodyAPIRoute)
templates = Jinja2Templates(directory='src/templates/')


@app.get('/')
@app.head('/')
async def index(request: Request):
    ctx = {k: escape(v) for k, v in glove.settings.dict(include={'commit', 'release_date', 'build_time'}).items()}
    ctx['request'] = request
    return templates.TemplateResponse('index.jinja', context=ctx)


@app.get('/l{token}')
async def click_redirect_view(
    token: str,
    request: Request,
    u: Optional[str] = None,
    X_Forwarded_For: Optional[str] = Header(None),
    X_Request_Start: Optional[str] = Header('.'),
    User_Agent: Optional[str] = Header(None),
    conn: AsyncSession = Depends(get_session),
):
    token = token.rstrip('.')
    try:
        link = await Link.manager(conn).get(token=token)
    except NoResultFound:
        link = None
    if arg_url := u:
        try:
            arg_url = base64.urlsafe_b64decode(arg_url.encode()).decode()
        except ValueError:
            arg_url = None

    if link:
        if ip_address := X_Forwarded_For:
            ip_address = ip_address.split(',', 1)[0]

        try:
            ts = float(X_Request_Start)
        except ValueError:
            ts = time()

        await glove.redis.enqueue_job('store_click', link_id=link.id, ip=ip_address, user_agent=User_Agent, ts=ts)
        if arg_url and arg_url != link.url:
            logger.warning('db url does not match arg url: %r != %r', link.url, arg_url)
        return RedirectResponse(url=link.url)
    elif arg_url:
        logger.warning('no url found, using arg url "%s"', arg_url)
        return RedirectResponse(url=arg_url)
    else:
        return templates.TemplateResponse(
            'not-found.jinja', context={'url': request.url, 'request': request}, status_code=404
        )
