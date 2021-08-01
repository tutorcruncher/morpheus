import base64
import logging
from fastapi import APIRouter, Depends, Header
from foxglove import glove
from foxglove.exceptions import HttpRedirect
from foxglove.route_class import KeepBodyAPIRoute
from html import escape
from starlette.requests import Request
from starlette.templating import Jinja2Templates
from time import time
from typing import Optional

from src import crud
from src.utils import get_db

logger = logging.getLogger('views.common')
app = APIRouter(route_class=KeepBodyAPIRoute)
templates = Jinja2Templates(directory='src/templates/')


@app.get('/')
async def index(request: Request):
    ctx = {k: escape(v) for k, v in glove.settings.dict(include={'commit', 'release_date', 'build_time'}).items()}
    ctx['request'] = request
    return templates.TemplateResponse('index.jinja', context=ctx)


@app.get('/l{token}/')
async def click_redirect_view(
    token: str,
    u: str,
    request: Request,
    X_Forwarded_For: Optional[str] = Header(None),
    X_Request_Start: Optional[str] = Header(None),
    User_Agent: Optional[str] = Header(None),
    conn=Depends(get_db),
):
    token = token.rstrip('.')
    link = crud.get_link(conn, token)

    if arg_url := u:
        try:
            arg_url = base64.urlsafe_b64decode(arg_url.encode()).decode()
        except ValueError:
            arg_url = None

    if link:
        if ip_address := X_Forwarded_For:
            ip_address = ip_address.split(',', 1)[0]

        try:
            ts = float(X_Request_Start, '.')
        except ValueError:
            ts = time()

        link_id, url = link

        await glove.redis.enqueue_job('store_click', link_id=link_id, ip=ip_address, user_agent=User_Agent, ts=ts)
        if arg_url and arg_url != url:
            logger.warning('db url does not match arg url: %r != %r', url, arg_url)
        raise HttpRedirect(location=url)
    elif arg_url:
        logger.warning('no url found, using arg url "%s"', arg_url)
        raise HttpRedirect(location=arg_url)
    else:
        return dict(url=request.url, http_status_=404)
