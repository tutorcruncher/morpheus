import base64
import logging
from buildpg.asyncpg import BuildPgConnection
from fastapi import APIRouter, Depends, Header
from foxglove import glove
from foxglove.db.middleware import get_db
from foxglove.route_class import KeepBodyAPIRoute
from html import escape
from jinja2 import Template
from pathlib import Path
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse
from time import time
from typing import Optional

logger = logging.getLogger('views.common')
app = APIRouter(route_class=KeepBodyAPIRoute)
templates_dir = Path('src/templates/')


@app.get('/', response_class=HTMLResponse)
@app.head('/', response_class=HTMLResponse)
async def index(request: Request):
    ctx = {k: escape(v) for k, v in glove.settings.dict(include={'commit', 'release_date', 'build_time'}).items()}
    ctx['request'] = request
    with open(templates_dir / 'index.jinja') as f:
        html = Template(f.read()).render(**ctx)
    return HTMLResponse(html)


@app.get('/l{token}', response_class=HTMLResponse)
async def click_redirect_view(
    token: str,
    request: Request,
    u: Optional[str] = None,
    X_Forwarded_For: Optional[str] = Header(None),
    X_Request_Start: Optional[str] = Header('.'),
    User_Agent: Optional[str] = Header(None),
    conn: BuildPgConnection = Depends(get_db),
):
    token = token.rstrip('.')
    link = await conn.fetchrow_b('select id, url from links where token=:token limit 1', token=token)
    if arg_url := u:
        try:
            arg_url = base64.urlsafe_b64decode(arg_url.encode()).decode()
        except ValueError:
            arg_url = None

    if link:
        link_id, link_url = link
        if ip_address := X_Forwarded_For:
            ip_address = ip_address.split(',', 1)[0]

        try:
            ts = float(X_Request_Start)
        except ValueError:
            ts = time()

        await glove.redis.enqueue_job('store_click', link_id=link_id, ip=ip_address, user_agent=User_Agent, ts=ts)
        if arg_url and arg_url != link_url:
            logger.warning('db url does not match arg url: %r != %r', link_url, arg_url)
        return RedirectResponse(url=link_url)
    elif arg_url:
        logger.warning('no url found, using arg url "%s"', arg_url)
        return RedirectResponse(url=arg_url)
    else:
        with open(templates_dir / 'not-found.jinja') as f:
            html = Template(f.read()).render({'url': request.url, 'request': request})
        return HTMLResponse(html, status_code=404)
