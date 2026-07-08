import base64
import logging
from html import escape
from pathlib import Path
from time import time
from typing import Optional

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Template
from sqlmodel import select

from app.core.config import settings
from app.core.database import DBSession, get_db
from app.messages.models import Link
from app.messages.tasks import store_click

logger = logging.getLogger('views.common')
router = APIRouter()
templates_dir = Path(__file__).parent.parent.parent / 'templates'


@router.get('/', response_class=HTMLResponse)
@router.head('/', response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    ctx = {
        k: escape(v or '')
        for k, v in {
            'commit': settings.commit,
            'release_date': settings.release_date,
            'build_time': settings.build_time,
        }.items()
    }
    ctx['request'] = request  # ty:ignore[invalid-assignment]
    with open(templates_dir / 'index.jinja') as f:
        html = Template(f.read()).render(**ctx)
    return HTMLResponse(html)


@router.get('/l{token}', response_class=HTMLResponse)
def click_redirect_view(
    token: str,
    request: Request,
    u: Optional[str] = None,
    X_Forwarded_For: Optional[str] = Header(None),
    X_Request_Start: Optional[str] = Header('.'),
    User_Agent: Optional[str] = Header(None),
    db: DBSession = Depends(get_db),
):
    token = token.rstrip('.')
    link = db.exec(select(Link.id, Link.url).where(Link.token == token).limit(1)).first()
    arg_url: Optional[str] = u
    if arg_url:
        try:
            arg_url = base64.urlsafe_b64decode(arg_url.encode()).decode()
        except ValueError:
            arg_url = None

    if link:
        link_id, link_url = link
        ip_address = X_Forwarded_For
        if ip_address:
            ip_address = ip_address.split(',', 1)[0]

        try:
            ts = float(X_Request_Start)  # ty:ignore[invalid-argument-type]
        except ValueError:
            ts = time()

        store_click.delay(link_id=link_id, ip=ip_address, user_agent=User_Agent, ts=ts)
        if arg_url and arg_url != link_url:
            logger.warning('db url does not match arg url: %r != %r', link_url, arg_url)
        return RedirectResponse(url=link_url)
    elif arg_url:
        logger.warning('no url found, using arg url "%s"', arg_url)
        return RedirectResponse(url=arg_url)
    else:
        with open(templates_dir / 'not-found.jinja') as f:
            html = Template(f.read()).render(url=str(request.url), request=request)
        return HTMLResponse(html, status_code=404)
