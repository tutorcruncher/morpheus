from aiohttp.web import Application

from .settings import Settings


def create_app(settings: Settings=None):
    settings = settings or Settings()
    app = Application()  # TODO middleware
    app['settings'] = settings

    return app
