"""HTTP exceptions whose response bodies are `{'message': '...'}`.

Mirrors the legacy foxglove behaviour so existing client integrations and tests keep working.
"""

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from starlette.requests import Request
from starlette.responses import Response


class HttpMessageError(HTTPException):
    def __init__(self, status_code: int, message: str) -> None:
        self.message = message
        super().__init__(status_code=status_code, detail=message)


class HTTP400(HttpMessageError):
    def __init__(self, message: str = 'Bad request') -> None:
        super().__init__(400, message)


class HTTP403(HttpMessageError):
    def __init__(self, message: str = 'Forbidden') -> None:
        super().__init__(403, message)


class HTTP404(HttpMessageError):
    def __init__(self, message: str = 'Not found') -> None:
        super().__init__(404, message)


class HTTP409(HttpMessageError):
    def __init__(self, message: str = 'Conflict') -> None:
        super().__init__(409, message)


class HTTP422(HttpMessageError):
    def __init__(self, message: str = 'Unprocessable entity') -> None:
        super().__init__(422, message)


async def http_message_error_handler(_: Request, exc: HttpMessageError) -> Response:
    return JSONResponse({'message': exc.message}, status_code=exc.status_code)
