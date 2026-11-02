"""Local stand-in for Mandrill and MessageBird, so /pr-uat can run the real provider send path.

Serves the routes in tests/dummy_server.py over HTTP, so the behaviour matches the test suite, including
its failure triggers: a Mandrill subject of __502__, __500__, __500_nginx__, __slow__ or __connect_timeout__.
Every request is appended to provider-requests.jsonl in --dir: that file is the evidence of what the
provider would have received.
"""

import argparse
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required=True, type=Path)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--checkout', required=True, type=Path)
    args = parser.parse_args()

    sys.path.insert(0, str(args.checkout / 'tests'))
    from dummy_server import DummyState, make_handler

    handler = make_handler(DummyState())
    log_path = args.dir / 'provider-requests.jsonl'

    class Handler(BaseHTTPRequestHandler):
        def _serve(self) -> None:
            body = self.rfile.read(int(self.headers.get('Content-Length') or 0))
            request = httpx.Request(self.command, f'http://fake{self.path}', content=body, headers=dict(self.headers))
            try:
                response = handler(request)
            except httpx.TimeoutException:
                # Hold the connection past the client's timeout rather than answer, like a stuck provider.
                time.sleep(35)
                return
            entry = {'method': self.command, 'path': self.path, 'body': body.decode(), 'status': response.status_code}
            with log_path.open('a') as f:
                f.write(json.dumps(entry) + '\n')
            self.send_response(response.status_code)
            self.send_header('Content-Type', response.headers.get('content-type', 'text/plain'))
            self.send_header('Content-Length', str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = do_POST = _serve

        def log_message(self, format: str, *log_args) -> None:
            print(format % log_args, flush=True)

    ThreadingHTTPServer(('127.0.0.1', args.port), Handler).serve_forever()


if __name__ == '__main__':
    main()
