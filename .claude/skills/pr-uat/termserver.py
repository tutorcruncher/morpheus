"""Terminado + Tornado PTY server for the /pr-uat terminal harness.

Serves a real bash PTY over a websocket and renders it in xterm.js (index.html).
Each websocket connection spawns a FRESH shell running $TSH_DIR/run_case.sh, so a
new browser navigation re-runs whatever case was last written into that file.
setup-terminal.sh starts it with TSH_DIR and TSH_PORT for the checkout it belongs to.
"""

import os

import tornado.ioloop
import tornado.web
from terminado import TermSocket, UniqueTermManager

DIR = os.environ['TSH_DIR']
PORT = int(os.environ['TSH_PORT'])
term_manager = UniqueTermManager(shell_command=['bash', f'{DIR}/run_case.sh'])


class Index(tornado.web.RequestHandler):
    def get(self):
        with open(f'{DIR}/index.html') as f:
            self.write(f.read())


app = tornado.web.Application(
    [
        (r'/websocket', TermSocket, {'term_manager': term_manager}),
        (r'/', Index),
        (r'/(xterm\.js)', tornado.web.StaticFileHandler, {'path': DIR}),
        (r'/(xterm\.css)', tornado.web.StaticFileHandler, {'path': DIR}),
    ]
)

if __name__ == '__main__':
    app.listen(PORT, address='127.0.0.1')
    print(f'termserver on 127.0.0.1:{PORT} for {DIR}')
    tornado.ioloop.IOLoop.current().start()
