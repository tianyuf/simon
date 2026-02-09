"""WSGI entry point for production deployment."""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from web.app import app


class PrefixMiddleware:
    """WSGI middleware to handle URL prefix (e.g., /simon)."""

    def __init__(self, app, prefix=''):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        if self.prefix:
            environ['SCRIPT_NAME'] = self.prefix
            path_info = environ.get('PATH_INFO', '')
            if path_info.startswith(self.prefix):
                environ['PATH_INFO'] = path_info[len(self.prefix):] or '/'
        return self.app(environ, start_response)


# Get URL prefix from environment variable (set in systemd service)
# Leave empty for root deployment, set to '/simon' for subdirectory
url_prefix = os.environ.get('URL_PREFIX', '')
application = PrefixMiddleware(app, prefix=url_prefix)

if __name__ == "__main__":
    app.run()
