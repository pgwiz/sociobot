"""WSGI & ASGI entrypoint for Render and Gunicorn deployments."""

from bot.main import app
from a2wsgi import ASGIMiddleware

# Standard WSGI callable for 'gunicorn wsgi' or 'gunicorn wsgi:application'
application = ASGIMiddleware(app)

# Expose raw ASGI app for 'gunicorn -k uvicorn.workers.UvicornWorker wsgi:app'
__all__ = ["app", "application"]
