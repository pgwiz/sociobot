"""Compatibility entrypoint for Render 'gunicorn your_application.wsgi' start command."""

from wsgi import app, application

__all__ = ["app", "application"]
