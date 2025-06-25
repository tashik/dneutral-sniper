"""
Web API package for the dNeutral Sniper application.

This package provides the FastAPI-based web API for portfolio management,
including REST endpoints and WebSocket support for real-time updates.
"""

__version__ = "1.0.0"

# Import the FastAPI app from app.py to make it available when importing the package
from .app import app

__all__ = ["app"]
