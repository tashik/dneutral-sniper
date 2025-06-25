import asyncio
import logging
from fastapi import FastAPI, WebSocket, HTTPException, status, Request
from fastapi.websockets import WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional, Callable, AsyncGenerator
import uvicorn
from datetime import datetime
import json
import traceback
import logging

# Import API routers and services
from web_api.api.v1.endpoints.portfolios_v2 import router as portfolios_router
from web_api.schemas.portfolio import ErrorResponse
from web_api.services.portfolio_service_v2 import PortfolioService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager for FastAPI application."""
    # Startup logic
    logger.info("Starting up...")
    
    # Initialize the portfolio manager and load existing portfolios
    try:
        await portfolio_service.manager.initialize()
        logger.info("Successfully loaded portfolios from disk")
    except Exception as e:
        logger.error(f"Failed to load portfolios: {e}", exc_info=True)
    
    yield  # Application runs here
    
    # Shutdown logic
    logger.info("Shutting down...")
    await portfolio_service.manager.close()

app = FastAPI(
    title="Portfolio Management API",
    description="API for portfolio management and real-time monitoring",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# Add middleware to handle OPTIONS requests
@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

# Add CORS middleware as well for additional coverage
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Include API routers
app.include_router(
    portfolios_router,
    prefix="/api/v1",
    tags=["portfolios"],
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"model": ErrorResponse},
    },
)

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(
            ErrorResponse(
                detail=exc.detail,
                code=exc.status_code,
                metadata={"path": request.url.path}
            ).dict()
        ),
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder(
            ErrorResponse(
                detail="Validation error",
                code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                metadata={"errors": exc.errors(), "path": request.url.path}
            ).dict()
        ),
    )

# Health check endpoint
@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Portfolio Management API is running",
        "version": "1.0.0",
        "docs": "/api/docs",
        "redoc": "/api/redoc"
    }

# Test endpoint to verify CORS is working
@app.get("/test-cors")
async def test_cors():
    return {"message": "CORS test successful"}

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.client_counter = 0
        self.portfolio_service = None

    def set_portfolio_service(self, portfolio_service: PortfolioService):
        """Set the portfolio service for broadcasting updates."""
        self.portfolio_service = portfolio_service

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]

    async def broadcast(self, message: Dict[str, Any]):
        """Send a message to all connected clients."""
        if not self.active_connections:
            return

        # Convert message to JSON string once
        message_json = json.dumps(message)
        
        # Create tasks for sending messages concurrently
        tasks = []
        for connection in self.active_connections.values():
            try:
                # Create a task for each send operation
                task = asyncio.create_task(connection.send_text(message_json))
                tasks.append(task)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")
        
        # Wait for all send operations to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def send_to_client(self, client_id: str, message: Dict[str, Any]):
        """Send a message to a specific client."""
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
            except Exception as e:
                logger.error(f"Error sending to client {client_id}: {e}")
                # Remove the client if there's an error
                self.disconnect(client_id)

    def generate_client_id(self) -> str:
        """Generate a unique client ID."""
        self.client_counter += 1
        return f"client_{self.client_counter}"

# Initialize WebSocket manager and portfolio service
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(project_root, 'data', 'portfolios')

# First create the manager
manager = ConnectionManager()

# Then create the portfolio service with the manager as ws_manager
portfolio_service = PortfolioService(data_dir=data_dir, ws_manager=manager)

# Set the portfolio service on the manager
manager.set_portfolio_service(portfolio_service)

# Make portfolio service available to the app
app.state.portfolio_service = portfolio_service

# Include API routers
app.include_router(portfolios_router, prefix="/api/v1", tags=["portfolios"])

# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    client_id = manager.generate_client_id()
    await manager.connect(websocket, client_id)
    logger.info(f"Client connected: {client_id}")
    
    try:
        while True:
            # Wait for any message from client
            data = await websocket.receive_text()
            
            try:
                # Parse the incoming message
                message = json.loads(data)
                
                # Handle different message types
                if message.get("type") == "ping":
                    # Respond to ping with pong
                    await manager.send_to_client(
                        client_id,
                        {"type": "pong", "timestamp": datetime.now().isoformat()}
                    )
                
                # Add more message handlers here as needed
                
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON received from client {client_id}")
            except Exception as e:
                logger.error(f"Error processing message from client {client_id}: {e}")
    
    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {client_id}")
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}")
    finally:
        manager.disconnect(client_id)

# The app is now imported and used in app.py
# This allows the app to be run with: python -m web_api.app
# or with uvicorn directly: uvicorn web_api.app:app --reload
