# Portfolio Management API

A FastAPI-based backend service for managing option portfolios with real-time PnL tracking.

## Features

- **Portfolio Management**: Create, read, update, and delete portfolios
- **Option Positions**: Add, update, and remove option positions within portfolios
- **Real-time PnL**: WebSocket endpoint for real-time PnL updates
- **RESTful API**: Standard HTTP endpoints for all operations
- **Type Safety**: Built with Pydantic for request/response validation

## Prerequisites

- Python 3.8+
- pip (Python package manager)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/portfolio-manager.git
   cd portfolio-manager/web_api
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the API

Start the development server:

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

## API Documentation

- **Swagger UI**: `http://localhost:8000/api/docs`
- **ReDoc**: `http://localhost:8000/api/redoc`
- **OpenAPI Schema**: `http://localhost:8000/api/openapi.json`

## Environment Variables

Create a `.env` file in the project root with the following variables:

```env
# Server
HOST=0.0.0.0
PORT=8000
DEBUG=true

# CORS (comma-separated origins)
CORS_ORIGINS=http://localhost:3000,http://localhost:5173
```

## Project Structure

```
web_api/
├── api/
│   └── v1/
│       ├── endpoints/         # API route handlers
│       │   └── portfolios.py
│       └── __init__.py
├── schemas/                  # Pydantic models
│   └── portfolio.py
├── .env.example             # Example environment variables
├── main.py                  # FastAPI application
├── README.md                # This file
└── requirements.txt         # Project dependencies
```

## API Endpoints

### Portfolios

- `GET /api/v1/portfolios/` - List all portfolios
- `POST /api/v1/portfolios/` - Create a new portfolio
- `GET /api/v1/portfolios/{portfolio_id}` - Get portfolio details
- `PUT /api/v1/portfolios/{portfolio_id}` - Update a portfolio
- `DELETE /api/v1/portfolios/{portfolio_id}` - Delete a portfolio

### Options

- `GET /api/v1/portfolios/{portfolio_id}/options/` - List all options in a portfolio
- `POST /api/v1/portfolios/{portfolio_id}/options/` - Add an option to a portfolio
- `PUT /api/v1/portfolios/{portfolio_id}/options/{option_id}` - Update an option
- `DELETE /api/v1/portfolios/{portfolio_id}/options/{option_id}` - Remove an option

### WebSocket

- `ws://localhost:8000/ws/portfolios/{portfolio_id}/pnl` - Real-time PnL updates

## Development

1. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

2. Run tests:
   ```bash
   pytest
   ```

3. Run with auto-reload:
   ```bash
   uvicorn main:app --reload
   ```

## License

MIT
