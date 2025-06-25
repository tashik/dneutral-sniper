"""
Portfolio management API endpoints.

This module provides FastAPI endpoints for managing portfolios and options,
including WebSocket support for real-time PnL updates.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any

from fastapi import APIRouter, HTTPException, status, WebSocket, WebSocketDisconnect, Depends, Request
from fastapi.encoders import jsonable_encoder
from typing import Annotated

from web_api.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    OptionPositionCreate,
    OptionPositionResponse,
    PnLDataPoint,
    PnLResponse
)
from web_api.services.portfolio_service_v2 import PortfolioService

# Dependency to get the portfolio service from the app state
async def get_portfolio_service(request: Request) -> PortfolioService:
    return request.app.state.portfolio_service

# Set up logging
logger = logging.getLogger(__name__)

# Create API router
router = APIRouter()

# Track active WebSocket connections
active_connections: Dict[str, Set[WebSocket]] = {}


@router.get("/portfolios/")
async def list_portfolios_endpoint(
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> Dict[str, Any]:
    """List all portfolios"""
    try:
        portfolios = await portfolio_service.list_portfolios()
        return {"data": portfolios}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to list portfolios: " + str(e), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/portfolios/", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
async def create_portfolio_endpoint(
    portfolio: PortfolioCreate,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> Dict[str, Any]:
    """Create a new portfolio"""
    try:
        return await portfolio_service.create_portfolio(portfolio)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create portfolio: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create portfolio"
        )


@router.get("/portfolios/{portfolio_id}", response_model=PortfolioResponse)
async def get_portfolio_endpoint(
    portfolio_id: str,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> Dict[str, Any]:
    """Get a portfolio by ID"""
    try:
        return await portfolio_service.get_portfolio(portfolio_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get portfolio {portfolio_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get portfolio {portfolio_id}"
        )


@router.put("/portfolios/{portfolio_id}", response_model=PortfolioResponse)
async def update_portfolio_endpoint(
    portfolio_id: str,
    portfolio_update: PortfolioUpdate,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> Dict[str, Any]:
    """Update a portfolio"""
    try:
        return await portfolio_service.update_portfolio(portfolio_id, portfolio_update)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update portfolio {portfolio_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update portfolio {portfolio_id}"
        )


@router.delete("/portfolios/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_portfolio_endpoint(
    portfolio_id: str,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> None:
    """Delete a portfolio"""
    try:
        await portfolio_service.delete_portfolio(portfolio_id)
        # Clean up any active WebSocket connections
        if portfolio_id in active_connections:
            for websocket in active_connections[portfolio_id]:
                await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
            del active_connections[portfolio_id]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete portfolio {portfolio_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete portfolio {portfolio_id}"
        )


@router.post(
    "/portfolios/{portfolio_id}/options/",
    response_model=OptionPositionResponse,
    status_code=status.HTTP_201_CREATED
)
async def add_option_endpoint(
    portfolio_id: str,
    option: OptionPositionCreate,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> Dict[str, Any]:
    """Add an option to a portfolio"""
    try:
        return await portfolio_service.add_option(portfolio_id, option)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to add option to portfolio {portfolio_id}: {str(e)}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add option to portfolio {portfolio_id}"
        )


@router.get(
    "/portfolios/{portfolio_id}/options/",
    response_model=List[OptionPositionResponse]
)
async def list_options(
    portfolio_id: str,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> List[Dict[str, Any]]:
    """List all options in a portfolio"""
    try:
        portfolio = await portfolio_service.get_portfolio(portfolio_id)
        return portfolio.get("options", [])
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to list options for portfolio {portfolio_id}: {str(e)}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list options for portfolio {portfolio_id}"
        )


@router.put(
    "/portfolios/{portfolio_id}/options/{option_id}",
    response_model=OptionPositionResponse
)
async def update_option_endpoint(
    portfolio_id: str,
    option_id: str,
    option_update: OptionPositionCreate,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> Dict[str, Any]:
    """Update an option position"""
    try:
        # In our current service, we don't have a direct update method
        # So we'll remove and re-add the option
        await portfolio_service.remove_option(portfolio_id, option_id)
        return await portfolio_service.add_option(portfolio_id, option_update)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to update option {option_id} in portfolio {portfolio_id}: {str(e)}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update option {option_id} in portfolio {portfolio_id}"
        )


@router.delete(
    "/portfolios/{portfolio_id}/options/{option_id}",
    status_code=status.HTTP_204_NO_CONTENT
)
async def remove_option_endpoint(
    portfolio_id: str,
    option_id: str,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
) -> None:
    """Remove an option from a portfolio"""
    try:
        await portfolio_service.remove_option(portfolio_id, option_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to remove option {option_id} from portfolio {portfolio_id}: {str(e)}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove option {option_id} from portfolio {portfolio_id}"
        )


@router.websocket("/ws/portfolios/{portfolio_id}/pnl")
async def websocket_pnl(
    websocket: WebSocket,
    portfolio_id: str,
    portfolio_service: Annotated[PortfolioService, Depends(get_portfolio_service)]
):
    """WebSocket endpoint for real-time PnL updates"""
    try:
        # Verify portfolio exists
        await portfolio_service.get_portfolio(portfolio_id)
        await websocket.accept()

        # Add to active connections
        if portfolio_id not in active_connections:
            active_connections[portfolio_id] = set()
        active_connections[portfolio_id].add(websocket)

        try:
            while True:
                # Get current portfolio data (which includes PnL)
                portfolio = await portfolio_service.get_portfolio(portfolio_id)

                # Create PnL response
                pnl_data = {
                    "portfolio_id": portfolio_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "total_value": portfolio.get("total_value", 0.0),
                    "delta": portfolio.get("total_delta", 0.0),
                    "gamma": portfolio.get("total_gamma", 0.0),
                    "theta": portfolio.get("total_theta", 0.0),
                    "vega": portfolio.get("total_vega", 0.0)
                }

                # Send update to client
                await websocket.send_json(jsonable_encoder(pnl_data))

                # Wait before next update
                await asyncio.sleep(5)

        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected for portfolio {portfolio_id}")

    except HTTPException as e:
        logger.error(f"WebSocket connection error for portfolio {portfolio_id}: {str(e)}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket for portfolio {portfolio_id}: {str(e)}")
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
    finally:
        # Clean up connection
        if portfolio_id in active_connections and websocket in active_connections[portfolio_id]:
            active_connections[portfolio_id].remove(websocket)
            if not active_connections[portfolio_id]:
                del active_connections[portfolio_id]
