from fastapi import APIRouter, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from typing import List, Optional
from uuid import UUID
import asyncio
import json

from web_api.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    PortfolioListResponse,
    OptionPositionCreate,
    OptionPositionResponse,
    PnLResponse,
    ErrorResponse
)

router = APIRouter()

# In-memory storage for demo purposes
portfolios_db = {}
pnl_streams = {}


@router.post("/portfolios/", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
async def create_portfolio(portfolio: PortfolioCreate):
    """Create a new portfolio"""
    portfolio_id = str(uuid4())
    portfolio_data = {
        **portfolio.dict(),
        "id": portfolio_id,
        "created_at": "2023-01-01T00:00:00",  # In a real app, use datetime.utcnow()
        "updated_at": "2023-01-01T00:00:00",
        "options": [],
        "total_delta": 0.0,
        "total_gamma": 0.0,
        "total_theta": 0.0,
        "total_vega": 0.0,
        "total_value": 0.0
    }
    portfolios_db[portfolio_id] = portfolio_data
    return portfolio_data


@router.get("/portfolios/", response_model=PortfolioListResponse)
async def list_portfolios(skip: int = 0, limit: int = 100):
    """List all portfolios with pagination"""
    portfolios = list(portfolios_db.values())[skip:skip + limit]
    return {"portfolios": portfolios, "total": len(portfolios)}


@router.get("/portfolios/{portfolio_id}", response_model=PortfolioResponse)
async def get_portfolio(portfolio_id: str):
    """Get a specific portfolio by ID"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    return portfolios_db[portfolio_id]


@router.put("/portfolios/{portfolio_id}", response_model=PortfolioResponse)
async def update_portfolio(portfolio_id: str, portfolio_update: PortfolioUpdate):
    """Update a portfolio"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolios_db[portfolio_id]
    update_data = portfolio_update.dict(exclude_unset=True)
    
    # Update only provided fields
    for field, value in update_data.items():
        if field in portfolio:
            portfolio[field] = value
    
    portfolio["updated_at"] = "2023-01-01T00:00:00"  # In a real app, use datetime.utcnow()
    return portfolio


@router.delete("/portfolios/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_portfolio(portfolio_id: str):
    """Delete a portfolio"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    del portfolios_db[portfolio_id]
    return None


@router.post("/portfolios/{portfolio_id}/options/", response_model=OptionPositionResponse, status_code=status.HTTP_201_CREATED)
async def add_option(portfolio_id: str, option: OptionPositionCreate):
    """Add an option position to a portfolio"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolios_db[portfolio_id]
    option_id = str(uuid4())
    option_data = {
        **option.dict(),
        "id": option_id,
        "created_at": "2023-01-01T00:00:00",  # In a real app, use datetime.utcnow()
        "updated_at": "2023-01-01T00:00:00"
    }
    
    portfolio["options"].append(option_data)
    # In a real app, update Greeks and other calculations here
    
    return option_data


@router.delete("/portfolios/{portfolio_id}/options/{option_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_option(portfolio_id: str, option_id: str):
    """Remove an option position from a portfolio"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolios_db[portfolio_id]
    for i, option in enumerate(portfolio["options"]):
        if option["id"] == option_id:
            portfolio["options"].pop(i)
            # In a real app, update Greeks and other calculations here
            return None
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Option {option_id} not found in portfolio {portfolio_id}"
    )


@router.get("/portfolios/{portfolio_id}/options/", response_model=List[OptionPositionResponse])
async def list_options(portfolio_id: str):
    """List all options in a portfolio"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    return portfolios_db[portfolio_id]["options"]


@router.put("/portfolios/{portfolio_id}/options/{option_id}", response_model=OptionPositionResponse)
async def update_option(
    portfolio_id: str,
    option_id: str,
    option_update: OptionPositionCreate
):
    """Update an option position"""
    if portfolio_id not in portfolios_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio {portfolio_id} not found"
        )
    
    portfolio = portfolios_db[portfolio_id]
    
    # Find and update the option
    for option in portfolio["options"]:
        if option["id"] == option_id:
            update_data = option_update.dict(exclude_unset=True)
            for field, value in update_data.items():
                option[field] = value
            option["updated_at"] = datetime.utcnow().isoformat()
            return option
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Option {option_id} not found in portfolio {portfolio_id}"
    )


@router.websocket("/ws/portfolios/{portfolio_id}/pnl")
async def websocket_pnl(websocket: WebSocket, portfolio_id: str):
    """WebSocket endpoint for real-time PnL updates"""
    if portfolio_id not in portfolios_db:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    await websocket.accept()
    
    if portfolio_id not in pnl_streams:
        pnl_streams[portfolio_id] = set()
    
    pnl_streams[portfolio_id].add(websocket)
    
    try:
        last_pnl = 0.0
        
        while True:
            # Simulate PnL changes based on option positions
            portfolio = portfolios_db[portfolio_id]
            current_pnl = 0.0
            
            # Simple PnL calculation (replace with actual calculation)
            for option in portfolio.get("options", []):
                # This is a placeholder - in a real app, you'd calculate PnL based on market data
                current_pnl += option.get("premium", 0) * option.get("quantity", 0) * 0.01  # Random change
            
            # Generate some historical data points
            now = datetime.utcnow()
            historical_data = [
                {
                    "timestamp": (now - timedelta(minutes=i)).isoformat(),
                    "pnl": current_pnl * (1 - i * 0.01),  # Simulate some variation
                    "underlying_price": 1000 * (1 + i * 0.01)  # Simulated underlying price
                }
                for i in range(10, 0, -1)
            ]
            
            # Send update
            await websocket.send_json({
                "portfolio_id": portfolio_id,
                "current_pnl": current_pnl,
                "pnl_24h": current_pnl - last_pnl if last_pnl != 0 else 0.0,
                "data": historical_data
            })
            
            last_pnl = current_pnl
            await asyncio.sleep(5)  # Send updates every 5 seconds
            
    except WebSocketDisconnect:
        if portfolio_id in pnl_streams and websocket in pnl_streams[portfolio_id]:
            pnl_streams[portfolio_id].remove(websocket)
            if not pnl_streams[portfolio_id]:
                del pnl_streams[portfolio_id]
    except Exception as e:
        print(f"WebSocket error for portfolio {portfolio_id}: {str(e)}")
        if portfolio_id in pnl_streams and websocket in pnl_streams[portfolio_id]:
            pnl_streams[portfolio_id].remove(websocket)
            if not pnl_streams[portfolio_id]:
                del pnl_streams[portfolio_id]
        await websocket.close()
