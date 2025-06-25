"""
Portfolio service with PortfolioManager integration.

This service provides a clean interface between the API and the PortfolioManager,
handling data transformation and error handling.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import HTTPException, status

from dneutral_sniper.portfolio_manager import PortfolioManager
from dneutral_sniper.models import OptionType, VanillaOption
from dneutral_sniper.portfolio import Portfolio, PortfolioEventType

from web_api.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    OptionPositionCreate,
    OptionPositionResponse
)

logger = logging.getLogger(__name__)

class PortfolioService:
    """Service for managing portfolios with PortfolioManager integration."""

    def __init__(self, data_dir: str = "data/portfolios", ws_manager=None):
        """Initialize with data directory for portfolio storage and optional WebSocket manager.

        Args:
            data_dir: Directory to store portfolio data
            ws_manager: WebSocket manager for broadcasting updates
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.manager = PortfolioManager(portfolios_dir=str(self.data_dir))
        self.ws_manager = ws_manager
        self.portfolio_cache: Dict[str, Portfolio] = {}

    async def _broadcast_portfolio_update(self, portfolio: Portfolio):
        """Broadcast portfolio update to all connected WebSocket clients."""
        if not self.ws_manager:
            return

        try:
            # Convert portfolio to API response format
            portfolio_data = self._to_api_portfolio(portfolio)
            await self.ws_manager.broadcast({
                'type': 'portfolio_updated',
                'portfolio_id': str(portfolio.id),
                'data': portfolio_data,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.error(f"Error broadcasting portfolio update: {e}", exc_info=True)

    async def _get_portfolio(self, portfolio_id: str) -> Portfolio:
        """Get portfolio from cache or load from disk."""
        if portfolio_id in self.portfolio_cache:
            return self.portfolio_cache[portfolio_id]

        try:
            # Handle the async nature of get_portfolio
            portfolio = await self.manager.get_portfolio(portfolio_id)
            if not portfolio:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Portfolio {portfolio_id} not found"
                )
            self.portfolio_cache[portfolio_id] = portfolio
            return portfolio
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting portfolio {portfolio_id}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve portfolio: {str(e)}"
            )

    def _to_api_portfolio(self, portfolio: Portfolio) -> Dict[str, Any]:
        """Convert Portfolio to API response format."""
        def format_datetime(dt):
            if dt is None:
                return None
            if isinstance(dt, str):
                # Try to parse the string to datetime
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    return None
            if hasattr(dt, 'isoformat'):
                return dt.isoformat()
            return None

        # Get portfolio attributes with safe defaults
        portfolio_id = str(getattr(portfolio, 'id', '')) or f"portfolio_{int(datetime.now().timestamp())}"
        created_at = getattr(portfolio, 'created_at', None)
        updated_at = getattr(portfolio, 'updated_at', None)

        # Process options, ensuring quantity is always positive
        options = []
        for opt in getattr(portfolio, 'options', {}).values():
            option_data = self._to_api_option(opt)
            if option_data and option_data.get('quantity', 0) > 0:  # Only include options with positive quantity
                options.append(option_data)

        return {
            "id": portfolio_id,
            "name": str(getattr(portfolio, 'name', 'Unnamed Portfolio')),
            "underlying": str(getattr(portfolio, 'underlying', 'BTC-USD')),  # Default to BTC-USD if not set
            "description": str(getattr(portfolio, 'description', '')),
            "created_at": format_datetime(created_at),
            "updated_at": format_datetime(updated_at),
            "options": options,
            "total_delta": float(getattr(portfolio, 'total_delta', 0.0) or 0.0),
            "total_gamma": float(getattr(portfolio, 'total_gamma', 0.0) or 0.0),
            "total_theta": float(getattr(portfolio, 'total_theta', 0.0) or 0.0),
            "total_vega": float(getattr(portfolio, 'total_vega', 0.0) or 0.0),
            "total_value": float(getattr(portfolio, 'total_value', 0.0) or 0.0)
        }

    def _to_api_option(self, option: VanillaOption, option_id: str = None) -> Dict[str, Any]:
        """Convert VanillaOption to API response format.

        Args:
            option: The option to convert
            option_id: Optional explicit ID to use (for consistency with test expectations)
        """
        def safe_float(option_attr, attr_name, default=0.0):
            """Safely get and convert attribute to float, handling missing attributes and None values."""
            try:
                value = getattr(option_attr, attr_name, default)
                if value is None:
                    return default
                return float(value)
            except (AttributeError, TypeError, ValueError):
                return default

        def safe_str(value, default=''):
            """Safely convert value to string."""
            return str(value) if value is not None else default

        # Initialize result with defaults
        result = {
            "id": "",
            "symbol": "",
            "option_type": "call",
            "strike": 0.0,
            "expiration": datetime.now(timezone.utc).isoformat(),
            "quantity": 1.0,
            "premium": 0.0,
            "underlying": "UNKNOWN",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "delta": 0.0,
            "gamma": 0.0,
            "theta": 0.0,
            "vega": 0.0,
            "iv": 0.0,
            "mark_price": 0.0
        }

        try:
            # Use the provided ID or generate a consistent one based on option properties
            if option_id is None:
                expiry = getattr(option, 'expiry', datetime.now(timezone.utc))
                underlying = getattr(option, 'underlying', 'UNKNOWN')
                option_type = getattr(option, 'option_type', None)
                option_type_char = option_type.value[0].upper() if option_type else 'C'
                strike = safe_float(option, 'strike')
                option_id = f"{underlying}_{expiry.date()}_{option_type_char}_{strike}"

            # Get quantity, ensuring it's positive
            quantity = abs(float(getattr(option, 'quantity', getattr(option, 'size', 1.0)) or 1.0))

            # Get option type, defaulting to CALL if not set
            option_type = getattr(option, 'option_type', None)
            option_type_str = option_type.value.lower() if hasattr(option_type, 'value') else 'call'

            # Get mark_price once to avoid multiple lookups
            mark_price = safe_float(option, 'mark_price')

            # Get expiration date with fallback
            expiration = getattr(option, 'expiry', None)
            if expiration is None:
                expiration = datetime.now(timezone.utc)
            elif hasattr(expiration, 'isoformat'):
                expiration = expiration.isoformat()

            # Update result with actual values
            result.update({
                "id": str(option_id),
                "symbol": safe_str(getattr(option, 'instrument_name', '')),
                "option_type": option_type_str,
                "strike": safe_float(option, 'strike'),
                "expiration": expiration,
                "quantity": quantity,
                "premium": mark_price,
                "underlying": safe_str(getattr(option, 'underlying', 'UNKNOWN')),
                "mark_price": mark_price
            })
        except Exception as e:
            logger.warning(f"Error processing option data: {e}")
            # Keep the default values we already set
            pass

        # Try to get Greeks if they exist
        greeks = getattr(option, 'greeks', {})
        if isinstance(greeks, dict):
            for greek in ['delta', 'gamma', 'theta', 'vega', 'iv']:
                if greek in greeks and greeks[greek] is not None:
                    result[greek] = float(greeks[greek])

        return result

    def _to_option_type(self, option_type_str: str) -> OptionType:
        """Convert option type string to OptionType enum."""
        try:
            return OptionType[option_type_str.upper()]
        except KeyError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid option type: {option_type_str}. Must be 'call' or 'put'."
            ) from e

    async def create_portfolio(self, portfolio_data: PortfolioCreate) -> PortfolioResponse:
        """Create a new portfolio."""
        try:
            # Create portfolio using PortfolioManager with default values
            portfolio_id, portfolio = await self.manager.create_portfolio(
                underlying="BTC",  # Default underlying
                initial_balance=0.0  # Start with 0 balance
            )

            # Set additional attributes from PortfolioCreate
            portfolio.name = portfolio_data.name
            portfolio.description = portfolio_data.description or ""
            portfolio.created_at = datetime.now(timezone.utc)
            portfolio.updated_at = datetime.now(timezone.utc)

            # Save the portfolio with updated attributes
            await self.manager._save_portfolio(portfolio_id, portfolio)

            # Add to cache
            self.portfolio_cache[str(portfolio.id)] = portfolio

            # Broadcast the new portfolio
            await self._broadcast_portfolio_update(portfolio)

            return self._to_api_portfolio(portfolio)

        except Exception as e:
            logger.error(f"Failed to create portfolio: {str(e)}", exc_info=True)
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            detail = f"Failed to create portfolio: {str(e)}"
            if isinstance(e, ValueError) and "already exists" in str(e):
                status_code = status.HTTP_409_CONFLICT
            raise HTTPException(status_code=status_code, detail=detail) from e

    async def get_portfolio(self, portfolio_id: str) -> Dict[str, Any]:
        """Get a portfolio by ID."""
        portfolio = await self._get_portfolio(portfolio_id)
        return self._to_api_portfolio(portfolio)

    async def list_portfolios(self) -> List[Dict[str, Any]]:
        """List all portfolios.

        Returns:
            List of portfolio dictionaries in API response format

        Raises:
            HTTPException: If there's an error listing portfolios
        """
        try:
            logger.info(f"Listing portfolios. Current cache size: {len(self.portfolio_cache)}")
            logger.info(f"Data directory: {self.data_dir}")
            logger.info(f"Data directory exists: {self.data_dir.exists()}")
            
            # First check if we have any cached portfolios
            if not self.portfolio_cache:
                try:
                    logger.info("No cached portfolios, loading from manager...")
                    portfolio_ids = await self.manager.list_portfolios()
                    logger.info(f"Manager returned {len(portfolio_ids)} portfolio IDs: {portfolio_ids}")
                    
                    # Load each portfolio by ID and add to cache
                    for portfolio_id in portfolio_ids:
                        try:
                            logger.info(f"Loading portfolio with ID: {portfolio_id}")
                            portfolio = await self.manager.get_portfolio(portfolio_id)
                            if portfolio:
                                logger.info(f"Successfully loaded portfolio {portfolio_id}, adding to cache")
                                self.portfolio_cache[portfolio_id] = portfolio
                            else:
                                logger.warning(f"Manager returned None for portfolio ID: {portfolio_id}")
                        except Exception as e:
                            logger.error(f"Error loading portfolio {portfolio_id}: {str(e)}", exc_info=True)

                    if not portfolio_ids:
                        logger.info("No portfolios found via manager, trying direct file scan...")
                        if self.data_dir.exists():
                            portfolio_files = list(self.data_dir.glob("portfolio_*.json"))
                            logger.info(f"Found {len(portfolio_files)} portfolio files directly: {portfolio_files}")
                            for pf in portfolio_files:
                                try:
                                    # Extract portfolio ID from filename (portfolio_<id>.json)
                                    portfolio_id = pf.stem.split('_', 1)[1]
                                    logger.info(f"Attempting to load portfolio from file: {pf}, extracted ID: {portfolio_id}")
                                    portfolio = await self.manager.get_portfolio(portfolio_id)
                                    if portfolio:
                                        self.portfolio_cache[portfolio_id] = portfolio
                                        logger.info(f"Successfully loaded portfolio into cache: {portfolio_id}")
                                    else:
                                        logger.warning(f"Manager returned None for portfolio ID: {portfolio_id}")
                                except Exception as e:
                                    logger.error(f"Failed to load portfolio from {pf}: {str(e)}", exc_info=True)
                        else:
                            logger.error(f"Data directory does not exist: {self.data_dir}")
                except Exception as e:
                    logger.error(f"Failed to list portfolio IDs: {str(e)}", exc_info=True)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Failed to list portfolio IDs: {str(e)}"
                    )
            
            # If we have any cached portfolios, convert them to API format
            result = []
            if self.portfolio_cache:
                logger.info(f"Converting {len(self.portfolio_cache)} cached portfolios to API format")
                try:
                    for portfolio in self.portfolio_cache.values():
                        try:
                            api_portfolio = self._to_api_portfolio(portfolio)
                            result.append(api_portfolio)
                            logger.info(f"Successfully converted portfolio {getattr(portfolio, 'id', 'unknown')} to API format")
                        except Exception as e:
                            portfolio_id = getattr(portfolio, 'id', 'unknown')
                            logger.error(f"Failed to convert portfolio {portfolio_id} to API format: {str(e)}", exc_info=True)
                            continue
                except Exception as e:
                    logger.error(f"Error converting portfolios to API format: {str(e)}", exc_info=True)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Error converting portfolios to API format: {str(e)}"
                    )
            
            logger.info(f"Returning {len(result)} portfolios")
            return result
            
        except HTTPException:
            # Re-raise HTTP exceptions as-is
            logger.error("HTTPException in list_portfolios", exc_info=True)
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error in list_portfolios: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred while listing portfolios: {str(e)}"
            ) from e

    async def update_portfolio(
        self, portfolio_id: str, portfolio_data: PortfolioUpdate
    ) -> PortfolioResponse:
        """Update an existing portfolio."""
        try:
            # Get the portfolio
            portfolio = await self._get_portfolio(portfolio_id)

            # Update fields
            if portfolio_data.name is not None:
                portfolio.name = portfolio_data.name
            if portfolio_data.description is not None:
                portfolio.description = portfolio_data.description

            portfolio.updated_at = datetime.now(timezone.utc)

            # Save changes
            await self.manager.save_portfolio(portfolio)

            # Update cache
            self.portfolio_cache[str(portfolio.id)] = portfolio

            # Broadcast the update
            await self._broadcast_portfolio_update(portfolio)

            return self._to_api_portfolio(portfolio)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to update portfolio {portfolio_id}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update portfolio: {str(e)}"
            )

    async def delete_portfolio(self, portfolio_id: str) -> None:
        """Delete a portfolio."""
        try:
            # Get portfolio before deletion for broadcasting
            portfolio = await self._get_portfolio(portfolio_id)

            # Delete from cache first
            if portfolio_id in self.portfolio_cache:
                del self.portfolio_cache[portfolio_id]

            # Delete from disk
            await self.manager.delete_portfolio(portfolio_id)

            # Broadcast deletion
            if self.ws_manager:
                await self.ws_manager.broadcast({
                    'type': 'portfolio_deleted',
                    'portfolio_id': portfolio_id,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })

            logger.info(f"Successfully deleted portfolio {portfolio_id}")

        except HTTPException:
            logger.warning(f"Attempted to delete non-existent portfolio: {portfolio_id}")
            raise
        except Exception as e:
            logger.error(f"Failed to delete portfolio {portfolio_id}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete portfolio: {str(e)}"
            )

    async def add_option(
        self,
        portfolio_id: str,
        data: OptionPositionCreate
    ) -> Dict[str, Any]:
        """Add an option to a portfolio."""
        try:
            # Get the portfolio
            portfolio = await self._get_portfolio(portfolio_id)

            # Create the option
            option = VanillaOption(
                instrument_name=data.symbol,
                option_type=self._to_option_type(data.option_type),
                strike=data.strike,
                expiry=data.expiration,  # Using expiry instead of expiration
                underlying=data.underlying,
                quantity=data.quantity,  # Changed from size to quantity
                mark_price=data.premium or 0.0  # Ensure mark_price is not None
            )

            # Add the option to the portfolio
            if not hasattr(portfolio, 'options'):
                portfolio.options = {}

            # Generate a consistent ID for the option
            option_id = f"{data.underlying}_{data.expiration.date()}_{data.option_type[0].upper()}_{data.strike}"

            # Store the option with the generated ID
            portfolio.options[option_id] = option

            # Set the quantity on the option object
            option.quantity = data.quantity

            # Update portfolio metrics if they exist
            if hasattr(portfolio, 'total_delta'):
                portfolio.total_delta = (portfolio.total_delta or 0.0) + (getattr(option, 'delta', 0.0) * data.quantity)
            if hasattr(portfolio, 'total_vega'):
                portfolio.total_vega = (portfolio.total_vega or 0.0) + (getattr(option, 'vega', 0.0) * data.quantity)
            if hasattr(portfolio, 'total_theta'):
                portfolio.total_theta = (portfolio.total_theta or 0.0) + (getattr(option, 'theta', 0.0) * data.quantity)
            if hasattr(portfolio, 'total_gamma'):
                portfolio.total_gamma = (portfolio.total_gamma or 0.0) + (getattr(option, 'gamma', 0.0) * data.quantity)

            # Update timestamps
            portfolio.updated_at = datetime.now(timezone.utc)

            # Save the updated portfolio
            await self.manager.save_portfolio(portfolio)

            # Update cache
            self.portfolio_cache[portfolio_id] = portfolio

            # Return the added option with the generated ID
            return self._to_api_option(option, option_id=option_id)

        except HTTPException:
            logger.error(f"HTTP error adding option to portfolio {portfolio_id}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Failed to add option to portfolio {portfolio_id}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to add option: {str(e)}"
            )

    async def remove_option(self, portfolio_id: str, option_id: str) -> None:
        """Remove an option from a portfolio."""
        try:
            # Get the portfolio
            portfolio = await self._get_portfolio(portfolio_id)

            # Check if the option exists
            if not hasattr(portfolio, 'options') or option_id not in portfolio.options:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Option {option_id} not found in portfolio {portfolio_id}"
                )

            # Remove the option
            option = portfolio.options.pop(option_id)

            # Update portfolio metrics if they exist
            if hasattr(portfolio, 'total_delta'):
                portfolio.total_delta = (portfolio.total_delta or 0.0) - (getattr(option, 'delta', 0.0) * option.quantity)
            if hasattr(portfolio, 'total_vega'):
                portfolio.total_vega = (portfolio.total_vega or 0.0) - (getattr(option, 'vega', 0.0) * option.quantity)
            if hasattr(portfolio, 'total_theta'):
                portfolio.total_theta = (portfolio.total_theta or 0.0) - (getattr(option, 'theta', 0.0) * option.quantity)
            if hasattr(portfolio, 'total_gamma'):
                portfolio.total_gamma = (portfolio.total_gamma or 0.0) - (getattr(option, 'gamma', 0.0) * option.quantity)

            # Update timestamps
            portfolio.updated_at = datetime.now(timezone.utc)

            # Save the updated portfolio
            await self.manager.save_portfolio(portfolio)

            # Update cache
            self.portfolio_cache[portfolio_id] = portfolio

        except HTTPException:
            logger.error(f"HTTP error removing option {option_id} from portfolio {portfolio_id}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Failed to remove option {option_id} from portfolio {portfolio_id}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to remove option: {str(e)}"
            )

# Create a singleton instance of the service
portfolio_service = PortfolioService()
