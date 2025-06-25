from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from uuid import UUID, uuid4

class OptionPosition(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    symbol: str
    option_type: str  # 'call' or 'put'
    strike: float
    expiration: datetime
    quantity: int
    premium: float
    created_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))
    updated_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

class Portfolio(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))
    updated_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))
    options: List[OptionPosition] = []

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

# In-memory database (replace with a real database in production)
db: dict[UUID, Portfolio] = {}
