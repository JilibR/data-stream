from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import datetime
from typing import Optional

class SubscribeStream(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    result: Optional[int] = None
    id: int = 1

class TradeSymbol(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(alias="s")
    price: float = Field(alias="p")
    quantity: float = Field(alias="q")
    timestamp: int = Field(alias="T")
    is_buyer_maker: bool = Field(alias="m")

# Binance send : {"stream": "btcusdt@aggTrade", "data": {...}}
class BinanceMessage(BaseModel):
    stream: str
    data: TradeSymbol
    

