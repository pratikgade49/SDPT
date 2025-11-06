"""
Pydantic schemas for Sales Data endpoints.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import date as DateType, datetime as DateTimeType

class SalesDataRequest(BaseModel):
    """Request schema for Sales Data insertion."""

    master_id: str = Field(..., description="Reference to Master Data ID")
    date: DateType = Field(..., description="Sales date")
    quantity: float = Field(..., gt=0, description="Quantity sold")
    uom: str = Field(..., min_length=1, max_length=20, description="Unit of Measure")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "master_id": "550e8400-e29b-41d4-a716-446655440000",
                "date": "2024-01-15",
                "quantity": 100.5,
                "uom": "KG"
            }
        }
    )

class SalesDataResponse(BaseModel):
    """Response schema for Sales Data."""
    
    sales_id: str
    tenant_id: str
    master_id: str
    date: DateType
    quantity: float
    uom: str
    created_at: DateTimeType
    created_by: str

class SalesDataBulkRequest(BaseModel):
    """Request schema for bulk Sales Data insertion."""
    
    records: List[SalesDataRequest] = Field(..., min_items=1, max_items=5000, description="Records to insert")

class SalesDataBulkResponse(BaseModel):
    """Response schema for bulk Sales Data insertion."""
    
    success_count: int
    failed_count: int
    inserted_ids: List[str]
    errors: List[Dict[str, Any]] = []

class SalesDataFilterRequest(BaseModel):
    """Request schema for Sales Data filtering."""
    
    date_from: Optional[DateType] = Field(None, description="Start date")
    date_to: Optional[DateType] = Field(None, description="End date")
    master_id: Optional[str] = Field(None, description="Master Data ID filter")
    characteristic_filters: Optional[Dict[str, Any]] = Field(None, description="Filter by Master Data characteristics")
    uom: Optional[str] = Field(None, description="Filter by Unit of Measure")
    quantity_min: Optional[float] = Field(None, ge=0, description="Minimum quantity")
    quantity_max: Optional[float] = Field(None, ge=0, description="Maximum quantity")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=1000, description="Records per page")
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    sort_order: Optional[str] = Field("asc", pattern="^(asc|desc)$", description="Sort order")

class SalesDataListResponse(BaseModel):
    """Response schema for Sales Data list."""
    
    total_count: int
    page: int
    page_size: int
    total_pages: int
    records: List[SalesDataResponse]

class SalesDataAggregationResponse(BaseModel):
    """Response schema for Sales Data aggregation."""
    
    total_quantity: float
    average_quantity: float
    min_quantity: float
    max_quantity: float
    record_count: int
    uom: str
    period: Optional[str] = None
