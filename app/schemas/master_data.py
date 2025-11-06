"""
Pydantic schemas for Master Data endpoints.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime

class MasterDataRequest(BaseModel):
    """Request schema for Master Data insertion."""

    data: Dict[str, Any] = Field(..., description="Master data fields based on Field Catalogue")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "data": {
                    "product": "PROD001",
                    "product_category": "Electronics",
                    "product_hierarchy": "Level1/Electronics",
                    "customer": "CUST001",
                    "customer_region": "North",
                    "location": "LOC001",
                    "location_region": "North",
                    "plant": "PLANT001"
                }
            }
        }
    )

class MasterDataResponse(BaseModel):
    """Response schema for Master Data."""

    master_id: str
    tenant_id: str
    data: Dict[str, Any]
    created_at: datetime
    created_by: str
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "master_id": "123e4567-e89b-12d3-a456-426614174000",
                "tenant_id": "tenant_123",
                "data": {
                    "product": "PROD001",
                    "product_category": "Electronics",
                    "product_hierarchy": "Level1/Electronics",
                    "customer": "CUST001",
                    "customer_region": "North",
                    "location": "LOC001",
                    "location_region": "North",
                    "plant": "PLANT001"
                },
                "created_at": "2024-01-01T00:00:00",
                "created_by": "user@example.com",
                "updated_at": None,
                "updated_by": None
            }
        }
    )

class MasterDataBulkRequest(BaseModel):
    """Request schema for bulk Master Data insertion."""
    
    records: List[MasterDataRequest] = Field(..., min_items=1, max_items=1000, description="Records to insert")

class MasterDataBulkResponse(BaseModel):
    """Response schema for bulk Master Data insertion."""
    
    success_count: int
    failed_count: int
    inserted_ids: List[str]
    errors: List[Dict[str, Any]] = []

class MasterDataFilterRequest(BaseModel):
    """Request schema for Master Data filtering."""
    
    filters: Optional[Dict[str, Any]] = Field(None, description="Filter criteria")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=1000, description="Records per page")
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    sort_order: Optional[str] = Field("asc", pattern="^(asc|desc)$", description="Sort order")

class MasterDataUpdateRequest(BaseModel):
    """Request schema for Master Data update."""
    
    data: Dict[str, Any] = Field(..., description="Fields to update")

class MasterDataListResponse(BaseModel):
    """Response schema for Master Data list."""
    
    total_count: int
    page: int
    page_size: int
    total_pages: int
    records: List[MasterDataResponse]
