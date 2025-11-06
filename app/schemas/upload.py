"""
Pydantic schemas for Excel upload endpoints.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime

class ExcelUploadRequest(BaseModel):
    """Request schema for Excel file upload."""

    upload_type: str = Field(..., pattern="^(master_data|sales_data)$", description="Type of data being uploaded")
    catalogue_id: Optional[str] = Field(None, description="Field catalogue ID for master data validation (required for master_data)")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "upload_type": "master_data",
                "catalogue_id": "550e8400-e29b-41d4-a716-446655440000"
            }
        }
    )

class ExcelUploadResponse(BaseModel):
    """Response schema for Excel upload."""

    upload_id: str
    tenant_id: str
    upload_type: str
    file_name: str
    total_rows: int
    success_count: int
    failed_count: int
    status: str
    errors: List[Dict[str, Any]] = []
    uploaded_at: datetime
    uploaded_by: str

class UploadProgressResponse(BaseModel):
    """Response schema for upload progress."""

    upload_id: str
    status: str  # "processing", "completed", "failed"
    progress_percentage: int
    total_rows: int
    processed_rows: int
    success_count: int
    failed_count: int
    errors: List[Dict[str, Any]] = []
    started_at: datetime
    completed_at: Optional[datetime] = None

class UploadHistoryResponse(BaseModel):
    """Response schema for upload history."""

    upload_id: str
    tenant_id: str
    upload_type: str
    file_name: str
    total_rows: int
    success_count: int
    failed_count: int
    status: str
    uploaded_at: datetime
    uploaded_by: str
