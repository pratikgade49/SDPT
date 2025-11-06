"""
Standardized API response handler module.
Provides consistent response format across all endpoints.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class ResponseMetadata(BaseModel):
    """Metadata for API responses."""
    timestamp: str
    status_code: int

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "timestamp": "2024-01-15T10:30:45.123456",
                "status_code": 200
            }
        }
    )

class PaginationMetadata(BaseModel):
    """Pagination metadata for list responses."""
    page: int
    page_size: int
    total_count: int
    total_pages: int

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "page": 1,
                "page_size": 50,
                "total_count": 150,
                "total_pages": 3
            }
        }
    )

class SuccessResponse(BaseModel):
    """Standard success response format."""
    success: bool = True
    data: Optional[Any] = None
    metadata: ResponseMetadata

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "data": {"id": "123", "name": "Example"},
                "metadata": {
                    "timestamp": "2024-01-15T10:30:45.123456",
                    "status_code": 200
                }
            }
        }
    )

class ListResponse(BaseModel):
    """Standard list response format with pagination."""
    success: bool = True
    data: List[Any]
    pagination: PaginationMetadata
    metadata: ResponseMetadata

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "data": [{"id": "1", "name": "Item 1"}],
                "pagination": {
                    "page": 1,
                    "page_size": 50,
                    "total_count": 100,
                    "total_pages": 2
                },
                "metadata": {
                    "timestamp": "2024-01-15T10:30:45.123456",
                    "status_code": 200
                }
            }
        }
    )

class ErrorResponse(BaseModel):
    """Standard error response format."""
    success: bool = False
    error: Dict[str, Any]
    metadata: ResponseMetadata

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": False,
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Invalid input data",
                    "details": {}
                },
                "metadata": {
                    "timestamp": "2024-01-15T10:30:45.123456",
                    "status_code": 400
                }
            }
        }
    )

class ResponseHandler:
    """Utility class for generating standardized responses."""
    
    @staticmethod
    def success(
        data: Any = None,
        status_code: int = 200
    ) -> Dict[str, Any]:
        """
        Create a success response.
        
        Args:
            data: Response data
            status_code: HTTP status code
            
        Returns:
            Standardized success response dictionary
        """
        return {
            "success": True,
            "data": data,
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "status_code": status_code
            }
        }
    
    @staticmethod
    def list_response(
        data: List[Any],
        page: int,
        page_size: int,
        total_count: int,
        status_code: int = 200
    ) -> Dict[str, Any]:
        """
        Create a list response with pagination.
        
        Args:
            data: List of records
            page: Current page number
            page_size: Records per page
            total_count: Total number of records
            status_code: HTTP status code
            
        Returns:
            Standardized list response dictionary
        """
        total_pages = (total_count + page_size - 1) // page_size
        
        return {
            "success": True,
            "data": data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages
            },
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "status_code": status_code
            }
        }
    
    @staticmethod
    def error(
        code: str,
        message: str,
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create an error response.
        
        Args:
            code: Error code
            message: Error message
            status_code: HTTP status code
            details: Additional error details
            
        Returns:
            Standardized error response dictionary
        """
        return {
            "success": False,
            "error": {
                "code": code,
                "message": message,
                "details": details or {}
            },
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "status_code": status_code
            }
        }
