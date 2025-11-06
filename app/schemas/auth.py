"""
Pydantic schemas for authentication endpoints.
"""

from pydantic import BaseModel, Field, EmailStr, field_validator, ConfigDict
from typing import Optional

class TenantRegisterRequest(BaseModel):
    """Request schema for tenant registration."""
    
    tenant_name: str = Field(..., min_length=1, max_length=255, description="Tenant name")
    tenant_identifier: str = Field(..., min_length=1, max_length=100, description="Unique tenant identifier")
    email: EmailStr = Field(..., description="Admin email")
    password: str = Field(..., min_length=8, description="Admin password")
    
    @field_validator("tenant_identifier")
    @classmethod
    def validate_tenant_identifier(cls, v: str) -> str:
        """Validate tenant identifier format."""
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Tenant identifier can only contain alphanumeric characters, hyphens, and underscores")
        return v.lower()

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "tenant_name": "Acme Corporation",
                "tenant_identifier": "acme_corp",
                "email": "admin@acme.com",
                "password": "securepassword123"
            }
        }
    )

class TenantLoginRequest(BaseModel):
    """Request schema for tenant login."""

    tenant_identifier: str = Field(..., description="Tenant identifier")
    email: EmailStr = Field(..., description="Email")
    password: str = Field(..., description="Password")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "tenant_identifier": "acme_corp",
                "email": "admin@acme.com",
                "password": "securepassword123"
            }
        }
    )

class TenantLoginResponse(BaseModel):
    """Response schema for successful tenant login."""

    tenant_id: str
    tenant_name: str
    access_token: str
    token_type: str = "bearer"

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
                "tenant_name": "Acme Corporation",
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer"
            }
        }
    )

class TenantRegisterResponse(BaseModel):
    """Response schema for successful tenant registration."""

    tenant_id: str
    tenant_name: str
    message: str

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
                "tenant_name": "Acme Corporation",
                "message": "Tenant registered successfully. Please login to continue."
            }
        }
    )
