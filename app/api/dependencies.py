"""
API Dependencies.
Handles authentication and authorization for protected endpoints.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any
from app.core.auth_service import AuthService
from app.core.exceptions import AuthenticationException
import logging

logger = logging.getLogger(__name__)

security = HTTPBearer()


async def get_current_tenant(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """
    Dependency to get current authenticated tenant from JWT token.
    
    Args:
        credentials: HTTP Bearer token credentials
        
    Returns:
        Dictionary containing tenant information including database_name
        
    Raises:
        HTTPException: If authentication fails
    """
    try:
        token = credentials.credentials
        payload = AuthService.verify_tenant_token(token)
        
        return {
            "tenant_id": payload.get("tenant_id"),
            "tenant_identifier": payload.get("tenant_identifier"),
            "email": payload.get("email"),
            "database_name": payload.get("database_name")  # Database name for this tenant
        }
        
    except AuthenticationException as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def validate_tenant_access(
    tenant_id: str,
    current_tenant: Dict = Depends(get_current_tenant)
) -> bool:
    """
    Validate that current tenant has access to requested tenant_id.
    
    Args:
        tenant_id: Requested tenant ID
        current_tenant: Current authenticated tenant
        
    Returns:
        True if access is valid
        
    Raises:
        HTTPException: If access is denied
    """
    if current_tenant["tenant_id"] != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to this tenant's data"
        )
    return True

async def get_tenant_database(
    current_tenant: Dict = Depends(get_current_tenant)
) -> Dict[str, Any]:
    """
    Dependency to get and validate tenant database connection.
    
    Args:
        current_tenant: Current authenticated tenant
        
    Returns:
        Dictionary with tenant and database information
        
    Raises:
        HTTPException: If database is not accessible
    """
    try:
        from app.core.database import get_db_manager
        db_manager = get_db_manager()
        
        # Validate connection
        db_manager.validate_tenant_connection(current_tenant["database_name"])
        
        return {
            "tenant_id": current_tenant["tenant_id"],
            "database_name": current_tenant["database_name"],
            "email": current_tenant["email"]
        }
        
    except Exception as e:
        logger.error(f"Database validation failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection unavailable"
        )
