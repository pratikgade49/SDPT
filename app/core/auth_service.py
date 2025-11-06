"""
Authentication business logic service.
Handles tenant registration, login, and token management.
"""

import uuid
from typing import Dict, Any, Optional
from app.core.database import get_db_manager
from app.core.security import JWTHandler, PasswordHandler
from app.core.schema_manager import SchemaManager
from app.core.exceptions import (
    AuthenticationException,
    ConflictException,
    NotFoundException,
    DatabaseException
)
from app.schemas.auth import TenantLoginRequest, TenantRegisterRequest
from app.config import settings
import logging

logger = logging.getLogger(__name__)

class AuthService:
    """Service for authentication operations."""
    
    @staticmethod
    def _generate_database_name(tenant_identifier: str) -> str:
        """
        Generate database name from tenant identifier.
        
        Args:
            tenant_identifier: Tenant identifier
            
        Returns:
            Database name
        """
        # Clean tenant identifier for database name
        clean_identifier = tenant_identifier.lower().replace("-", "_")
        return f"{settings.TENANT_DB_PREFIX}{clean_identifier}"
    
    @staticmethod
    def register_tenant(request: TenantRegisterRequest) -> Dict[str, Any]:
        """
        Register a new tenant with dedicated database.
        
        Args:
            request: Tenant registration request
            
        Returns:
            Dictionary containing tenant_id, tenant_name, and database_name
            
        Raises:
            ConflictException: If tenant already exists
            DatabaseException: If registration fails
        """
        tenant_id = str(uuid.uuid4())
        database_name = AuthService._generate_database_name(request.tenant_identifier)
        db_manager = get_db_manager()
        
        try:
            # Check if tenant identifier already exists in master database
            with db_manager.get_master_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT tenant_id FROM public.tenants 
                        WHERE tenant_identifier = %s
                        """,
                        (request.tenant_identifier,)
                    )
                    if cursor.fetchone():
                        raise ConflictException(
                            f"Tenant identifier '{request.tenant_identifier}' already exists"
                        )
                    
                    # Check if email already exists
                    cursor.execute(
                        """
                        SELECT tenant_id FROM public.tenants 
                        WHERE admin_email = %s
                        """,
                        (request.email,)
                    )
                    if cursor.fetchone():
                        raise ConflictException(
                            f"Email '{request.email}' already registered"
                        )
                finally:
                    cursor.close()
            
            # Create tenant database and initialize tables
            SchemaManager.create_tenant_database(tenant_id, database_name)
            
            # Hash password
            password_hash = PasswordHandler.hash_password(request.password)
            
            # Insert tenant in master database
            with db_manager.get_master_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        INSERT INTO public.tenants 
                        (tenant_id, tenant_name, tenant_identifier, admin_email, 
                         admin_password_hash, database_name, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            tenant_id,
                            request.tenant_name,
                            request.tenant_identifier,
                            request.email,
                            password_hash,
                            database_name,
                            "ACTIVE"
                        )
                    )
                    conn.commit()
                finally:
                    cursor.close()
            
            # Insert tenant record in their own database
                        # Insert tenant record in their own database
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        INSERT INTO tenants 
                        (tenant_id, tenant_name, tenant_identifier, admin_email, 
                         admin_password_hash, database_name, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            tenant_id,
                            request.tenant_name,
                            request.tenant_identifier,
                            request.email,
                            password_hash,
                            database_name,
                            "ACTIVE"
                        )
                    )
                    conn.commit()
                finally:
                    cursor.close()
            
            logger.info(f"Tenant registered successfully: {tenant_id} with database: {database_name}")
            
            return {
                "tenant_id": tenant_id,
                "tenant_name": request.tenant_name,
                "database_name": database_name
            }
            
        except (ConflictException, DatabaseException):
            # Cleanup: try to drop database if it was created
            try:
                db_manager.drop_tenant_database(database_name)
            except:
                pass
            raise
        except Exception as e:
            # Cleanup: try to drop database if it was created
            try:
                db_manager.drop_tenant_database(database_name)
            except:
                pass
            logger.error(f"Tenant registration failed: {str(e)}")
            raise DatabaseException(f"Tenant registration failed: {str(e)}")
    
    @staticmethod
    def login_tenant(request: TenantLoginRequest) -> Dict[str, Any]:
        """
        Login a tenant and generate JWT token.
        
        Args:
            request: Tenant login request
            
        Returns:
            Dictionary containing tenant_id, tenant_name, database_name, and access_token
            
        Raises:
            AuthenticationException: If credentials are invalid
            NotFoundException: If tenant not found
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_master_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT tenant_id, tenant_name, admin_password_hash, database_name, status
                        FROM public.tenants
                        WHERE tenant_identifier = %s AND admin_email = %s
                        """,
                        (request.tenant_identifier, request.email)
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        raise NotFoundException("Tenant", request.tenant_identifier)
                    
                    tenant_id, tenant_name, password_hash, database_name, status = result
                    
                    # Check if tenant is active
                    if status != "ACTIVE":
                        raise AuthenticationException(f"Tenant account is {status}")
                    
                    # Verify password
                    if not PasswordHandler.verify_password(request.password, password_hash):
                        raise AuthenticationException("Invalid password")
                    
                    # Create JWT token with database_name
                    token_data = {
                        "tenant_id": tenant_id,
                        "tenant_identifier": request.tenant_identifier,
                        "email": request.email,
                        "database_name": database_name  # Include database name in token
                    }
                    access_token = JWTHandler.create_token(token_data)
                    
                    logger.info(f"Tenant logged in successfully: {tenant_id} (database: {database_name})")
                    
                    return {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "database_name": database_name,
                        "access_token": access_token,
                        "token_type": "bearer"
                    }
                    
                finally:
                    cursor.close()
                    
        except (AuthenticationException, NotFoundException):
            raise
        except Exception as e:
            logger.error(f"Tenant login failed: {str(e)}")
            raise DatabaseException(f"Login failed: {str(e)}")
    
    @staticmethod
    def verify_tenant_token(token: str) -> Dict[str, Any]:
        """
        Verify JWT token and extract tenant information.
        
        Args:
            token: JWT token
            
        Returns:
            Decoded token payload with database_name
            
        Raises:
            AuthenticationException: If token is invalid
        """
        try:
            payload = JWTHandler.verify_token(token)
            
            # Ensure database_name is in payload
            if "database_name" not in payload:
                raise AuthenticationException("Invalid token format")
            
            return payload
        except AuthenticationException:
            raise
        except Exception as e:
            logger.error(f"Token verification failed: {str(e)}")
            raise AuthenticationException("Invalid token")
        

    @staticmethod
    def get_tenant_by_id(tenant_id: str) -> Dict[str, Any]:
        """
        Get tenant details by tenant_id from master database.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            Dictionary with tenant information
            
        Raises:
            NotFoundException: If tenant not found
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_master_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT tenant_id, tenant_name, tenant_identifier, admin_email, 
                            database_name, status, created_at
                        FROM public.tenants
                        WHERE tenant_id = %s
                        """,
                        (tenant_id,)
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        raise NotFoundException("Tenant", tenant_id)
                    
                    tenant_id, tenant_name, tenant_identifier, admin_email, database_name, status, created_at = result
                    
                    return {
                        "tenant_id": tenant_id,
                        "tenant_name": tenant_name,
                        "tenant_identifier": tenant_identifier,
                        "admin_email": admin_email,
                        "database_name": database_name,
                        "status": status,
                        "created_at": created_at.isoformat() if created_at else None
                    }
                finally:
                    cursor.close()
                    
        except NotFoundException:
            raise
        except Exception as e:
            logger.error(f"Failed to get tenant: {str(e)}")
            raise DatabaseException(f"Failed to get tenant: {str(e)}")

    @staticmethod
    def update_tenant_status(tenant_id: str, new_status: str) -> bool:
        """
        Update tenant status (ACTIVE, INACTIVE, SUSPENDED).
        
        Args:
            tenant_id: Tenant identifier
            new_status: New status value
            
        Returns:
            True if update successful
            
        Raises:
            DatabaseException: If update fails
        """
        if new_status not in ["ACTIVE", "INACTIVE", "SUSPENDED"]:
            raise ValidationException(f"Invalid status: {new_status}")
        
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_master_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        UPDATE public.tenants
                        SET status = %s
                        WHERE tenant_id = %s
                        """,
                        (new_status, tenant_id)
                    )
                    conn.commit()
                    logger.info(f"Tenant {tenant_id} status updated to {new_status}")
                    return True
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to update tenant status: {str(e)}")
            raise DatabaseException(f"Failed to update tenant status: {str(e)}")
