"""
Field Catalogue business logic service.
Handles field definition management and validation.
"""

import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.core.database import get_db_manager
from app.core.schema_manager import SchemaManager
from app.core.exceptions import DatabaseException, ValidationException, NotFoundException
from app.models.database_models import FieldDefinition
from app.schemas.field_catalogue import FieldCatalogueItemRequest, FieldCatalogueRequest
import json
import logging

logger = logging.getLogger(__name__)

class FieldCatalogueService:
    """Service for field catalogue operations."""
    
    @staticmethod
    def create_field_catalogue(
        tenant_id: str,
        database_name: str,
        request: FieldCatalogueRequest,
        user_email: str
    ) -> Dict[str, Any]:
        """
        Create a new field catalogue in DRAFT status.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            request: Field catalogue request
            user_email: Email of user creating catalogue
            
        Returns:
            Created field catalogue details
            
        Raises:
            DatabaseException: If creation fails
            ValidationException: If validation fails
        """
        catalogue_id = str(uuid.uuid4())
        db_manager = get_db_manager()
        
        try:
            # Convert field requests to FieldDefinition objects
            field_definitions = [
                FieldDefinition(
                    field_name=f.field_name,
                    data_type=f.data_type,
                    field_length=f.field_length,
                    default_value=f.default_value,
                    is_characteristic=f.is_characteristic,
                    characteristic_type=f.characteristic_type,
                    characteristic_category=f.characteristic_category
                )
                for f in request.fields
            ]
            
            # Serialize fields to JSON
            fields_json = json.dumps([f.to_dict() for f in field_definitions])
            
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO field_catalogue 
                        (catalogue_id, tenant_id, version, status, fields_json, created_by)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            catalogue_id,
                            tenant_id,
                            1,
                            "DRAFT",
                            fields_json,
                            user_email
                        )
                    )
                    
                    conn.commit()
                    logger.info(f"Field catalogue created: {catalogue_id} in database: {database_name}")
                    
                    return {
                        "catalogue_id": catalogue_id,
                        "tenant_id": tenant_id,
                        "fields": [f.to_dict() for f in field_definitions],
                        "version": 1,
                        "status": "DRAFT",
                        "created_at": datetime.utcnow().isoformat(),
                        "created_by": user_email
                    }
                    
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to create field catalogue: {str(e)}")
            raise DatabaseException(f"Failed to create field catalogue: {str(e)}")
    
    @staticmethod
    def finalize_field_catalogue(
        tenant_id: str,
        database_name: str,
        catalogue_id: str,
        user_email: str
    ) -> Dict[str, Any]:
        """
        Finalize a field catalogue and create master data table.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            catalogue_id: Catalogue identifier
            user_email: Email of user finalizing catalogue
            
        Returns:
            Updated field catalogue details
            
        Raises:
            DatabaseException: If finalization fails
            NotFoundException: If catalogue not found
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    # Fetch current catalogue
                    cursor.execute("""
                        SELECT fields_json, status FROM field_catalogue 
                        WHERE catalogue_id = %s AND tenant_id = %s
                        """,
                        (catalogue_id, tenant_id)
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        raise NotFoundException("Field Catalogue", catalogue_id)
                    
                    fields_json_raw, status = result
                    
                    if status == "FINALIZED":
                        raise ValidationException("Catalogue is already finalized")
                    
                    # Parse fields
                    if isinstance(fields_json_raw, str):
                        fields_data = json.loads(fields_json_raw)
                    elif isinstance(fields_json_raw, list):
                        fields_data = fields_json_raw
                    else:
                        fields_data = fields_json_raw

                    field_definitions = [
                        FieldDefinition(**field_data)
                        for field_data in fields_data
                    ]
                    
                    # Create master data table
                    SchemaManager.create_master_data_table(tenant_id, database_name, field_definitions)
                    
                    # Update catalogue status
                    cursor.execute("""
                        UPDATE field_catalogue 
                        SET status = %s, updated_at = %s, updated_by = %s
                        WHERE catalogue_id = %s
                        """,
                        ("FINALIZED", datetime.utcnow(), user_email, catalogue_id)
                    )
                    
                    conn.commit()
                    logger.info(f"Field catalogue finalized: {catalogue_id} in database: {database_name}")
                    
                    return {
                        "catalogue_id": catalogue_id,
                        "tenant_id": tenant_id,
                        "fields": fields_data,
                        "version": 1,
                        "status": "FINALIZED",
                        "created_by": user_email
                    }
                    
                finally:
                    cursor.close()
                    
        except (NotFoundException, ValidationException):
            raise
        except Exception as e:
            logger.error(f"Failed to finalize field catalogue: {str(e)}")
            raise DatabaseException(f"Failed to finalize field catalogue: {str(e)}")
    
    @staticmethod
    def get_field_catalogue(
        tenant_id: str,
        database_name: str,
        catalogue_id: str
    ) -> Dict[str, Any]:
        """
        Get field catalogue details.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            catalogue_id: Catalogue identifier
            
        Returns:
            Field catalogue details
            
        Raises:
            NotFoundException: If catalogue not found
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        SELECT catalogue_id, tenant_id, version, status, fields_json, 
                               created_at, created_by, updated_at, updated_by
                        FROM field_catalogue 
                        WHERE catalogue_id = %s AND tenant_id = %s
                        """,
                        (catalogue_id, tenant_id)
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        raise NotFoundException("Field Catalogue", catalogue_id)
                    
                    cat_id, ten_id, version, status, fields_json_raw, created_at, created_by, updated_at, updated_by = result
                    
                    # Parse fields
                    if isinstance(fields_json_raw, str):
                        fields_data = json.loads(fields_json_raw)
                    elif isinstance(fields_json_raw, list):
                        fields_data = fields_json_raw
                    else:
                        fields_data = fields_json_raw
                    
                    logger.info(f"Retrieved field catalogue {catalogue_id} with {len(fields_data)} fields from {database_name}")
                    
                    return {
                        "catalogue_id": cat_id,
                        "tenant_id": ten_id,
                        "fields": fields_data,
                        "version": version,
                        "status": status,
                        "created_at": created_at.isoformat() if created_at else None,
                        "created_by": created_by,
                        "updated_at": updated_at.isoformat() if updated_at else None,
                        "updated_by": updated_by
                    }
                    
                finally:
                    cursor.close()
                    
        except NotFoundException:
            raise
        except Exception as e:
            logger.error(f"Failed to get field catalogue: {str(e)}")
            raise DatabaseException(f"Failed to get field catalogue: {str(e)}")
    
    @staticmethod
    def list_field_catalogues(
        tenant_id: str,
        database_name: str,
        page: int = 1,
        page_size: int = 50
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        List all field catalogues for a tenant.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            page: Page number
            page_size: Records per page
            
        Returns:
            Tuple of (catalogues list, total count)
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    # Get total count
                    cursor.execute(
                        "SELECT COUNT(*) FROM field_catalogue WHERE tenant_id = %s",
                        (tenant_id,)
                    )
                    total_count = cursor.fetchone()[0]
                    
                    # Get paginated results
                    offset = (page - 1) * page_size
                    cursor.execute("""
                        SELECT catalogue_id, tenant_id, version, status, fields_json, 
                               created_at, created_by, updated_at, updated_by
                        FROM field_catalogue 
                        WHERE tenant_id = %s
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        (tenant_id, page_size, offset)
                    )
                    
                    catalogues = []
                    for row in cursor.fetchall():
                        cat_id, ten_id, version, status, fields_json_raw, created_at, created_by, updated_at, updated_by = row
                        
                        # Parse fields
                        if isinstance(fields_json_raw, str):
                            fields_data = json.loads(fields_json_raw)
                        elif isinstance(fields_json_raw, list):
                            fields_data = fields_json_raw
                        else:
                            fields_data = fields_json_raw
                        
                        catalogues.append({
                            "catalogue_id": cat_id,
                            "tenant_id": ten_id,
                            "fields": fields_data,
                            "version": version,
                            "status": status,
                            "created_at": created_at.isoformat() if created_at else None,
                            "created_by": created_by,
                            "updated_at": updated_at.isoformat() if updated_at else None,
                            "updated_by": updated_by
                        })
                    
                    return catalogues, total_count
                    
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to list field catalogues: {str(e)}")
            raise DatabaseException(f"Failed to list field catalogues: {str(e)}")