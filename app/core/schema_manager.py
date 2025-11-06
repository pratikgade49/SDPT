"""
Schema management utilities for tenant isolation.
Handles tenant database creation and table setup.
"""

from typing import List, Dict, Any
from app.models.database_models import FieldDefinition
from app.core.database import get_db_manager
from app.core.exceptions import DatabaseException
import logging

logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages database schemas and tenant isolation."""
    
    @staticmethod
    def create_tenant_database(tenant_id: str, database_name: str) -> bool:
        """
        Create a new database for tenant and initialize tables.
        
        Args:
            tenant_id: Unique tenant identifier
            database_name: Name for the tenant database
            
        Returns:
            True if database created successfully
            
        Raises:
            DatabaseException: If database creation fails
        """
        db_manager = get_db_manager()
        
        try:
            # Create the database
            db_manager.create_tenant_database(database_name)
            
            # Initialize tables in the new database
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    # Create tenants table (for tenant's own record)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tenants (
                            tenant_id UUID PRIMARY KEY,
                            tenant_name VARCHAR(255) NOT NULL,
                            tenant_identifier VARCHAR(100) NOT NULL,
                            admin_email VARCHAR(255) NOT NULL,
                            admin_password_hash VARCHAR(255) NOT NULL,
                            database_name VARCHAR(100) NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            status VARCHAR(50) DEFAULT 'ACTIVE'
                        )
                    """)
                    
                    # Create field_catalogue table
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS field_catalogue (
                            catalogue_id UUID PRIMARY KEY,
                            tenant_id UUID NOT NULL,
                            version INT DEFAULT 1,
                            status VARCHAR(50) DEFAULT 'DRAFT',
                            fields_json JSONB NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_by VARCHAR(255) NOT NULL,
                            updated_at TIMESTAMP,
                            updated_by VARCHAR(255)
                        )
                    """)
                    
                    # Create master_data table (will be modified when catalogue finalized)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS master_data (
                            master_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            tenant_id UUID NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_by VARCHAR(255) NOT NULL,
                            updated_at TIMESTAMP,
                            updated_by VARCHAR(255)
                        )
                    """)
                    
                    # Create sales_data table
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS sales_data (
                            sales_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            tenant_id UUID NOT NULL,
                            master_id UUID NOT NULL REFERENCES master_data(master_id),
                            date DATE NOT NULL,
                            quantity DECIMAL(18, 2) NOT NULL,
                            uom VARCHAR(20) NOT NULL,
                            unit_price DECIMAL(18, 2),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_by VARCHAR(255) NOT NULL
                        )
                    """)
                    
                    # Create upload_history table
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS upload_history (
                            upload_id UUID PRIMARY KEY,
                            tenant_id UUID NOT NULL,
                            upload_type VARCHAR(50) NOT NULL,
                            file_name VARCHAR(255) NOT NULL,
                            total_rows INT DEFAULT 0,
                            success_count INT DEFAULT 0,
                            failed_count INT DEFAULT 0,
                            status VARCHAR(50) NOT NULL,
                            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            uploaded_by VARCHAR(255) NOT NULL
                        )
                    """)
                    
                    # Create indexes
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_data_date ON sales_data(date)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_data_master_id ON sales_data(master_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_upload_history_tenant ON upload_history(tenant_id)")
                    
                    conn.commit()
                    logger.info(f"Database initialized successfully for tenant: {tenant_id}")
                    return True
                    
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to create tenant database {database_name}: {str(e)}")
            raise DatabaseException(f"Failed to create tenant database: {str(e)}")
    
    @staticmethod
    def create_master_data_table(
        tenant_id: str,
        database_name: str,
        fields: List[FieldDefinition]
    ) -> bool:
        """
        Create dynamic master data table based on field catalogue.
        All characteristic fields are NULLABLE to allow partial data.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            fields: List of field definitions
            
        Returns:
            True if table created successfully
            
        Raises:
            DatabaseException: If table creation fails
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    # Drop existing master_data table
                    cursor.execute("DROP TABLE IF EXISTS master_data CASCADE")
                    
                    # Build column definitions
                    columns = []
                    
                    # Add master_id as primary key
                    columns.append("master_id UUID PRIMARY KEY DEFAULT gen_random_uuid()")
                    
                    # Add all fields as NULLABLE
                    for field in fields:
                        sql_type = field.get_sql_type()
                        columns.append(f'"{field.field_name}" {sql_type}')
                    
                    # Add audit columns
                    columns.extend([
                        "tenant_id UUID NOT NULL",
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                        "created_by VARCHAR(255) NOT NULL",
                        "updated_at TIMESTAMP",
                        "updated_by VARCHAR(255)"
                    ])
                    
                    # Create table
                    create_table_sql = f"""
                        CREATE TABLE master_data (
                            {', '.join(columns)}
                        )
                    """
                    cursor.execute(create_table_sql)
                    
                    # Recreate sales_data table with foreign key
                    cursor.execute("DROP TABLE IF EXISTS sales_data CASCADE")
                    cursor.execute("""
                        CREATE TABLE sales_data (
                            sales_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            tenant_id UUID NOT NULL,
                            master_id UUID NOT NULL REFERENCES master_data(master_id),
                            date DATE NOT NULL,
                            quantity DECIMAL(18, 2) NOT NULL,
                            uom VARCHAR(20) NOT NULL,
                            unit_price DECIMAL(18, 2),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            created_by VARCHAR(255) NOT NULL
                        )
                    """)
                    
                    # Recreate indexes
                    cursor.execute("CREATE INDEX idx_sales_data_date ON sales_data(date)")
                    cursor.execute("CREATE INDEX idx_sales_data_master_id ON sales_data(master_id)")
                    
                    conn.commit()
                    logger.info(f"Master data table created in {database_name} with {len(fields)} fields (all nullable)")
                    return True
                    
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to create master data table in {database_name}: {str(e)}")
            raise DatabaseException(f"Failed to create master data table: {str(e)}")
    
    @staticmethod
    def add_upload_history_table(tenant_id: str, database_name: str) -> bool:
        """
        Ensure upload_history table exists in tenant database.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            
        Returns:
            True if successful
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS upload_history (
                            upload_id UUID PRIMARY KEY,
                            tenant_id UUID NOT NULL,
                            upload_type VARCHAR(50) NOT NULL,
                            file_name VARCHAR(255) NOT NULL,
                            total_rows INT DEFAULT 0,
                            success_count INT DEFAULT 0,
                            failed_count INT DEFAULT 0,
                            status VARCHAR(50) NOT NULL,
                            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            uploaded_by VARCHAR(255) NOT NULL
                        )
                    """)
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            logger.error(f"Failed to ensure upload_history table in {database_name}: {str(e)}")
            raise DatabaseException(f"Failed to ensure upload history table: {str(e)}")
    
    @staticmethod
    def table_exists(tenant_id: str, database_name: str, table_name: str) -> bool:
        """
        Check if a table exists in tenant database.
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            table_name: Table name to check
            
        Returns:
            True if table exists
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table_name,))
                    return cursor.fetchone()[0]
                finally:
                    cursor.close()
        except Exception as e:
            logger.error(f"Failed to check if table exists: {str(e)}")
            return False
    
    @staticmethod
    def validate_tenant_database(database_name: str) -> Dict[str, Any]:
        """
        Validate tenant database structure and report any issues.
        
        Args:
            database_name: Tenant's database name
            
        Returns:
            Dictionary with validation results
            
        Raises:
            DatabaseException: If validation fails
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    # Get all tables in the database
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)
                    
                    tables = {row[0] for row in cursor.fetchall()}
                    
                    # Check required tables
                    required_tables = {'tenants', 'field_catalogue', 'master_data', 'sales_data', 'upload_history'}
                    missing_tables = required_tables - tables
                    
                    # Get row counts
                    row_counts = {}
                    for table in required_tables:
                        if table in tables:
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            row_counts[table] = cursor.fetchone()[0]
                    
                    return {
                        "database_name": database_name,
                        "all_tables_exist": len(missing_tables) == 0,
                        "missing_tables": list(missing_tables),
                        "row_counts": row_counts,
                        "valid": len(missing_tables) == 0
                    }
                    
                finally:
                    cursor.close()
                    
        except Exception as e:
            logger.error(f"Failed to validate database {database_name}: {str(e)}")
            raise DatabaseException(f"Database validation failed: {str(e)}")

    @staticmethod
    def drop_tenant_database(tenant_id: str, database_name: str) -> bool:
        """
        Drop tenant's database (use with caution!).
        
        Args:
            tenant_id: Tenant identifier
            database_name: Tenant's database name
            
        Returns:
            True if successful
        """
        db_manager = get_db_manager()
        
        try:
            db_manager.drop_tenant_database(database_name)
            logger.info(f"Dropped database for tenant {tenant_id}: {database_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop tenant database: {str(e)}")
            raise DatabaseException(f"Failed to drop tenant database: {str(e)}")