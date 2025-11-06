"""
Database connection and management module.
Handles PostgreSQL connections with one database per tenant architecture.
"""

from psycopg2 import pool, connect
from psycopg2.extensions import connection as Connection, ISOLATION_LEVEL_AUTOCOMMIT
from contextlib import contextmanager
from typing import Generator, Optional, Dict, Any
import logging
import uuid

from app.config import settings
from app.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections with one database per tenant."""
    
    _instance: Optional["DatabaseManager"] = None
    _master_pool: Optional[pool.SimpleConnectionPool] = None
    _tenant_pools: Dict[str, pool.SimpleConnectionPool] = {}
    
    def __new__(cls):
        """Singleton pattern for DatabaseManager."""
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialize_master_pool()
        return cls._instance
    
    def _ensure_master_database_exists(self) -> None:
        """Ensure master database and tables exist."""
        # Parse database name from MASTER_DATABASE_URL
        # URL format: postgresql://user:pass@host:port/database
        url_parts = settings.MASTER_DATABASE_URL.split('/')
        master_db_name = url_parts[-1]

        try:
            # Connect to postgres database to check/create master database
            conn = connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                database="postgres"
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            try:
                # Check if master database exists
                cursor.execute(f"""
                    SELECT 1 FROM pg_database WHERE datname = '{master_db_name}'
                """)
                exists = cursor.fetchone()

                if not exists:
                    logger.info(f"Creating master database '{master_db_name}'...")
                    cursor.execute(f"CREATE DATABASE {master_db_name}")
                    logger.info("Master database created successfully")

                    # Create tables in master database
                    logger.info("Creating master database tables...")
                    master_conn = connect(
                        host=settings.DB_HOST,
                        port=settings.DB_PORT,
                        user=settings.DB_USER,
                        password=settings.DB_PASSWORD,
                        database=master_db_name
                    )
                    master_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    master_cursor = master_conn.cursor()

                    try:
                        # Create tenants table
                        master_cursor.execute("""
                            CREATE TABLE IF NOT EXISTS public.tenants (
                                tenant_id UUID PRIMARY KEY,
                                tenant_name VARCHAR(255) NOT NULL,
                                tenant_identifier VARCHAR(100) UNIQUE NOT NULL,
                                admin_email VARCHAR(255) UNIQUE NOT NULL,
                                admin_password_hash VARCHAR(255) NOT NULL,
                                database_name VARCHAR(100) UNIQUE NOT NULL,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                status VARCHAR(50) DEFAULT 'ACTIVE',
                                CONSTRAINT check_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED'))
                            )
                        """)
                        logger.info("Created tenants table")

                        # Create indexes
                        master_cursor.execute("""
                            CREATE INDEX IF NOT EXISTS idx_tenants_identifier
                            ON public.tenants(tenant_identifier)
                        """)
                        master_cursor.execute("""
                            CREATE INDEX IF NOT EXISTS idx_tenants_email
                            ON public.tenants(admin_email)
                        """)
                        master_cursor.execute("""
                            CREATE INDEX IF NOT EXISTS idx_tenants_database_name
                            ON public.tenants(database_name)
                        """)
                        logger.info("Created indexes on tenants table")

                        # Create audit log table
                        master_cursor.execute("""
                            CREATE TABLE IF NOT EXISTS public.audit_log (
                                log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                tenant_id UUID REFERENCES public.tenants(tenant_id) ON DELETE CASCADE,
                                action VARCHAR(50) NOT NULL,
                                entity_type VARCHAR(100),
                                entity_id UUID,
                                performed_by VARCHAR(255),
                                performed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                details JSONB
                            )
                        """)
                        master_cursor.execute("""
                            CREATE INDEX IF NOT EXISTS idx_audit_tenant
                            ON public.audit_log(tenant_id)
                        """)
                        master_cursor.execute("""
                            CREATE INDEX IF NOT EXISTS idx_audit_timestamp
                            ON public.audit_log(performed_at DESC)
                        """)
                        logger.info("Created audit_log table")

                        logger.info("Master database initialization completed successfully")

                    finally:
                        master_cursor.close()
                        master_conn.close()
                else:
                    logger.info(f"Master database '{master_db_name}' already exists")

            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            logger.error(f"Failed to ensure master database exists: {str(e)}")
            raise DatabaseException(f"Master database initialization failed: {str(e)}")

    def _initialize_master_pool(self) -> None:
        """Initialize master database connection pool."""
        try:
            # Ensure master database exists before initializing pool
            self._ensure_master_database_exists()

            self._master_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=settings.MASTER_DATABASE_URL
            )
            logger.info("Master database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize master database pool: {str(e)}")
            raise DatabaseException(f"Master database pool initialization failed: {str(e)}")
    
    def _get_tenant_database_url(self, database_name: str) -> str:
        """
        Construct database URL for tenant database.
        
        Args:
            database_name: Name of tenant database
            
        Returns:
            Database connection URL
        """
        return f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{database_name}"
    
    def _initialize_tenant_pool(self, database_name: str) -> None:
        """
        Initialize connection pool for a tenant database.
        
        Args:
            database_name: Name of tenant database
        """
        if database_name in self._tenant_pools:
            return
        
        try:
            db_url = self._get_tenant_database_url(database_name)
            self._tenant_pools[database_name] = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=settings.DB_POOL_SIZE,
                dsn=db_url
            )
            logger.info(f"Connection pool initialized for tenant database: {database_name}")
        except Exception as e:
            logger.error(f"Failed to initialize pool for {database_name}: {str(e)}")
            raise DatabaseException(f"Tenant database pool initialization failed: {str(e)}")
    
    @contextmanager
    def get_master_connection(self) -> Generator[Connection, None, None]:
        """
        Get a connection to master database (tenant registry).
        
        Yields:
            Database connection to master database
        """
        connection = None
        try:
            if not self._master_pool:
                raise DatabaseException("Master connection pool not initialized")
            
            connection = self._master_pool.getconn()
            yield connection
            connection.commit()
            
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Master database error: {str(e)}")
            raise DatabaseException(f"Master database operation failed: {str(e)}")
        finally:
            if connection:
                self._master_pool.putconn(connection)
    
    @contextmanager
    def get_tenant_connection(self, database_name: str) -> Generator[Connection, None, None]:
        """
        Get a connection to a tenant's database.
        
        Args:
            database_name: Name of tenant database
            
        Yields:
            Database connection to tenant database
        """
        connection = None
        try:
            # Initialize pool if not exists
            if database_name not in self._tenant_pools:
                self._initialize_tenant_pool(database_name)
            
            connection = self._tenant_pools[database_name].getconn()
            yield connection
            connection.commit()
            
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Tenant database error ({database_name}): {str(e)}")
            raise DatabaseException(f"Tenant database operation failed: {str(e)}")
        finally:
            if connection and database_name in self._tenant_pools:
                self._tenant_pools[database_name].putconn(connection)
    
    @contextmanager
    def get_connection(self, tenant_id: Optional[str] = None, database_name: Optional[str] = None) -> Generator[Connection, None, None]:
        """
        Get a database connection (backward compatibility method).
        
        Args:
            tenant_id: Tenant identifier (deprecated, use database_name)
            database_name: Name of tenant database
            
        Yields:
            Database connection
        """
        if database_name:
            with self.get_tenant_connection(database_name) as conn:
                yield conn
        elif tenant_id:
            # Backward compatibility: lookup database_name from master
            db_name = self._get_database_name_for_tenant(tenant_id)
            with self.get_tenant_connection(db_name) as conn:
                yield conn
        else:
            raise DatabaseException("Either tenant_id or database_name must be provided")
    
    def _get_database_name_for_tenant(self, tenant_id: str) -> str:
        """
        Get database name for a tenant ID from master database.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            Database name
        """
        with self.get_master_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT database_name FROM public.tenants WHERE tenant_id = %s",
                    (tenant_id,)
                )
                result = cursor.fetchone()
                if not result:
                    raise DatabaseException(f"Tenant not found: {tenant_id}")
                return result[0]
            finally:
                cursor.close()
    
    @contextmanager
    def get_system_connection(self) -> Generator[Connection, None, None]:
        """
        Get a connection to system/master database.
        Alias for get_master_connection for backward compatibility.
        
        Yields:
            Database connection to master database
        """
        with self.get_master_connection() as conn:
            yield conn
    
    def create_tenant_database(self, database_name: str) -> bool:
        """
        Create a new database for a tenant.
        
        Args:
            database_name: Name of database to create
            
        Returns:
            True if database created successfully
        """
        try:
            # Connect to postgres database to create new database
            conn = connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                database="postgres"
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            try:
                # Check if database exists
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (database_name,)
                )
                if cursor.fetchone():
                    logger.warning(f"Database {database_name} already exists")
                    return True
                
                # Create database
                cursor.execute(f'CREATE DATABASE "{database_name}"')
                logger.info(f"Created tenant database: {database_name}")
                return True
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to create database {database_name}: {str(e)}")
            raise DatabaseException(f"Failed to create tenant database: {str(e)}")
    
    def drop_tenant_database(self, database_name: str) -> bool:
        """
        Drop a tenant's database.
        
        Args:
            database_name: Name of database to drop
            
        Returns:
            True if database dropped successfully
        """
        try:
            # Close connection pool if exists
            if database_name in self._tenant_pools:
                self._tenant_pools[database_name].closeall()
                del self._tenant_pools[database_name]
            
            # Connect to postgres database to drop tenant database
            conn = connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                database="postgres"
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            try:
                # Terminate existing connections
                cursor.execute(f"""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{database_name}'
                    AND pid <> pg_backend_pid()
                """)
                
                # Drop database
                cursor.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
                logger.info(f"Dropped tenant database: {database_name}")
                return True
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to drop database {database_name}: {str(e)}")
            raise DatabaseException(f"Failed to drop tenant database: {str(e)}")
    
    def execute_query(
        self,
        query: str,
        params: tuple = (),
        tenant_id: Optional[str] = None,
        database_name: Optional[str] = None,
        fetch_one: bool = False
    ):
        """
        Execute a database query.
        
        Args:
            query: SQL query string
            params: Query parameters
            tenant_id: Optional tenant identifier
            database_name: Optional database name
            fetch_one: Whether to fetch single row
            
        Returns:
            Query result(s)
        """
        with self.get_connection(tenant_id=tenant_id, database_name=database_name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params)
                if fetch_one:
                    return cursor.fetchone()
                return cursor.fetchall()
            finally:
                cursor.close()

    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get status of all active connection pools.
        
        Returns:
            Dictionary with pool status information
        """
        status = {
            "master_pool": {
                "initialized": self._master_pool is not None,
                "active_pools": len(self._tenant_pools)
            },
            "tenant_pools": {}
        }
        
        for db_name in self._tenant_pools.keys():
            status["tenant_pools"][db_name] = {
                "database": db_name,
                "active": True
            }
        
        return status

    def validate_tenant_connection(self, database_name: str) -> bool:
        """
        Validate that a tenant database connection is working.
        
        Args:
            database_name: Name of tenant database
            
        Returns:
            True if connection is valid
            
        Raises:
            DatabaseException: If connection validation fails
        """
        try:
            with self.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            logger.error(f"Connection validation failed for {database_name}: {str(e)}")
            raise DatabaseException(f"Connection validation failed: {str(e)}")

    
    def close_pool(self) -> None:
        """Close all connection pools."""
        if self._master_pool:
            self._master_pool.closeall()
            logger.info("Master database connection pool closed")
        
        for db_name, pool_obj in self._tenant_pools.items():
            pool_obj.closeall()
            logger.info(f"Closed connection pool for: {db_name}")
        
        self._tenant_pools.clear()

def get_db_manager() -> DatabaseManager:
    """Get DatabaseManager singleton instance."""
    return DatabaseManager()