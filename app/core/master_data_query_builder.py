"""
Master Data Query Builder - SQL query construction.
Centralized query building with parameterized queries for security.
"""

from typing import Dict, Any, List, Tuple
from datetime import datetime


class MasterDataQueryBuilder:
    """Builds SQL queries for master data operations."""
    
    @staticmethod
    def build_insert_query(
        tenant_id: str,
        master_id: str,
        data: Dict[str, Any],
        user_email: str
    ) -> Tuple[str, List[Any]]:
        """
        Build INSERT query with parameterized values.
        
        Args:
            tenant_id: Tenant identifier
            master_id: Master data identifier
            data: Data to insert
            user_email: User email
            
        Returns:
            Tuple of (query, values)
        """
        schema_name = f"tenant_{tenant_id}"
        
        columns = ['master_id', 'created_at', 'created_by'] + list(data.keys())
        placeholders = ['%s'] * len(columns)
        values = [master_id, datetime.utcnow(), user_email] + list(data.values())
        
        query = f"""
            INSERT INTO "{schema_name}".master_data_extended 
            ({', '.join(f'"{col}"' for col in columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING master_id, created_at
        """
        
        return query, values
    
    @staticmethod
    def build_bulk_insert_query(
        tenant_id: str,
        records_data: List[Dict[str, Any]],
        master_ids: List[str],
        user_email: str
    ) -> Tuple[str, List[Any]]:
        """
        Build bulk INSERT query for multiple records.
        Uses VALUES clause with multiple rows - single query execution.
        
        Args:
            tenant_id: Tenant identifier
            records_data: List of data dictionaries
            master_ids: List of generated master IDs
            user_email: User email
            
        Returns:
            Tuple of (query, values)
        """
        schema_name = f"tenant_{tenant_id}"
        
        # Get all unique columns across all records
        all_columns = set()
        for record in records_data:
            all_columns.update(record.keys())
        
        sorted_columns = sorted(all_columns)
        columns = ['master_id', 'created_at', 'created_by'] + sorted_columns
        
        # Build values list and placeholders
        values = []
        row_placeholders = []
        current_time = datetime.utcnow()
        
        for master_id, record in zip(master_ids, records_data):
            row_values = [master_id, current_time, user_email]
            row_values.extend(record.get(col) for col in sorted_columns)
            values.extend(row_values)
            row_placeholders.append(f"({', '.join(['%s'] * len(columns))})")
        
        query = f"""
            INSERT INTO "{schema_name}".master_data_extended 
            ({', '.join(f'"{col}"' for col in columns)})
            VALUES {', '.join(row_placeholders)}
            RETURNING master_id
        """
        
        return query, values
    
    @staticmethod
    def build_select_by_id_query(tenant_id: str) -> str:
        """
        Build SELECT query for single record by ID.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            SQL query string
        """
        schema_name = f"tenant_{tenant_id}"
        
        return f"""
            SELECT * FROM "{schema_name}".master_data_extended 
            WHERE master_id = %s
        """
    
    @staticmethod
    def build_update_query(
        tenant_id: str,
        master_id: str,
        data: Dict[str, Any],
        user_email: str
    ) -> Tuple[str, List[Any]]:
        """
        Build UPDATE query with parameterized values.
        
        Args:
            tenant_id: Tenant identifier
            master_id: Master data identifier
            data: Fields to update
            user_email: User email
            
        Returns:
            Tuple of (query, values)
        """
        schema_name = f"tenant_{tenant_id}"
        
        set_clauses = [f'"{col}" = %s' for col in data.keys()]
        set_clauses.extend(['"updated_at" = %s', '"updated_by" = %s'])
        
        values = list(data.values()) + [datetime.utcnow(), user_email, master_id]
        
        query = f"""
            UPDATE "{schema_name}".master_data_extended 
            SET {', '.join(set_clauses)}
            WHERE master_id = %s
        """
        
        return query, values
    
    @staticmethod
    def build_delete_query(tenant_id: str) -> str:
        """
        Build DELETE query.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            SQL query string
        """
        schema_name = f"tenant_{tenant_id}"
        
        return f"""
            DELETE FROM "{schema_name}".master_data_extended 
            WHERE master_id = %s
        """
    
    @staticmethod
    def build_list_query(
        tenant_id: str,
        filters: Dict[str, Any],
        sort_by: str,
        sort_order: str,
        limit: int,
        offset: int
    ) -> Tuple[str, List[Any]]:
        """
        Build paginated list query with filters.
        
        Args:
            tenant_id: Tenant identifier
            filters: Filter conditions
            sort_by: Column to sort by
            sort_order: Sort order (asc/desc)
            limit: Records limit
            offset: Records offset
            
        Returns:
            Tuple of (query, values)
        """
        schema_name = f"tenant_{tenant_id}"
        values = []
        where_clauses = []
        
        # Build WHERE conditions
        for col, val in filters.items():
            where_clauses.append(f'"{col}" = %s')
            values.append(val)
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Add pagination values
        values.extend([limit, offset])
        
        query = f"""
            SELECT * FROM "{schema_name}".master_data_extended 
            {where_sql}
            ORDER BY "{sort_by}" {sort_order.upper()}
            LIMIT %s OFFSET %s
        """
        
        return query, values
    
    @staticmethod
    def build_count_query(
        tenant_id: str,
        filters: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """
        Build count query for pagination.
        
        Args:
            tenant_id: Tenant identifier
            filters: Filter conditions
            
        Returns:
            Tuple of (query, values)
        """
        schema_name = f"tenant_{tenant_id}"
        values = []
        where_clauses = []
        
        for col, val in filters.items():
            where_clauses.append(f'"{col}" = %s')
            values.append(val)
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
            SELECT COUNT(*) FROM "{schema_name}".master_data_extended 
            {where_sql}
        """
        
        return query, values
    
    @staticmethod
    def build_response_dict(columns: List[str], row: Tuple) -> Dict[str, Any]:
        """
        Build response dictionary from query result.

        Args:
            columns: Column names
            row: Query result row

        Returns:
            Dictionary with column:value pairs
        """
        result = {}
        system_fields = {}
        data_fields = {}

        for col, val in zip(columns, row):
            if col in ('master_id', 'tenant_id', 'created_at', 'created_by', 'updated_at', 'updated_by'):
                system_fields[col] = val.isoformat() if hasattr(val, 'isoformat') else val
            else:
                data_fields[col] = val

        result.update(system_fields)
        result['data'] = data_fields

        return result

    @staticmethod
    def build_select_by_master_id_query(tenant_id: str) -> str:
        """
        Build SELECT query for single record by master_id.

        Args:
            tenant_id: Tenant identifier

        Returns:
            SQL query string
        """
        schema_name = f"tenant_{tenant_id}"

        return f"""
            SELECT * FROM "{schema_name}".master_data
            WHERE master_id = %s
        """

    @staticmethod
    def build_select_by_master_ids_query(tenant_id: str, master_ids: List[str]) -> Tuple[str, List[Any]]:
        """
        Build SELECT query for multiple records by master_ids.

        Args:
            tenant_id: Tenant identifier
            master_ids: List of master IDs

        Returns:
            Tuple of (query, values)
        """
        schema_name = f"tenant_{tenant_id}"
        placeholders = ', '.join(['%s'] * len(master_ids))

        query = f"""
            SELECT * FROM "{schema_name}".master_data
            WHERE master_id IN ({placeholders})
        """

        return query, master_ids
