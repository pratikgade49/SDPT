"""
Enhanced Excel upload service for automatic data distribution.
Handles automatic distribution of data between master_data and sales_data tables.
"""

import uuid
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime
from io import BytesIO
import logging

from app.core.database import get_db_manager
from app.core.schema_manager import SchemaManager
from app.core.field_catalogue_service import FieldCatalogueService
from app.core.exceptions import ValidationException, DatabaseException, NotFoundException
from app.schemas.upload import ExcelUploadResponse

logger = logging.getLogger(__name__)

class ExcelUploadService:
    """Service for handling Excel file uploads and automatic data distribution."""

    # Standard sales data column patterns (case-insensitive)
    SALES_DATA_PATTERNS = {
        'date': ['date', 'sales_date', 'transaction_date', 'period'],
        'quantity': ['quantity', 'qty', 'volume', 'amount'],
        'uom': ['uom', 'unit', 'unit_of_measure', 'measure'],
        'unit_price': ['unit_price', 'price', 'rate', 'cost', 'unitprice']
    }

    @staticmethod
    def get_db_manager():
        """Get database manager instance."""
        return get_db_manager()

    @staticmethod
    def validate_excel_file(file_content: bytes) -> pd.DataFrame:
        """
        Validate and parse Excel file content.
        """
        try:
            df = pd.read_excel(BytesIO(file_content), engine='openpyxl')

            if df.empty:
                raise ValidationException("Excel file is empty")

            # Remove completely empty rows
            df = df.dropna(how='all')

            if df.empty:
                raise ValidationException("Excel file contains no valid data rows")

            if len(df.columns) < 2:
                raise ValidationException("Excel file must have at least 2 columns")

            # Clean column names - strip whitespace
            df.columns = df.columns.str.strip()

            return df

        except Exception as e:
            if isinstance(e, ValidationException):
                raise
            logger.error(f"Error parsing Excel file: {str(e)}")
            raise ValidationException(f"Invalid Excel file format: {str(e)}")

    @staticmethod
    def identify_column_types(
        df: pd.DataFrame,
        field_catalogue: Dict[str, Any]
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Automatically identify which columns are master data vs sales data.
        
        Returns:
            Tuple of (master_data_column_mapping, sales_data_mapping)
            where master_data_column_mapping is {catalogue_field_name: excel_column_name}
        """
        df_columns_lower = {col.lower().strip(): col for col in df.columns}
        master_data_mapping = {}  # Maps catalogue field name to Excel column name
        sales_data_mapping = {}

        # Get field catalogue column names (case-insensitive)
        catalogue_fields_lower = {
            field['field_name'].lower().strip(): field['field_name'] 
            for field in field_catalogue.get('fields', [])
        }

        logger.info(f"Excel columns: {list(df.columns)}")
        logger.info(f"Catalogue fields: {list(catalogue_fields_lower.values())}")

        # First, identify sales data columns by matching patterns
        sales_columns_found = set()
        for sales_field, patterns in ExcelUploadService.SALES_DATA_PATTERNS.items():
            for pattern in patterns:
                if pattern in df_columns_lower:
                    sales_data_mapping[sales_field] = df_columns_lower[pattern]
                    sales_columns_found.add(df_columns_lower[pattern])
                    logger.info(f"Mapped sales field '{sales_field}' to column '{df_columns_lower[pattern]}'")
                    break

        # Check for required sales fields
        if 'date' not in sales_data_mapping:
            raise ValidationException(
                f"Missing required 'date' column. Expected one of: {ExcelUploadService.SALES_DATA_PATTERNS['date']}"
            )
        
        if 'quantity' not in sales_data_mapping:
            raise ValidationException(
                f"Missing required 'quantity' column. Expected one of: {ExcelUploadService.SALES_DATA_PATTERNS['quantity']}"
            )

        # Set default UoM if not found
        if 'uom' not in sales_data_mapping:
            sales_data_mapping['uom'] = None  # Will use default value "EACH"

        # Map remaining columns to catalogue fields (case-insensitive matching)
        for excel_col in df.columns:
            if excel_col not in sales_columns_found:
                excel_col_lower = excel_col.lower().strip()
                
                # Try to find matching catalogue field
                if excel_col_lower in catalogue_fields_lower:
                    # Map catalogue field name to Excel column name
                    catalogue_field_name = catalogue_fields_lower[excel_col_lower]
                    master_data_mapping[catalogue_field_name] = excel_col
                    logger.info(f"Mapped master field '{catalogue_field_name}' to Excel column '{excel_col}'")
                else:
                    # Excel column not in catalogue - skip it with warning
                    logger.warning(f"Excel column '{excel_col}' not found in field catalogue - skipping")

        logger.info(f"Master data column mapping: {master_data_mapping}")
        logger.info(f"Sales data mapping: {sales_data_mapping}")

        return master_data_mapping, sales_data_mapping

    @staticmethod
    def clean_value(value: Any) -> Optional[str]:
        """Clean and normalize a value from Excel."""
        if pd.isna(value):
            return None
        
        value_str = str(value).strip()
        
        # Treat empty strings as None
        if value_str == '' or value_str.lower() in ('nan', 'null', 'none'):
            return None
            
        return value_str

    @staticmethod
    def extract_master_data(
        row_data: Dict[str, Any],
        master_data_mapping: Dict[str, str],
        field_catalogue: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract and clean master data from a row.
        Allows NULL values for all characteristics.
        
        Args:
            row_data: Raw row data from Excel
            master_data_mapping: Dict mapping catalogue field names to Excel column names
            field_catalogue: Field catalogue definition
        """
        master_data = {}

        # Use the mapping to get values with correct field names
        for catalogue_field_name, excel_column_name in master_data_mapping.items():
            raw_value = row_data.get(excel_column_name)
            cleaned_value = ExcelUploadService.clean_value(raw_value)
            
            # Store with catalogue field name (lowercase)
            master_data[catalogue_field_name] = cleaned_value

        logger.debug(f"Extracted master data: {master_data}")
        return master_data

    @staticmethod
    def extract_sales_data(
        row_data: Dict[str, Any],
        sales_mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Extract and validate sales data from a row.
        """
        sales_data = {}

        # Extract date
        date_col = sales_mapping.get('date')
        if date_col and date_col in row_data:
            try:
                date_value = row_data[date_col]
                if pd.isna(date_value):
                    raise ValidationException("Date cannot be empty")
                sales_data['date'] = pd.to_datetime(date_value).date()
            except Exception as e:
                raise ValidationException(f"Invalid date format in column '{date_col}': {str(e)}")
        else:
            raise ValidationException("Date is required for sales data")

        # Extract quantity
        qty_col = sales_mapping.get('quantity')
        if qty_col and qty_col in row_data:
            try:
                qty_value = row_data[qty_col]
                if pd.isna(qty_value):
                    raise ValidationException("Quantity cannot be empty")
                quantity = float(qty_value)
                if quantity <= 0:
                    raise ValidationException("Quantity must be greater than 0")
                sales_data['quantity'] = quantity
            except (ValueError, TypeError) as e:
                raise ValidationException(f"Invalid quantity format in column '{qty_col}': {str(e)}")
        else:
            raise ValidationException("Quantity is required for sales data")

        # Extract UoM (optional)
        uom_col = sales_mapping.get('uom')
        if uom_col and uom_col in row_data and not pd.isna(row_data[uom_col]):
            uom_value = str(row_data[uom_col]).strip()
            if len(uom_value) > 20:
                uom_value = uom_value[:20]
            sales_data['uom'] = uom_value
        else:
            sales_data['uom'] = 'EACH'  # Default UoM

        # Extract unit price (optional)
        price_col = sales_mapping.get('unit_price')
        if price_col and price_col in row_data and not pd.isna(row_data[price_col]):
            try:
                sales_data['unit_price'] = float(row_data[price_col])
            except (ValueError, TypeError):
                logger.warning(f"Invalid unit price format, skipping: {row_data[price_col]}")
                sales_data['unit_price'] = None
        else:
            sales_data['unit_price'] = None

        return sales_data

    @staticmethod
    def find_or_create_master_record(
        cursor,
        tenant_id: str,
        master_data: Dict[str, Any],
        user_email: str
    ) -> str:
        """
        Find existing master data record or create new one.
        Matches based on ALL characteristic values (including NULLs).
        """
        # Build WHERE clause that properly handles NULL values
        where_conditions = []
        values = []

        for field, value in master_data.items():
            if value is None or value == '':
                where_conditions.append(f'"{field}" IS NULL')
            else:
                where_conditions.append(f'"{field}" = %s')
                values.append(value)

        # Always include tenant_id in the match
        where_conditions.append('tenant_id = %s')
        values.append(tenant_id)

        if not where_conditions:
            # Shouldn't happen, but create a minimal record if it does
            master_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO master_data (master_id, tenant_id, created_at, created_by)
                VALUES (%s, %s, %s, %s)
                RETURNING master_id
            """, (master_id, tenant_id, datetime.utcnow(), user_email))
            return cursor.fetchone()[0]

        # Try to find existing record
        where_clause = ' AND '.join(where_conditions)
        query = f"SELECT master_id FROM master_data WHERE {where_clause}"
        
        logger.debug(f"Searching for master record with query: {query}")
        logger.debug(f"Values: {values}")
        
        cursor.execute(query, values)
        result = cursor.fetchone()

        if result:
            logger.debug(f"Found existing master record: {result[0]}")
            return result[0]

        # Create new master record
        master_id = str(uuid.uuid4())
        columns = ['master_id', 'tenant_id', 'created_at', 'created_by']
        insert_values = [master_id, tenant_id, datetime.utcnow(), user_email]

        # Add master data fields
        for field, value in master_data.items():
            columns.append(f'"{field}"')
            insert_values.append(value)

        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(insert_values))

        insert_query = f"""
            INSERT INTO master_data ({columns_str})
            VALUES ({placeholders})
            RETURNING master_id
        """
        
        logger.debug(f"Creating new master record with query: {insert_query}")
        cursor.execute(insert_query, insert_values)
        
        new_master_id = cursor.fetchone()[0]
        logger.info(f"Created new master record: {new_master_id}")
        return new_master_id

    @staticmethod
    def process_single_row(
        cursor,
        tenant_id: str,
        row_dict: Dict[str, Any],
        master_data_mapping: Dict[str, str],
        sales_mapping: Dict[str, str],
        field_catalogue: Dict[str, Any],
        user_email: str
    ) -> None:
        """
        Process a single row within its own savepoint for error isolation.
        """
        # Extract master data
        master_data = ExcelUploadService.extract_master_data(
            row_dict, master_data_mapping, field_catalogue
        )

        # Find or create master data record
        master_id = ExcelUploadService.find_or_create_master_record(
            cursor, tenant_id, master_data, user_email
        )

        # Extract sales data
        sales_data = ExcelUploadService.extract_sales_data(
            row_dict, sales_mapping
        )

        # Insert sales data
        sales_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO sales_data 
            (sales_id, tenant_id, master_id, date, quantity, uom, unit_price, created_at, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            sales_id,
            tenant_id,
            master_id,
            sales_data['date'],
            sales_data['quantity'],
            sales_data['uom'],
            sales_data.get('unit_price'),
            datetime.utcnow(),
            user_email
        ))

    @staticmethod
    def process_mixed_data_upload(
        tenant_id: str,
        database_name: str,
        df: pd.DataFrame,
        field_catalogue: Dict[str, Any],
        user_email: str
    ) -> Tuple[int, int, List[Dict[str, Any]]]:
        """
        Process mixed data upload (master data + sales data in single file).
        Automatically distributes data between master_data and sales_data tables.
        Uses savepoints for row-level error isolation.
        """
        db_manager = get_db_manager()
        success_count = 0
        failed_count = 0
        errors = []

        # Identify column types
        master_data_mapping, sales_mapping = ExcelUploadService.identify_column_types(
            df, field_catalogue
        )

        logger.info(f"Processing {len(df)} rows with mixed data distribution")

        with db_manager.get_connection(tenant_id) as conn:
            cursor = conn.cursor()
            try:
                for idx, row in df.iterrows():
                    # Create a savepoint for this row
                    savepoint_name = f"row_{idx}"
                    
                    try:
                        cursor.execute(f"SAVEPOINT {savepoint_name}")
                        
                        row_dict = row.to_dict()
                        
                        # Process the row
                        ExcelUploadService.process_single_row(
                            cursor=cursor,
                            tenant_id=tenant_id,
                            row_dict=row_dict,
                            master_data_mapping=master_data_mapping,
                            sales_mapping=sales_mapping,
                            field_catalogue=field_catalogue,
                            user_email=user_email
                        )
                        
                        # Release savepoint on success
                        cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        success_count += 1
                        
                        if success_count % 100 == 0:
                            logger.info(f"Processed {success_count} rows successfully...")

                    except Exception as e:
                        # Rollback to savepoint on error
                        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        
                        failed_count += 1
                        error_msg = str(e)
                        errors.append({
                            'row': idx + 2,  # +2 because Excel is 1-indexed and has header
                            'error': error_msg
                        })
                        logger.error(f"Error processing row {idx + 2}: {error_msg}")

                # Commit the transaction
                conn.commit()
                logger.info(f"Mixed data upload completed: {success_count} success, {failed_count} failed")

            except Exception as e:
                conn.rollback()
                logger.error(f"Fatal error during mixed data upload: {str(e)}")
                raise DatabaseException(f"Database error during mixed data upload: {str(e)}")
            finally:
                cursor.close()

        return success_count, failed_count, errors
    
    @staticmethod
    def validate_master_data_table_exists(
        database_name: str,
        field_catalogue: Dict[str, Any]
    ) -> bool:
        """
        Validate that master_data table exists and has correct structure.
        
        Args:
            database_name: Tenant's database name
            field_catalogue: Field catalogue definition
            
        Returns:
            True if table exists and is valid
            
        Raises:
            ValidationException: If table is invalid
        """
        db_manager = get_db_manager()
        
        try:
            with db_manager.get_tenant_connection(database_name) as conn:
                cursor = conn.cursor()
                try:
                    # Check if table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = 'master_data'
                        )
                    """)
                    
                    if not cursor.fetchone()[0]:
                        raise ValidationException("master_data table does not exist")
                    
                    # Check if table has expected columns
                    cursor.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'master_data'
                    """)
                    
                    existing_columns = {row[0] for row in cursor.fetchall()}
                    
                    # Verify critical columns exist
                    required_columns = {'master_id', 'tenant_id', 'created_at', 'created_by'}
                    if not required_columns.issubset(existing_columns):
                        raise ValidationException(
                            f"master_data table missing required columns: {required_columns - existing_columns}"
                        )
                    
                    return True
                    
                finally:
                    cursor.close()
                    
        except ValidationException:
            raise
        except Exception as e:
            logger.error(f"Failed to validate master_data table: {str(e)}")
            raise ValidationException(f"Table validation failed: {str(e)}")


    @staticmethod
    def upload_excel_file(
        tenant_id: str,
        database_name: str,
        file_content: bytes,
        file_name: str,
        upload_type: str,
        catalogue_id: Optional[str],
        user_email: str
    ) -> ExcelUploadResponse:
        """
        Main method to upload and process Excel file.
        Supports automatic distribution for 'mixed_data' upload type.
        """
        upload_id = str(uuid.uuid4())

        # Ensure upload_history table exists
        try:
            SchemaManager.add_upload_history_table(tenant_id)
        except Exception as e:
            logger.warning(f"Could not ensure upload_history table exists: {str(e)}")

        try:
            # Validate and parse Excel file
            df = ExcelUploadService.validate_excel_file(file_content)
            total_rows = len(df)

            logger.info(f"Processing upload: {file_name} ({total_rows} rows, type: {upload_type})")

            # Process based on upload type
            if upload_type == "mixed_data":
                if not catalogue_id:
                    raise ValidationException("catalogue_id is required for mixed data uploads")

                # Get field catalogue
                field_catalogue = FieldCatalogueService.get_field_catalogue(tenant_id, database_name, catalogue_id)
                
                if field_catalogue['status'] != 'FINALIZED':
                    raise ValidationException("Field catalogue must be finalized before uploading data")

                success_count, failed_count, errors = ExcelUploadService.process_mixed_data_upload(
                    tenant_id, database_name, df, field_catalogue, user_email
                )
            else:
                raise ValidationException(
                    f"Unsupported upload type: {upload_type}. Use 'mixed_data' for automatic distribution."
                )

            # Log upload in database
            db_manager = get_db_manager()
            with db_manager.get_connection(tenant_id) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO upload_history
                        (upload_id, tenant_id, upload_type, file_name, total_rows, 
                         success_count, failed_count, status, uploaded_at, uploaded_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        upload_id,
                        tenant_id,
                        upload_type,
                        file_name,
                        total_rows,
                        success_count,
                        failed_count,
                        'completed',
                        datetime.utcnow(),
                        user_email
                    ))
                    conn.commit()
                finally:
                    cursor.close()

            logger.info(f"Excel upload completed: {upload_id}, success: {success_count}, failed: {failed_count}")

            return ExcelUploadResponse(
                upload_id=upload_id,
                tenant_id=tenant_id,
                upload_type=upload_type,
                file_name=file_name,
                total_rows=total_rows,
                success_count=success_count,
                failed_count=failed_count,
                status='completed',
                errors=errors[:100] if errors else [],  # Return first 100 errors
                uploaded_at=datetime.utcnow(),
                uploaded_by=user_email
            )


        except Exception as e:
            # Log failed upload
            try:
                db_manager = get_db_manager()
                with db_manager.get_connection(tenant_id) as conn:
                    cursor = conn.cursor()
                    try:
                        # Check if upload_id already exists to avoid duplicate key violation
                        cursor.execute(
                            "SELECT 1 FROM upload_history WHERE upload_id = %s",
                            (upload_id,)
                        )
                        if not cursor.fetchone():
                            cursor.execute("""
                                INSERT INTO upload_history
                                (upload_id, tenant_id, upload_type, file_name, total_rows,
                                 success_count, failed_count, status, uploaded_at, uploaded_by)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                upload_id,
                                tenant_id,
                                upload_type,
                                file_name,
                                0,
                                0,
                                0,
                                'failed',
                                datetime.utcnow(),
                                user_email
                            ))
                            conn.commit()
                    finally:
                        cursor.close()
            except Exception:
                pass  # Don't let logging failure mask original error

            if isinstance(e, (ValidationException, DatabaseException)):
                raise
            raise ValidationException(f"Upload failed: {str(e)}")