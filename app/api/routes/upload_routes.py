"""
Excel upload API routes.
Endpoints for uploading Excel files with automatic data distribution.
"""

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
from typing import Dict, Any
from app.core.excel_upload_service import ExcelUploadService
from app.core.responses import ResponseHandler
from app.core.exceptions import AppException
from app.api.dependencies import get_current_tenant
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/excel", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
async def upload_excel_file(
    file: UploadFile = File(...),
    upload_type: str = Form(..., pattern="^(mixed_data)$"),
    catalogue_id: str = Form(...),
    tenant_data: Dict = Depends(get_current_tenant)
):

    try:
        # Validate file type
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only Excel files (.xlsx, .xls) are allowed"
            )

        # Validate catalogue_id
        if not catalogue_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="catalogue_id is required for data uploads"
            )

        # Read file content
        file_content = await file.read()

        # Process upload
        result = ExcelUploadService.upload_excel_file(
            tenant_id=tenant_data["tenant_id"],
            database_name=tenant_data["database_name"],
            file_content=file_content,
            file_name=file.filename,
            upload_type=upload_type,
            catalogue_id=catalogue_id,
            user_email=tenant_data["email"]
        )

        return ResponseHandler.success(data=result.model_dump(), status_code=201)

    except AppException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Unexpected error in upload_excel_file: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/history", response_model=Dict[str, Any])
async def get_upload_history(
    tenant_data: Dict = Depends(get_current_tenant),
    page: int = 1,
    page_size: int = 50
):
    """
    Get upload history for the tenant.

    - **page**: Page number (default: 1)
    - **page_size**: Records per page (default: 50, max: 100)
    """
    try:
        if page_size > 100:
            page_size = 100

        db_manager = ExcelUploadService.get_db_manager()
        offset = (page - 1) * page_size

        with db_manager.get_connection(tenant_data["tenant_id"]) as conn:
            cursor = conn.cursor()
            try:
                # Get total count
                cursor.execute(
                    "SELECT COUNT(*) FROM upload_history WHERE tenant_id = %s",
                    (tenant_data["tenant_id"],)
                )
                total_count = cursor.fetchone()[0]

                # Get paginated results
                cursor.execute("""
                    SELECT upload_id, tenant_id, upload_type, file_name, total_rows,
                           success_count, failed_count, status, uploaded_at, uploaded_by
                    FROM upload_history
                    WHERE tenant_id = %s
                    ORDER BY uploaded_at DESC
                    LIMIT %s OFFSET %s
                """, (tenant_data["tenant_id"], page_size, offset))

                uploads = []
                for row in cursor.fetchall():
                    upload_id, ten_id, upload_type, file_name, total_rows, success_count, failed_count, status, uploaded_at, uploaded_by = row
                    uploads.append({
                        "upload_id": upload_id,
                        "tenant_id": ten_id,
                        "upload_type": upload_type,
                        "file_name": file_name,
                        "total_rows": total_rows,
                        "success_count": success_count,
                        "failed_count": failed_count,
                        "status": status,
                        "uploaded_at": uploaded_at.isoformat() if uploaded_at else None,
                        "uploaded_by": uploaded_by
                    })

                return ResponseHandler.list_response(
                    data=uploads,
                    page=page,
                    page_size=page_size,
                    total_count=total_count
                )

            finally:
                cursor.close()

    except AppException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Unexpected error in get_upload_history: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/history/{upload_id}", response_model=Dict[str, Any])
async def get_upload_details(
    upload_id: str,
    tenant_data: Dict = Depends(get_current_tenant)
):
    """
    Get detailed information about a specific upload.

    - **upload_id**: Upload identifier
    """
    try:
        db_manager = ExcelUploadService.get_db_manager()

        with db_manager.get_connection(tenant_data["tenant_id"]) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT upload_id, tenant_id, upload_type, file_name, total_rows,
                           success_count, failed_count, status, uploaded_at, uploaded_by
                    FROM upload_history
                    WHERE upload_id = %s AND tenant_id = %s
                """, (upload_id, tenant_data["tenant_id"]))

                result = cursor.fetchone()
                if not result:
                    raise HTTPException(status_code=404, detail="Upload not found")

                upload_id, ten_id, upload_type, file_name, total_rows, success_count, failed_count, status, uploaded_at, uploaded_by = result

                upload_details = {
                    "upload_id": upload_id,
                    "tenant_id": ten_id,
                    "upload_type": upload_type,
                    "file_name": file_name,
                    "total_rows": total_rows,
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "status": status,
                    "uploaded_at": uploaded_at.isoformat() if uploaded_at else None,
                    "uploaded_by": uploaded_by
                }

                return ResponseHandler.success(data=upload_details)

            finally:
                cursor.close()

    except HTTPException:
        raise
    except AppException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Unexpected error in get_upload_details: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")