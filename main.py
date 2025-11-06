"""
Main FastAPI Application.
Entry point for the Smart Demand Planning Tool API.
"""

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import sys

from app.config import settings
from app.core.database import get_db_manager
from app.core.exceptions import AppException

# Import routers
from app.api.routes import auth_routes, field_catalogue_routes, upload_routes

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    logger.info("Starting Smart Demand Planning Tool API")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    
    # Initialize database connection pool
    try:
        get_db_manager()
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Smart Demand Planning Tool API")
    try:
        db_manager = get_db_manager()
        db_manager.close_pool()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Multi-tenant demand planning tool with dynamic field catalogue",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)


# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure based on environment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler
@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    """Handle custom application exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": {
                "code": exc.error_code,
                "message": exc.message,
                "details": exc.details
            },
            "metadata": {
                "timestamp": request.state.timestamp if hasattr(request.state, 'timestamp') else None,
                "status_code": exc.status_code
            }
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred" if not settings.DEBUG else str(exc),
                "details": {}
            },
            "metadata": {
                "timestamp": request.state.timestamp if hasattr(request.state, 'timestamp') else None,
                "status_code": 500
            }
        }
    )


# Request timestamp middleware
@app.middleware("http")
async def add_timestamp(request: Request, call_next):
    """Add timestamp to request state."""
    from datetime import datetime
    request.state.timestamp = datetime.utcnow().isoformat()
    response = await call_next(request)
    return response


# Include routers
app.include_router(auth_routes.router, prefix="/api/v1")
app.include_router(field_catalogue_routes.router, prefix="/api/v1")
app.include_router(upload_routes.router, prefix="/api/v1")


# Health check endpoint
@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT
    }


# Root endpoint
@app.get("/", tags=["System"])
async def root():
    """Root endpoint with API information."""
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/api/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )