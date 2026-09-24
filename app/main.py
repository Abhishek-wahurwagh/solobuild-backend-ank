from contextlib import asynccontextmanager
from fastapi import FastAPI, status
from fastapi.openapi.utils import get_openapi
from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine
from app.core.redis import get_redis_client, close_redis_pool

from app.domains.auth.router import router as auth_router
from app.domains.agents.router import router as agents_router
from app.domains.campaigns.router import router as campaigns_router
from app.domains.telephony.router import router as telephony_router
"""
from app.domains.analytics.router import router as analytics_router
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    # -- Startup --
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1;"))
        print("✅ Database connection established successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Database: {e}")

    # Warm the Redis connection pool
    try:
        redis = await get_redis_client()
        await redis.ping()
        await redis.aclose()
        print("✅ Redis connection established successfully.")
    except Exception as e:
        print(f"⚠️  Redis not available (workers may fail): {e}")

    yield

    # -- Shutdown --
    await close_redis_pool()
    await engine.dispose()
    print("🔌 Database & Redis connections closed.")


app = FastAPI(
    title=settings.PROJECT_NAME,
    debug=settings.DEBUG,
    lifespan=lifespan,
)

# Register Domain Routers under /api/v1
app.include_router(auth_router, prefix="/api/v1", tags=["Auth"])
app.include_router(agents_router, prefix="/api/v1", tags=["Agents"])
app.include_router(campaigns_router, prefix="/api/v1", tags=["Campaigns"])
app.include_router(telephony_router, prefix="/api/v1", tags=["Telephony"])
"""
app.include_router(analytics_router, prefix="/api/v1", tags=["Analytics"])
"""

# Root & Health Check Endpoints

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Workaround for Swagger UI multiple file upload bug in OpenAPI 3.1.0
    # It renders array of strings instead of file upload button unless we use `format: binary`
    for path in openapi_schema.get("paths", {}).values():
        for method in path.values():
            content = method.get("requestBody", {}).get("content", {})
            if "multipart/form-data" in content:
                schema = content["multipart/form-data"].get("schema", {})
                if "$ref" in schema:
                    ref_name = schema["$ref"].split("/")[-1]
                    comp = openapi_schema["components"]["schemas"].get(ref_name, {})
                    for prop_val in comp.get("properties", {}).values():
                        if prop_val.get("type") == "array" and prop_val.get("items", {}).get("type") == "string":
                            prop_val["items"]["format"] = "binary"
                            prop_val["items"].pop("contentMediaType", None)

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
@app.get("/", tags=["System"])
async def root():
    return {"message": f"Welcome to {settings.PROJECT_NAME} API"}


@app.get("/api/v1/health/db", tags=["System"])
async def health_check():
    """Health check endpoint for DB ping verification."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1;"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}