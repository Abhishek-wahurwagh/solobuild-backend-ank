from contextlib import asynccontextmanager
from fastapi import FastAPI, status
from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine

from app.domains.auth.router import router as auth_router
from app.domains.agents.router import router as agents_router
"""
from app.domains.campaigns.router import router as campaigns_router
from app.domains.telephony.router import router as telephony_router
from app.domains.analytics.router import router as analytics_router
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1;"))
        print("✅ Database connection established successfully.")
    except Exception as e:
        print(f"❌ Failed to connect to Database: {e}")

    yield

    # Shutdown logic
    await engine.dispose()
    print("🔌 Database engine connections closed.")


app = FastAPI(
    title=settings.PROJECT_NAME,
    debug=settings.DEBUG,
    lifespan=lifespan,
)

# Register Domain Routers under /api/v1
app.include_router(auth_router, prefix="/api/v1", tags=["Auth"])
app.include_router(agents_router, prefix="/api/v1/agents", tags=["Agents"])
"""
app.include_router(campaigns_router, prefix="/api/v1/campaigns", tags=["Campaigns"])
app.include_router(telephony_router, prefix="/api/v1/telephony", tags=["Telephony"])
app.include_router(analytics_router, prefix="/api/v1/analytics", tags=["Analytics"])
"""

# Root & Health Check Endpoints
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