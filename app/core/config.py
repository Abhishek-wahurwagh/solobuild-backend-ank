from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "SoloBuildAI"
    ENVIRONMENT: str = "development"
    DEBUG: bool = False
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""
    POSTGRES_HOST: str = ""
    POSTGRES_PORT: int = 5432
    DATABASE_URL: str = ""

    AWS_BUCKET_NAME: str = ""
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = ""

    AI_EMBEDDING_PROVIDER: str = "gemini"
    GEMINI_API_KEY: str = "your-gemini-key"
    GEMINI_EMBEDDING_MODEL: str = "models/text-embedding-004"
    GEMINI_EXTRACTION_MODEL: str = "gemini-1.5-flash"
    LLM_REQUEST_TIMEOUT_SECONDS: int = 180
    LLM_MAX_RETRIES: int = 2
    LLM_RETRY_DELAY_SECONDS: float = 2.0
    WORKER_MAX_TRIES: int = 3
    ARQ_JOB_TIMEOUT_SECONDS: int = 1800

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # Voice & Telephony
    VOICE_PROVIDER: str = "mock"
    VOBIZ_API_KEY: str = ""
    PIPECAT_WEBHOOK_URL: str = ""

    # Resume pipeline
    RESUME_MAX_FILE_COUNT: int = 500

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )


settings = Settings()