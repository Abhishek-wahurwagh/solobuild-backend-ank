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
    OPENAI_API_KEY: str = ""
    OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-small"
    GEMINI_API_KEY: str = "your-gemini-key"
    GEMINI_EMBEDDING_MODEL: str = "models/text-embedding-004"
    GEMINI_EXTRACTION_MODEL: str = "gemini-1.5-flash"

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # Resume pipeline
    RESUME_MAX_FILE_COUNT: int = 500
    PRE_SCREEN_MATCH_THRESHOLD: float = 0.4

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )


settings = Settings()