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

    # Telephony and real-time voice runtime
    TELEPHONY_CARRIER: str = "vobiz"
    APP_BASE_URL: str = "http://localhost:8000"
    VOBIZ_API_BASE_URL: str = "https://api.vobiz.ai/api/v1"
    VOBIZ_AUTH_ID: str = ""
    VOBIZ_AUTH_TOKEN: str = ""
    VOBIZ_PHONE_NUMBER: str = ""
    VOBIZ_ANSWER_PATH: str = "/api/v1/telephony/vobiz/answer"
    VOBIZ_RECORDING_PATH: str = "/api/v1/telephony/vobiz/recording-ready"
    VOBIZ_MEDIA_PATH: str = "/api/v1/telephony/vobiz/media"
    VOBIZ_REQUEST_TIMEOUT_SECONDS: float = 15.0
    TELEPHONY_WEBHOOK_SECRET: str = ""
    TELEPHONY_WEBHOOK_HEADER: str = "X-Telephony-Signature"
    VOBIZ_WEBHOOK_SECRET: str = ""
    PIPECAT_LLM_PROVIDER: str = "gemini"
    PIPECAT_LLM_MODEL: str = ""
    PIPECAT_STT_PROVIDER: str = "gemini"
    PIPECAT_TTS_PROVIDER: str = "gemini"
    PIPECAT_STT_MODEL: str = "gpt-4o-mini-transcribe"
    PIPECAT_TTS_MODEL: str = "gpt-4o-mini-tts"
    PIPECAT_TTS_VOICE: str = "alloy"

    # Resume & CSV pipeline
    CSV_CHUNK_SIZE: int = 50
    RESUME_MAX_FILE_COUNT: int = 500
    ZIP_MAX_UNCOMPRESSED_BYTES: int = 500 * 1024 * 1024
    ZIP_MAX_RATIO: int = 50

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )


settings = Settings()