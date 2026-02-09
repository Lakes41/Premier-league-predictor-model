# src/config/secrets.py

from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


def project_root() -> Path:
    """
    Project root = folder that contains 'src/'.
    This file is at: <root>/src/config/secrets.py
    So parents[2] is the project root.
    """
    return Path(__file__).resolve().parents[2]


ENV_PATH = project_root() / ".env"

# Deterministic: load .env into process env once, before Settings() is created
load_dotenv(dotenv_path=ENV_PATH, override=False)


class Settings(BaseSettings):
    API_KEY: str
    API_BASE_URL: str

    model_config = SettingsConfigDict(
        # Keep this too, but load_dotenv is the hard guarantee
        env_file=str(ENV_PATH),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


settings = Settings()