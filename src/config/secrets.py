# src/config/secrets.py

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def env_path(filename: str = ".env") -> Path:
    """
    Returns an absolute Path to the env file.

    This resolves relative to this file's directory so it works
    regardless of where you run the app from.
    """
    return Path(__file__).resolve().parent / filename


class Settings(BaseSettings):
    """
    Project settings loaded from environment variables and an optional .env file.
    """

    API_KEY: str
    API_BASE_URL: str

    model_config = SettingsConfigDict(
        env_file=env_path(".env"),
        env_file_encoding="utf-8",
        extra="ignore",          # ignore any unexpected env vars
        case_sensitive=False,    # set True if you want strict casing
    )


settings = Settings()