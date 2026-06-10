from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseEnvConfig(BaseSettings):
    minio_access_key: str
    minio_secret_key: str
    minio_endpoint: str

    model_config = SettingsConfigDict(env_file=".env")


class BronzeEnvConfig(BaseEnvConfig):
    schema_registry_url: str
    kafka_bootstrap_server: str


class SilverEnvConfig(BaseEnvConfig):
    pass
