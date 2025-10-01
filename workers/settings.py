from dataclasses import dataclass, field


@dataclass
class Settings:
    controller_db: dict = field(default_factory=dict)
    metrics_db: dict = field(default_factory=dict)

    crc_storage: dict = field(default_factory=dict)
    s3_storage: dict = field(default_factory=dict)

    retention: dict = field(default_factory=dict)


settings = Settings()
