import os

from dataclasses import dataclass, field


@dataclass
class Settings:
    controller_db: dict = field(default_factory=dict)
    metrics_db: dict = field(default_factory=dict)

    crc_storage: dict = field(default_factory=dict)
    s3_storage: dict = field(default_factory=dict)

    retention: dict = field(default_factory=dict)


settings = Settings(
    controller_db={
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'awx',
        'USER': 'myuser',
        'PASSWORD': 'mypassword',
        'HOST': os.getenv('METRICS_UTILITY_DB_HOST', 'localhost'),
        'PORT': '5432',
    },
    metrics_db={
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'metrics_service',
        'USER': 'metrics_service',
        'PASSWORD': 'metrics_service',
        'HOST': os.getenv('METRICS_UTILITY_DB_HOST', 'localhost'),
        'PORT': '5432',
    },
)
