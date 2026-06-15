import os


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('METRICS_UTILITY_DB_NAME', 'awx'),
        'USER': os.getenv('METRICS_UTILITY_DB_USER', 'awx'),
        'PASSWORD': os.getenv('METRICS_UTILITY_DB_PASSWORD', 'awx'),
        'HOST': os.getenv('METRICS_UTILITY_DB_HOST', 'localhost'),
        'PORT': os.getenv('METRICS_UTILITY_DB_PORT', '5432'),
    },
    # metrics-service database — same Postgres instance, different DB name/credentials.
    # Override any of these via environment variables:
    #   METRICS_SERVICE_DB_HOST      (defaults to METRICS_UTILITY_DB_HOST / localhost)
    #   METRICS_SERVICE_DB_PORT      (defaults to 5432)
    #   METRICS_SERVICE_DB_NAME      (defaults to metrics_service)
    #   METRICS_SERVICE_DB_USER      (defaults to metrics_service)
    #   METRICS_SERVICE_DB_PASSWORD  (defaults to metrics_service)
    'metrics_service': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('METRICS_SERVICE_DB_NAME', 'metrics_service'),
        'USER': os.getenv('METRICS_SERVICE_DB_USER', 'metrics_service'),
        'PASSWORD': os.getenv('METRICS_SERVICE_DB_PASSWORD', 'metrics_service'),
        'HOST': os.getenv('METRICS_SERVICE_DB_HOST', os.getenv('METRICS_UTILITY_DB_HOST', 'localhost')),
        'PORT': os.getenv('METRICS_SERVICE_DB_PORT', '5432'),
    },
}

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
INSTALLED_APPS = ['awx.conf']
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Collected
AUTHENTICATION_BACKENDS = ('awx.main.backends.AWXModelBackend',)
AUTOMATION_ANALYTICS_LAST_ENTRIES = ''
INSTALL_UUID = '00000000-0000-0000-0000-000000000000'
LOG_AGGREGATOR_ENABLED = False
LOG_AGGREGATOR_LOGGERS = ['awx', 'activity_stream', 'job_events', 'system_tracking', 'broadcast_websocket', 'job_lifecycle']
LOG_AGGREGATOR_TYPE = None
PENDO_TRACKING_STATE = 'off'
SUBSCRIPTION_USAGE_MODEL = ''
SYSTEM_UUID = '00000000-0000-0000-0000-000000000000'
TOWER_URL_BASE = 'https://platformhost'
