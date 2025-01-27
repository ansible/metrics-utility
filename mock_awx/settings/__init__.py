DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "awx",
        "USER": "myuser",
        "PASSWORD": "mypassword",
        "HOST": "localhost",
        "PORT": "5432",
    }
}

INSTALLED_APPS = ['awx.conf']

# Fixes TypeError: can't compare offset-naive and offset-aware datetimes, when comparing to django.utils.timezone.now
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Collected
AUTHENTICATION_BACKENDS = ('awx.main.backends.AWXModelBackend',) #FIXME mock?, empty?
AUTOMATION_ANALYTICS_LAST_ENTRIES = ''
INSTALL_UUID = '00000000-0000-0000-0000-000000000000'
LOG_AGGREGATOR_ENABLED = False
LOG_AGGREGATOR_LOGGERS =['awx', 'activity_stream', 'job_events', 'system_tracking', 'broadcast_websocket', 'job_lifecycle']
LOG_AGGREGATOR_TYPE = None
PENDO_TRACKING_STATE = "off"
SUBSCRIPTION_USAGE_MODEL = ''
SYSTEM_UUID = '00000000-0000-0000-0000-000000000000'
TOWER_URL_BASE = "https://platformhost"
