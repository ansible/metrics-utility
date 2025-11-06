import os


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'awx',
        'USER': 'myuser',
        'PASSWORD': 'mypassword',
        'HOST': os.getenv('METRICS_UTILITY_DB_HOST', 'localhost'),
        'PORT': '5432',
    }
}
DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'
INSTALLED_APPS = ['awx.conf']
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# collected but also used by Package
INSTALL_UUID = '00000000-0000-0000-0000-000000000000'
SYSTEM_UUID = '00000000-0000-0000-0000-000000000000'
