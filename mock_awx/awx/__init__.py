import os


def prepare_env():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
