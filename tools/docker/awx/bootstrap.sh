#!/bin/bash
# AWX container entrypoint - prepare database & instance, then run supervisord
set -euo pipefail

cd /awx_devel

# register the mounted checkout as the "awx" distribution (entry points, version)
if [ ! -d awx.egg-info ]; then
  python3 - << 'EOF'
import setuptools
from setuptools.command.egg_info import egg_info

class egg_info_dev(egg_info):
    def find_sources(self):
        pass  # skip MANIFEST.in parsing, not needed for a dev checkout

setuptools.setup(script_name='setup.py', script_args=['egg_info_dev'], cmdclass={'egg_info_dev': egg_info_dev})
EOF
fi

awx-manage migrate --noinput
awx-manage collectstatic --clear --noinput > /dev/null

# no UI build in here, the api still expects an index
if [ ! -d awx/ui/build/awx ]; then
  mkdir -p awx/ui/build/awx
  cp awx/ui/placeholder_index_awx.html awx/ui/build/awx/index_awx.html
fi

# always admin:admin, even if the user was changed since
ANSIBLE_REVERSE_RESOURCE_SYNC=false DJANGO_SUPERUSER_PASSWORD=admin \
  awx-manage createsuperuser --noinput --username=admin --email=admin@localhost 2> /dev/null || true
ANSIBLE_REVERSE_RESOURCE_SYNC=false awx-manage shell -c "
from django.contrib.auth.models import User
u = User.objects.get(username='admin')
u.set_password('admin')
u.is_superuser = True
u.is_active = True
u.save()
"
echo "Admin user: admin:admin"

ANSIBLE_REVERSE_RESOURCE_SYNC=false awx-manage create_preload_data
awx-manage register_default_execution_environments

HOST="$(uname -n)"
awx-manage provision_instance --hostname="$HOST" --node_type=hybrid
awx-manage add_receptor_address --instance="$HOST" --address="$HOST" --port=2222 --canonical
# only this instance - mock data instances would get jobs assigned and never run them
awx-manage register_queue --queuename=controlplane --hostnames="$HOST"
awx-manage register_queue --queuename=default --hostnames="$HOST"

exec supervisord -n -c /etc/supervisord.conf
