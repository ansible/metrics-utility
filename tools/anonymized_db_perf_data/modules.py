"""
Module names for performance testing.

This file contains a comprehensive list of Ansible modules from various
collection sources (certified, validated, and community) to generate
realistic performance test data.
"""

MODULES = [
    # Red Hat Certified - ansible.builtin
    'ansible.builtin.copy',
    'ansible.builtin.file',
    'ansible.builtin.template',
    'ansible.builtin.service',
    'ansible.builtin.yum',
    'ansible.builtin.apt',
    'ansible.builtin.command',
    'ansible.builtin.shell',
    'ansible.builtin.debug',
    'ansible.builtin.set_fact',
    'ansible.builtin.include_tasks',
    'ansible.builtin.user',
    'ansible.builtin.group',
    'ansible.builtin.lineinfile',
    'ansible.builtin.stat',
    'ansible.builtin.git',
    'ansible.builtin.package',
    'ansible.builtin.systemd',
    'ansible.builtin.dnf',
    'ansible.builtin.cron',
    'ansible.builtin.uri',
    'ansible.builtin.get_url',
    'ansible.builtin.fetch',
    'ansible.builtin.wait_for',
    # Red Hat Certified - ansible.posix
    'ansible.posix.mount',
    'ansible.posix.sysctl',
    'ansible.posix.firewalld',
    'ansible.posix.selinux',
    'ansible.posix.authorized_key',
    # Red Hat Certified - redhat.*
    'redhat.rhel_system_roles.network',
    'redhat.rhel_system_roles.firewall',
    'redhat.rhel_system_roles.selinux',
    'redhat.satellite.activation_key',
    'redhat.satellite.repository',
    # Validated - AWS (certified partner)
    'amazon.aws.ec2_instance',
    'amazon.aws.s3_bucket',
    'amazon.aws.ec2_vpc_subnet',
    'amazon.aws.ec2_security_group',
    'amazon.aws.rds_instance',
    'amazon.aws.iam_role',
    # Validated - Azure (certified partner)
    'azure.azcollection.azure_rm_virtualmachine',
    'azure.azcollection.azure_rm_storageaccount',
    'azure.azcollection.azure_rm_networkinterface',
    'azure.azcollection.azure_rm_securitygroup',
    # Validated - Google Cloud (certified partner)
    'google.cloud.gcp_compute_instance',
    'google.cloud.gcp_storage_bucket',
    'google.cloud.gcp_compute_disk',
    # Validated - VMware (certified partner)
    'vmware.vmware_rest.vcenter_vm',
    'vmware.vmware_rest.vcenter_datastore',
    # Validated - Cisco (certified partner)
    'cisco.iosxr.iosxr_config',
    'cisco.nxos.nxos_config',
    'cisco.ios.ios_command',
    # Validated - F5 (certified partner)
    'f5networks.f5_modules.bigip_virtual_server',
    'f5networks.f5_modules.bigip_pool',
    # Community - community.general
    'community.general.docker_container',
    'community.general.docker_image',
    'community.general.docker_network',
    'community.general.postgresql_db',
    'community.general.git_config',
    'community.general.jenkins_job',
    'community.general.npm',
    'community.general.alternatives',
    'community.general.timezone',
    'community.general.hostname',
    'community.general.lvg',
    'community.general.lvol',
    # Community - community.mysql
    'community.mysql.mysql_db',
    'community.mysql.mysql_user',
    'community.mysql.mysql_replication',
    # Community - community.postgresql
    'community.postgresql.postgresql_db',
    'community.postgresql.postgresql_user',
    'community.postgresql.postgresql_query',
    # Community - community.docker
    'community.docker.docker_container',
    'community.docker.docker_compose',
    'community.docker.docker_swarm',
    # Community - community.kubernetes
    'community.kubernetes.k8s',
    'community.kubernetes.helm',
    'community.kubernetes.k8s_info',
    # Community - community.vmware
    'community.vmware.vmware_guest',
    'community.vmware.vmware_host',
    'community.vmware.vmware_datacenter',
    # Community - community.windows
    'community.windows.win_domain',
    'community.windows.win_iis_website',
    'community.windows.win_scheduled_task',
]
