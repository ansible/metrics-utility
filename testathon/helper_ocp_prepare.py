# Accepts token and server url and creates env variables

# We know OC_LOGIN_COMMAND, now we wants to create OC_COMMAND
# Get namespace, begins with aap, run subprocess

import os
import subprocess


def get_aap_namespace():
    """Get namespace that begins with 'aap' using oc command"""
    try:
        # Run oc get namespaces command and filter for those starting with 'aap'
        result = subprocess.run(
            ['oc', 'get', 'namespaces', '--no-headers', '-o', 'custom-columns=NAME:.metadata.name'], capture_output=True, text=True, check=True
        )

        # Filter namespaces that start with 'aap'
        namespaces = result.stdout.strip().split('\n')
        aap_namespaces = [ns for ns in namespaces if ns.startswith('aap')]

        if aap_namespaces:
            # Return the first aap namespace found
            return aap_namespaces[0]
        else:
            print("No namespace starting with 'aap' found")
            return None

    except subprocess.CalledProcessError as e:
        print(f'Error running oc command: {e}')
        return None
    except FileNotFoundError:
        print('oc command not found. Make sure OpenShift CLI is installed and in PATH')
        return None


def get_controller_pod_name(aap_namespace):
    """Get controller pod name using oc command"""

    result = subprocess.run(['oc', 'get', 'pods', '-n', aap_namespace], capture_output=True, text=True, check=True)
    pods_text = result.stdout.strip()

    # split them into lines
    pods = pods_text.split('\n')

    for pod in pods:
        # search for pod name that contains 'controller-task'
        if 'controller-task' in pod:
            # select only the pod name - first word
            pod_name = pod.split()[0]
            print(f'pod_name found: {pod_name}')
            return pod_name

    print('No controller pod found')
    return None


def create_oc_command():
    """Create OC_COMMAND environment variable with the aap namespace"""
    aap_namespace = get_aap_namespace()

    print(f'aap_namespace found: {aap_namespace}')

    pod_name = get_controller_pod_name(aap_namespace)

    print(f'pod_name found: {pod_name}')

    # create OC_COMMAND
    oc_command = f'oc exec -n {aap_namespace} {pod_name}'
    print(f'OC_COMMAND: {oc_command}')

    # set env variable
    os.environ['OC_COMMAND'] = oc_command


if __name__ == '__main__':
    create_oc_command()
