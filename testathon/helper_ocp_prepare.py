import os
import subprocess


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


def create_oc_environs():
    """Create POD_NAME and NAMESPACE environment variables with the aap namespace"""
    aap_namespace = get_aap_namespace()

    print(f'aap_namespace found: {aap_namespace}')

    pod_name = get_controller_pod_name(aap_namespace)

    print(f'pod_name found: {pod_name}')

    # create POD_NAME and NAMESPACE
    os.environ['POD_NAME'] = pod_name
    os.environ['NAMESPACE'] = aap_namespace

    print(f'POD_NAME: {os.getenv("POD_NAME")}')
    print(f'NAMESPACE: {os.getenv("NAMESPACE")}')
