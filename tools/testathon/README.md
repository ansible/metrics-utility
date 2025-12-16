# TLDR

For testathon, how to fill data:

BEWARE the script will remove data that may already be there (must implement the flag to insert without deletion).
In your local computer inside root of metrics-utility:

cd tools/testathon

## RPM:

export PASSWORD='Fill here gateway password'
export USERNAME='admin'

--API_URL is url of gateway
export API_URL='https://44.201.90.229'
export SSH_USER='ec2-user'

--URL of controller
export SSH_URL='54.90.173.180'
export ENVIRONMENT=RPM

uv run testathon_data_prepare.py
uv run gather_all.py

You can also run:

uv run build_and_copy.py

This will connect to RPM, runs build_report command and copy output file into the tools/testathon folder.
You can provide arguments, for example uv run build_and_copy.py --month=2025-08

And you can set environment variables, all env variables that begins with METRICS_UTILITY will be also set in remote machine when executing build command remotely

This way, you can test it much easier, otherwise you will need to copy your file back every time you execute build_command

## Containerized:

The same as above, but:

export SSH_USER='ansible'
export ENVIRONMENT=containerized

## Openshift:

First, ensure oc is installed on the machine.

You need to fill this env variable:
OC_LOGIN_COMMAND

How to do it:

### Step 1: Access OpenShift Web Console

Open the hive_cluster_claim.yml file
Locate the hive_cluster_deployment_web_console_url field
Copy the URL (example: https://console-openshift-console.apps.aap-test-v418-x86-64-klfxw.ocp4.testing.ansible.com)
Open the URL in your web browser

### Step 2: Login to OpenShift Console

Username: kubeadmin Password: Retrieve from the hive_cluster_claim_admin_password file

Click Log in

### Step 3: Configure CLI Access

In the OpenShift web console, click on kube:admin (top right corner)
Select Copy login command
A new tab will open - click Display Token
Copy the command under Log in with this token

This is your:
OC_LOGIN_COMMAND env variable

set also:
export ENVIRONMENT=OpenShift

You do not need to specify POD_NAME and NAMESPACE, the script will handle it by itself.
Then again run data prepare and gather all scripts.

# printvars.py

This will print env vars that are used in scripts. Its better than printvars command, more readable.

Variable ENVIRONMENT is:
None (local)
RPM
OpenShift
containerized

# testathon_data_prepare.py

If you run this directly without any parameters, it will prepopulate local environment db, provided that it runs
at this url:

https://localhost:8030/api/controller/v2
and credentials: admin/admin

The script will use API to insert testing data. It also connects directly to DB, deletes all main_jobhostsummary table content
and runs jobs that will populate this table again. It will then run updates to change modified dates of job runs, so users
can test different gather ranges.

You can override url and credentails by environmental variables:
USERNAME, PASSWORD, API_URL, ENVIRONMENT (either set to local, RPM, OpenShift, containerized)

## Local Example:
uv run testathon_data_prepare.py

This will run on local machine when AWX is on.

## RPM build

It can also connect to RMP jenkins build, but you have to provide specific parameters.

Besides USERNAME, PASSWORD, API_URL to gateway api, you need also to specify:

SSH_URL, SSH_USER to controller instance. 

Script will directly connect using SSH and do some modification to DB.

If the server is on VPN, do not forget to connect on VPN.

Script will repeatedly ask for passphrase for SSH key, for example:
Enter passphrase for key '/home/milan/.ssh/id_ed19561'

You can add it before script running:

ssh-add ~/.ssh/id_ed19561

You will find API_URL as gateway URL in inventory artifact of jenkins.

SSH_URL is IP of controller server.

## RPM Example:

export PASSWORD='**Fill here**'
export USERNAME='admin'
export API_URL='https://44.201.90.229/api/controller/v2'

export SSH_USER='ec2-user'
export SSH_URL='54.90.173.180'
export ENVIRONMENT=RPM

uv run testathon_data_prepare.py

## Containerized build

Containerized build runs in one server and each service runs in containers.

You can access it also using SSH and classic URL, the script then connect to the container by itself.

SSH will be again for controller.

You will find it again in inventory file of jenkins artifact.

SSH_USER should be: ansible

## Openshift build

This build is very different. 

First, ensure oc is installed on the machine.

You need to fill this env variable:

OC_LOGIN_COMMAND

Oc login command is used to login only once. It needs token.

Below will be description how to obtain this variable. Now we will show examples:

OC_LOGIN_COMMAND:
oc login --token={token} --server=https://api.aap-test-v418-x86-64-knmrp.ocp4.testing.ansible.com:6443

Follow steps 1-3 below to fill OC_LOGIN_COMMAND

POD_NAME and NAMESPACE is optional env variables, it will be computed on the fly if not empty, otherwise used.

Below is detailed description by Mauricio Magnani and Apurva.

### Step 1: Access OpenShift Web Console

Open the hive_cluster_claim.yml file

Locate the hive_cluster_deployment_web_console_url field

Copy the URL (example: https://console-openshift-console.apps.aap-test-v418-x86-64-klfxw.ocp4.testing.ansible.com)

Open the URL in your web browser

### Step 2: Login to OpenShift Console

Username: kubeadmin
Password: Retrieve from the hive_cluster_claim_admin_password file

Click Log in

### Step 3: Configure CLI Access

In the OpenShift web console, click on kube:admin (top right corner)

Select Copy login command

A new tab will open - click Display Token

Copy the command under Log in with this token

Paste and execute the command in your terminal


### Step 4: Identify AAP Namespace

First, determine what is the AAP namespace. Go to the web console, tab Namespaces. Search for the namespace that
begins by aap (it changes).

For example, it can be aap-wrongly-airedale

Run the following command to list all pods in the AAP namespace:

oc get pods -n aap-wrongly-airedale
You should see output similar to:
NAME                                                              READY   STATUS      RESTARTS   AGE
2c5b7eff00e099e06640bb667ad004d92f6d3f1c8d4e261bf38f6d14956jxnp   0/1     Completed   0          128m
aap-bf37650a-controller-migration-4.6.19-xpxfc                    0/1     Completed   0          121m
aap-bf37650a-controller-task-7798677c6c-zxmgz                     4/4     Running     0          122m
aap-bf37650a-controller-web-bb766d678-fqq78                       3/3     Running     0          122m
aap-bf37650a-eda-activation-worker-54885997c5-l2mj9               1/1     Running     0          122m
aap-bf37650a-eda-activation-worker-54885997c5-zzzxd               1/1     Running     0          122m
aap-bf37650a-eda-api-865c748b57-v9plc                             3/3     Running     0          122m
aap-bf37650a-eda-default-worker-d4fc96799-d8h44                   1/1     Running     0          122m
aap-bf37650a-eda-default-worker-d4fc96799-zjlgl                   1/1     Running     0          122m
aap-bf37650a-eda-event-stream-7f7b458c69-cmb8q                    2/2     Running     0          122m
aap-bf37650a-eda-scheduler-57459b79b-sfht8                        1/1     Running     0          122m
aap-bf37650a-eda-scheduler-57459b79b-wd8hk                        1/1     Running     0          122m
aap-bf37650a-gateway-5dffd658-8rm9x                               2/2     Running     0          125m
aap-bf37650a-hub-api-7f5bb74cd-r7qqx                              1/1     Running     0          122m
aap-bf37650a-hub-content-74b6bdbc89-2wpj7                         1/1     Running     0          122m
aap-bf37650a-hub-content-74b6bdbc89-7vbw5                         1/1     Running     0          122m
aap-bf37650a-hub-redis-7757c684d-82slk                            1/1     Running     0          122m
aap-bf37650a-hub-web-fd448b87f-l4mpq                              1/1     Running     0          122m
aap-bf37650a-hub-worker-7489896757-cr5zh                          1/1     Running     0          122m
aap-bf37650a-hub-worker-7489896757-zxndz                          1/1     Running     0          122m
aap-bf37650a-postgres-15-0                                        1/1     Running     0          126m
aap-bf37650a-redis-0                                              1/1     Running     0          126m
aap-gateway-operator-controller-manager-7b786c9897-qpfzv          2/2     Running     0          128m
ansible-lightspeed-operator-controller-manager-9584689f5-v7vr6    2/2     Running     0          128m
automation-controller-operator-controller-manager-5f48c466792dt   2/2     Running     0          128m
automation-hub-operator-controller-manager-658b7cf6c7-ltznv       2/2     Running     0          128m
cvporiib-bgrkp                                                    1/1     Running     0          130m
eda-server-operator-controller-manager-5ccf6f4b74-w2g9m           2/2     Running     0          128m
resource-operator-controller-manager-5dcc57b745-p9nmx             2/2     Running     0          128m

### Step 5: Access Controller Pod Shell

Connect to the controller task pod using remote shell:
oc rsh -n aap-wrongly-airedale aap-bf37650a-controller-task-7798677c6c-zxmgz

Once inside the pod shell, you can use the metrics-utility command


# gather_all.py

Gather all gathers whole data from begining to datetime now. It uses the same env variables as previous script.

## Gather all example

uv run gather_all.py

It will run it locally, or if RPM variables (above) are set, it will gather in RPM.

# build_and_copy.py

Useful script that takes the same env variables as previous, connects to environment, runs build report with all of the parameters
and input and then copy report back to the folder where it was called.

Works only for RPM and Containerized.




