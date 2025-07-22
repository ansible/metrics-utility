# testathon_data_prepare

if you run this directly without any parameters, it will fill local environment db, provided that it runs
at this url:

https://localhost:8030/api/controller/v2
and credentials: admin/admin

The script will use API to insert testing data. It also connects directly to DB, deletes all main_jobhostsummary table content
and runs jobs that will populate this table again. It will then run updates to change modified dates of job runs, so users
can test different gather ranges.

You can override url and credentails by environmental variables:
USERNAME, PASSWORD, API_URL

## RPM build

It can also connect to RMP jenkins build, but you have to provide specific parameters.

Besides USERNAME, PASSWORD, API_URL to gateway api, you need also to specify:

SSH_URL, SSH_USER to controller instance. 

Script will directly connect using SSH and do some modification to DB.

If the server is on VPN, do not forget to connect on VPN.

# gather_all

Gather all gathers whole data from begining to datetime now. It uses SSH_URL and SSH_USER as previous script. 




