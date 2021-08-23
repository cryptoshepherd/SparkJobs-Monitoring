import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import configparser
import json
import paramiko

# Load the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Set the proxy to Call the Microsoft Teams WebHook
proxyDict = {
        'http': 'http://proxy:PORT',
        'https': 'http://proxy:PORT'
}

# ssh
username = "xxx"
password = "xxx"

# call API
for api in ['https://CLOUDERA_API_MASTER:PORT', 'https://CLOUDERA_API_MASTER:7183', 'https://CLOUDERA_API_MASTER:PORT']:
    try:
        response = requests.get((api + config.get('cloudera', 'endpoint')),auth=HTTPBasicAuth('USER', 'PASSWD'), verify=False, timeout=5)
        response.raise_for_status()
        if response.status_code == 200:
            host = api
            yarn_api = requests.get((api + config.get('cloudera','yarn')), auth=HTTPBasicAuth('USER', 'PASSWD'), verify=False, timeout=5)
            spark_jobs = yarn_api.json()
            applications_lst = spark_jobs['applications']
            break
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

class Core:
    # In case the job_name is not in API output
    def job_check(job_name, command):
        app_name = [app['name'] for app in applications_lst]
        if job_name not in app_name:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            res_str = host.replace('https://', '').replace(':PORT', '')
            client.connect(hostname=res_str, username=username, password=password)
            root_path = '/app/spark_submit/' + command
            spark_submit = open(root_path).read()
            stdin, stdout, stderr = client.exec_command(spark_submit)
            payload = {'title': 'Spark Job Has Been Started',
                    'text': 'Task ID:  ' + str(job_name) + '   Has been restarted'
                       }
            # POST to Microsoft Teams WebHook to be notified that the ID Has been restarted
            requests.post(config.get('webhook', 'url'), data=json.dumps(payload), proxies=proxyDict)
            client.close()

    def running_check(job_name, command):
        for app in applications_lst:
            app_name = app['name']
            if job_name == app_name:
                if 'RUNNING' not in app['state']:
                    if 'KILLED' not in app['state']:
                        client = paramiko.SSHClient()
                        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        res_str = host.replace('https://', '').replace(':PORT', '')
                        client.connect(hostname=res_str, username=username, password=password)
                        root_path = '/app/spark_submit/' + command
                        spark_submit = open(root_path).read()
                        stdin, stdout, stderr = client.exec_command(spark_submit)
                        teams = open('job_restart.json')
                        jobRestart = teams.read()
                        # Building payload replacing some value
                        my_payload = jobRestart.replace('app', str(job_name))
                        # Calling Teams WebHook
                        requests.post(config.get('webhook', 'url'), data=my_payload, proxies=proxyDict, timeout=6)
                        client.close()
