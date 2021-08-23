import configparser
from core_spark import Core



# Load the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# S2K

s2k_job = Core
s2k_job.job_check(job_name=config.get('s2k', 's2k_test'), command=config.get('s2k', 's2k_test_submit'))
s2k_job.running_check(job_name=config.get('s2k', 's2k_test'), command=config.get('s2k', 's2k_test_submit'))
