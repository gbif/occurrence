""" Script to test a list of json files containing download requests objects.
  It creates a download for each file, waits for its response, once it finishes runs the tests in download_tests.py
"""

import glob, json, os, requests, time, sys, argparse, subprocess

# Download finish statuses
finish_status = ['SUCCEEDED', 'CANCELLED', 'KILLED', 'FAILED', 'SUSPENDED', 'FILE_ERASED']

# environment with postfix names
env_uat = ['dev', 'uat']

""" Builds the url to create a download
"""
def download_request_url(env, download_type):
  return 'https://api.gbif' + ('-' + env if env in env_uat else env)  + '.org/v1/' + download_type + '/download/request'


""" Triggers the creation of an download
"""
def post_request_download(download_request, username, password, env, download_type):
  return requests.post(download_request_url(env, download_type), json=download_request, auth=(username, password)).text


""" Builds the url to get a download info
"""
def download_url(env, download_type):
  return 'https://api.gbif' + ('-' + env if env in env_uat else env)  + '.org/v1/' + download_type + '/download/'


""" Gets a download data
"""
def get_download(download_id, username, password, env, download_type):
  return requests.get(download_url(env, download_type) + download_id, auth=(username, password)).json()


""" Builds a download request object using a json file
"""
def download_request(file, username, email):
  f = open(file)
  download_request = json.load(f)
  download_request['creator'] = username
  download_request['notification_address'] = [email]
  f.close()
  return download_request


""" Executes the test cases in file downloads_test.py
"""
def run_download_tests(download_id, env, work_dir, download_type):
  subprocess.call(['python', 'downloads_test.py', download_url(env, download_type) + download_id, work_dir])


if __name__ == '__main__':

  #arguments parsing
  parser = argparse.ArgumentParser()
  parser.add_argument("username", help="GBIF Portal username")
  parser.add_argument("password", help="GBIF Portal user password")
  parser.add_argument("email", help="Download notification email")
  parser.add_argument("env", help="GBIF environment")
  parser.add_argument("download_type", help="Download type occurrence or event",  nargs="?", default="occurrence")
  parser.add_argument("source_dir", help="Directory containing the Download JSON requests", nargs="?", default="../../../example-jobs/")
  parser.add_argument("work_dir", help="Directory where files are downloaded ", nargs="?", default="/tmp/")
  args = parser.parse_args()

  dir_path = r"{}".format(args.source_dir + '*.json')
  files = glob.glob(dir_path)
  for file in files:
    download_request_obj = download_request(file, args.username, args.email)
    download_type = download_request_obj.get('type', args.download_type).lower()
    download_id = post_request_download(download_request_obj, args.username, args.password, args.env, download_type)

    finished = False
    while not finished:
      time.sleep(10)
      download_response = get_download(download_id, args.username, args.password, args.env, download_type)

      if download_response['status'] in finish_status:
        finished = True
        if download_response['status'] == 'SUCCEEDED':
          run_download_tests(download_id, args.env, args.work_dir, download_type)
        else:
          sys.exit('Download ' + download_id + ' failed')
