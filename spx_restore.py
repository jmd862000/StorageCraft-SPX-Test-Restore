import sys
import requests
import argparse
import time
import os
from functools import partial
requests.packages.urllib3.disable_warnings()

class Test_Spx_Restore:
    def __init__(self, username, password,host='localhost',port=13581):
        self.SPX_HOST = host
        self.SPX_PORT = port
        self._SPX_BASE = 'https://{host}:{port}/spx'.format(host=self.SPX_HOST, port=self.SPX_PORT)
        token = self._get_auth_token(username,password)
        headers = {'Authorization': 'Token {token}'.format(token=token)}
        self._get = partial(requests.get, verify=False,headers=headers)
        self._post = partial(requests.post,verify=False, headers=headers)
        self._put = partial(requests.put,verify=False, headers=headers)
        self._delete = partial(requests.delete,verify=False, headers=headers)

    def _get_auth_token(self,username, password):
        try:
            res = requests.post(
                self._SPX_BASE + '/auth/login',
                json={
                    'username': username,
                    'password': password,
                },
                 verify=False,
            )
        except (requests.ConnectionError, requests.HTTPError) as e:
            sys.stderr.write('Fail: {e}\n'.format(e=e))
            sys.exit(1)
        else:
            if res.status_code != 200:
                sys.stderr.write('Fail: {e}\n'.format(e=res.text))
                sys.exit(1)
            return res.json()['token']

    def _get_latest_images(self):
        res = self._get(self._SPX_BASE + '/v1/image')
        volumes = {v['drive_letter'] for v in res.json()}
        result = {}
        for volume in volumes:
            images = [v for v in res.json() if v['drive_letter'] == volume]
            latest_image = max(images,key=lambda x:x['snapshot_time'])
            result[volume]={'uuid':latest_image['uuid'],'path':latest_image['filename']}
        return result

    def _mount_image(self,image_path,mountpoint="R:",read_only=True,encryption_key=None):
        res = self._post(self._SPX_BASE + '/v1/mounted_image',
            json={
                'img' : image_path,
                'mountpoint' : mountpoint,
                'read_only' : read_only,
                'use_existing_buffer' : True,
                'password' : encryption_key
            })
        return res.text

    def _job_complete(self,job_id):
        res = self._get(self._SPX_BASE + '/v1/chore/' + job_id)
        if res.json()['status']==100:
            return True
        elif res.json()['status']<0:
            raise RuntimeError("The job failed to complete")
            
    def _read_backup_data(self,file_path,verification_string):
        with open(file_path,'r') as file:
            if verification_string in file.read():
                return True
            else:  
                return False

    def _unmount_images(self,volume_letter,save_changes_to_incremental=False):
        mounted_vols = self._get(self._SPX_BASE + '/v1/mounted_image').json()
        for vol in mounted_vols:
            if vol['snap_drive'][0] == volume_letter[0]:
                res = self._delete(self._SPX_BASE + '/v1/mounted_image/' + vol['vol_num'],json={'generate_incremental':save_changes_to_incremental})
                return res.text
    
    def _wait_for_job(self,job_id,timeout=300):
        status,x = False,0
        while (not status and x<timeout):
            status = self._job_complete(job_id)
            time.sleep(1)
            x+=1
        return status

    def initiate_test_restore(self, mountpoint='R:',encryption_key=None,test_file='Test 1.txt',test_string='Data'):
        images = self._get_latest_images()
        for k,v in images.items():
            print("Mounting volume {0} using image file: {1}".format(k,v['path']))
            job_id = self._mount_image(v['path'],mountpoint=mountpoint,encryption_key=encryption_key)
            if self._wait_for_job(job_id):
                if self._read_backup_data(file_path=os.path.join(mountpoint,test_file),verification_string=test_string):
                    print("Data verification of {test_file} was successful.".format(test_file=test_file))
                else:
                    print("Data verification failed when examining {test_file} for {test_string}.".format(test_file=test_file,test_string=test_string))
                job_id = self._unmount_images(volume_letter=k)
                if job_id:
                    print("Successfully unmounted image {path} from volume {mountpoint}.".format(path=v['path'],mountpoint=mountpoint))
                else:
                    raise TimeoutError("The unmount operation for volume {mountpoint} failed to complete in the specified time.".format(mountpoint=mountpoint))            
            else:
                raise TimeoutError("The mount operation failed to complete in the specified time.")
            
def get_parser():
    parser = argparse.ArgumentParser(
        description='Test file restores of the latest backup images for each volume.'
        )
    parser.add_argument('-u','--username',help='SPX admin username',required=True)
    parser.add_argument('-p','--password',help='SPX admin password',required=True)
    parser.add_argument('-k','--encryption-key',help='SPX backup encryption key',default=None)
    return parser

def command_line():
    parser = get_parser()
    args = vars(parser.parse_args())
    restore = Test_Spx_Restore(username=args['username'],password=args['password'])
    restore.initiate_test_restore(encryption_key=args['encryption_key'])

if __name__=="__main__":
    command_line()