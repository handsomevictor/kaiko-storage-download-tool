import os
import boto3
import gzip
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm
import shutil


# noinspection PyShadowingNames,PyPep8Naming,PyProtectedMember
class AwsS3VictorTool:
    def __init__(self, bucket_name, login=True):
        self.temp_dir = None

        if login:
            session = boto3.Session()
            mfa_serial = session._session.full_config['profiles']['default']['mfa_serial']
            mfa_token = input('Please enter your 6 digit MFA code:')
            sts = session.client('sts')
            MFA_validated_token = sts.get_session_token(SerialNumber=mfa_serial, TokenCode=mfa_token)

            s3 = boto3.resource('s3',
                                aws_session_token=MFA_validated_token['Credentials']['SessionToken'],
                                aws_secret_access_key=MFA_validated_token['Credentials']['SecretAccessKey'],
                                aws_access_key_id=MFA_validated_token['Credentials']['AccessKeyId']
                                )
            s3_cli = boto3.client('s3',
                                  aws_session_token=MFA_validated_token['Credentials']['SessionToken'],
                                  aws_secret_access_key=MFA_validated_token['Credentials']['SecretAccessKey'],
                                  aws_access_key_id=MFA_validated_token['Credentials']['AccessKeyId']
                                  )
            self.s3_cli = s3_cli
            self.my_bucket = s3.Bucket(bucket_name)
            self.s3 = s3
            self.bucket_name = bucket_name
            self.subfolder_name = None
            self.local_database_dir = 'database_aws_mfa'
            if not os.path.exists(self.local_database_dir):
                os.makedirs(self.local_database_dir)

    def download_single_file_to_local(self, single_file_s3_dir, file_type='csv.gz'):
        # This script will download the file from AWS S3 to database_aws_mfa
        file_name = single_file_s3_dir

        download_target_dir = os.path.join(os.getcwd(), self.local_database_dir)
        download_target_file_dir = os.path.join(download_target_dir, file_name)

        try:
            if not os.path.exists(download_target_dir):
                os.makedirs(download_target_dir)
        except FileExistsError:
            pass

        temp_dir = '/'.join(download_target_file_dir.split('/')[:-1])
        # self.temp_dir = temp_dir

        try:
            if not os.path.exists(os.path.join(os.getcwd(), self.local_database_dir, temp_dir)):
                os.makedirs(temp_dir)
        except FileExistsError:
            pass

        self.s3_cli.download_file(Bucket=self.bucket_name, Key=file_name,
                                  Filename=os.path.join(os.getcwd(),
                                                        self.local_database_dir,
                                                        download_target_file_dir))

        # Now unzip the file from gz to csv
        if file_type == 'csv.gz':
            file_name_unzipped = os.path.join(os.getcwd(), self.local_database_dir,
                                              file_name.split('/')[-1].split('.')[0] + '.csv')
            with gzip.open(download_target_file_dir, 'rb') as f_in:
                with open(file_name_unzipped, 'wb') as f_out:
                    f_out.writelines(f_in)
            os.remove(download_target_file_dir)

    def download_all_file_names_in_folder(self, folder_name):
        """
        Because s3_cli.list_objects_v2 only returns 1000 files at a time, we need to use a while loop to get all files
        """
        self.subfolder_name = folder_name

        file_names = []
        continuation_token = None
        i = 0
        while True:
            kwargs = {'Bucket': self.bucket_name, 'Prefix': folder_name}
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token
            response = self.s3_cli.list_objects_v2(**kwargs)
            file_names.extend([item['Key'] for item in response.get('Contents', [])])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')
            i += 1
            print(f'Iteration {i}')

        # save file names
        all_names_dir = os.path.join(os.getcwd(), f'all_files_dir_aws_in_subfolder_{folder_name.split("/")[0]}.txt')
        with open(all_names_dir, 'w') as f:
            for file in file_names:
                f.write(file + '\n')

    def download_files_from_s3_concurrent(self, max_workers, file_type='csv.gz'):
        """
        Must run download_all_file_names_in_folder first

        The reason that I don't detect the file type is because if so, it's hard to detect file type discrepency
        in the folder (there should be one type of file in one sub folder)
        """

        with open (f'all_files_dir_aws_in_subfolder_{self.subfolder_name.split("/")[0]}.txt', 'r') as f:
            all_files_to_download = [file_name.strip() for file_name in f.readlines()]

        print(f'Total number of files to download: {len(all_files_to_download)}')

        # download files
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            list(tqdm(pool.map(self.download_single_file_to_local,
                               all_files_to_download,
                               repeat(file_type)),
                      total=len(all_files_to_download)))
        print(f'All files are downloaded to {self.local_database_dir}!')

        # delete the folder that contains temp files & all names
        shutil.rmtree(os.path.join(os.getcwd(), self.local_database_dir, self.subfolder_name.split('/')[0]))
        os.remove(f'all_files_dir_aws_in_subfolder_{self.subfolder_name.split("/")[0]}.txt')

        print(f'Removed temp subfolder in {self.local_database_dir}!')
        print(f'Removed temp file that contains all file names in {self.local_database_dir}!')


if __name__ == '__main__':
    ...
