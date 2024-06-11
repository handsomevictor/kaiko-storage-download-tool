"""
To download to local, a good way is to firstly store all file names that you want to download in a list,
and then download them using multithreading (for MFA enabled account, never use multiprocessing).
"""

import boto3
from botocore.config import Config
import json
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm
import gzip
import os


# noinspection PyIncorrectDocstring
class WasabiVictorTool:
    def __init__(self, bucket_name='trades-data', end_point_url='https://s3.us-east-2.wasabisys.com',
                 aws_arn='iam::100000052685:user/zhenning.li'):

        self.mfa_token = input('Please enter your 6 digit MFA code:')
        self.bucket_name = bucket_name
        self.wasabi_subfolder_name = None
        self.download_to_dir = None

        if end_point_url is None:
            self.end_point_url = 'https://s3.us-east-2.wasabisys.com'
        else:
            self.end_point_url = end_point_url

        if aws_arn is None:
            self.aws_arn = 'iam::100000052685:user/zhenning.li'
        else:
            self.aws_arn = aws_arn

        res = os.popen(
            f'aws sts get-session-token --serial-number arn:aws:{self.aws_arn} --token-code '
            f'{self.mfa_token} --profile wasabi --endpoint-url=https://sts.wasabisys.com')

        print(
            f'aws sts get-session-token --serial-number arn:aws:{self.aws_arn} --token-code '
            f'{self.mfa_token} --profile wasabi --endpoint-url=https://sts.wasabisys.com'
        )
        res = json.loads(res.read())

        AccessKeyId = res['Credentials']['AccessKeyId']
        SecretAccessKey = res['Credentials']['SecretAccessKey']
        SessionToken = res['Credentials']['SessionToken']

        print("AccessKeyId: ", AccessKeyId)
        print("SecretAccessKey", SecretAccessKey)
        print("SessionToken[:10]", SessionToken[:10])

        end_point_url = 'https://s3.us-east-2.wasabisys.com'

        # download all file names
        s3 = boto3.resource('s3',
                            endpoint_url=end_point_url,
                            aws_access_key_id=AccessKeyId,
                            aws_secret_access_key=SecretAccessKey,
                            aws_session_token=SessionToken
                            )

        # give more retry times - Sometimes it will fail when there are too many files to download in many threads
        my_config = Config(
            retries={
                'max_attempts': 100,
                'mode': 'standard'
            }
        )
        s3_cli = boto3.client('s3',
                              aws_session_token=SessionToken,
                              aws_secret_access_key=SecretAccessKey,
                              aws_access_key_id=AccessKeyId,
                              endpoint_url=end_point_url,
                              config=my_config
                              )
        self.s3 = s3
        self.s3_cli = s3_cli

    def store_all_file_names(self, download_to_file_dir=f'all_file_names.txt'):
        """
        Store all file names of a bucket, not usually used
        """

        my_bucket = self.s3.Bucket(self.bucket_name)
        all_files = list(my_bucket.objects.all())

        with open(download_to_file_dir, 'w') as f:
            for file in all_files:
                f.write(file.key + '\n')

    def store_file_names_subfolder(self, wasabi_subfolder_name, download_to_file_dir=None):
        """
        Only store file names in a sub folder, often used
        """
        self.wasabi_subfolder_name = wasabi_subfolder_name
        if not download_to_file_dir:
            download_to_file_dir = f'all_files_{"-".join(wasabi_subfolder_name.split("/"))}.txt'

        response = self.s3_cli.list_objects_v2(Bucket=self.bucket_name, Prefix=wasabi_subfolder_name)
        # print(response)
        file_names = [obj['Key'] for obj in response['Contents']]

        # Extract the list of file names from the response
        i = 0
        while 'Contents' in response:
            print(f'Page {i}')
            i += 1
            # do something with each object
            for obj in response['Contents']:
                file_names.append(obj['Key'])

            # check if there are more results to retrieve
            if response['IsTruncated']:
                # get next page of results
                response = self.s3_cli.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=wasabi_subfolder_name,
                    ContinuationToken=response['NextContinuationToken']
                )
            else:
                break

        # save file names
        with open(download_to_file_dir, 'w') as f:
            for file in file_names:
                f.write(file + '\n')

    def download_single_file(self, single_file_wasabi_dir, download_to_dir=None, file_type='csv.gz'):
        """

        :param file_type: either 'csv' or 'csv.gz'
        :param single_file_wasabi_dir: should be like: tick_csv/v1/gz_v1/bbst/BTC30DEC2226000C/2022_10
               /bbst_BTC30DEC2226000C_trades_2022_10_14.csv.gz
        :param download_to_dir: local dir, like: os.path.join(os.getcwd(), 'database_wasabi_mfa')
        :return:
        """
        file_name = single_file_wasabi_dir.split('/')[-1]

        if not download_to_dir:
            download_target_file_dir = os.path.join(os.getcwd(), 'database_wasabi_mfa', file_name)
        else:
            download_target_file_dir = os.path.join(download_to_dir, file_name)

        print(f'Downloading {file_name} to {download_target_file_dir}')
        self.s3_cli.download_file(self.bucket_name, single_file_wasabi_dir, download_target_file_dir)

        # Save to local according to the original folder structure
        single_instrument_dir = '/'.join(single_file_wasabi_dir.split('/')[:-1])
        pair_dir = os.path.join(os.getcwd(), 'database_wasabi_mfa', single_instrument_dir)
        try:
            if not os.path.exists(pair_dir):
                os.makedirs(pair_dir)
        except FileExistsError:
            pass

        if file_type == 'csv.gz':
            file_name_unzipped = os.path.join(os.getcwd(), pair_dir,
                                              file_name.split('.')[0] + '.csv')
            with gzip.open(download_target_file_dir, 'rb') as f_in:
                with open(file_name_unzipped, 'wb') as f_out:
                    f_out.writelines(f_in)

            try:
                os.remove(download_target_file_dir)
            except FileNotFoundError:
                print(f'File {download_target_file_dir} not found, skip removing')
            print(f"Downloaded {file_name_unzipped} to {pair_dir}, now in csv format")

    def download_files(self, all_files_wasabi_dir: list = None, max_workers_process=30, wasabi_folder=None,
                       download_to_dir=None, file_type='csv.gz', remove_name_file=True, only_selected_dates=None):
        """
        :param wasabi_folder: either be all (download all files in this bucket (takes a long time), or a subfolder name
        :param file_type: Should be either 'csv' or 'csv.gz'
        """
        if not download_to_dir:
            download_to_dir = 'database_wasabi_mfa'

        wasabi_folder = 'all_file_names.txt' if wasabi_folder == 'all' \
            else f'all_files_in_{wasabi_folder.split("/")[-1]}.txt'

        if all_files_wasabi_dir is None:
            all_files_wasabi_dir = []
            try:
                with open(os.path.join(os.getcwd(), wasabi_folder), 'r') as f:
                    for line in f:
                        all_files_wasabi_dir.append(line.strip())
            except FileNotFoundError:
                print(f'File {wasabi_folder} not found, please run store_all_file_names() first')

        # only select the file names that are in the selected dates
        # print(all_files_wasabi_dir)

        if isinstance(only_selected_dates, list):
            all_files_wasabi_dir = [file for file in all_files_wasabi_dir if
                                    any(date in file for date in only_selected_dates)]

        # delete temp file that stores all file names
        if remove_name_file:
            os.remove(os.path.join(os.getcwd(), wasabi_folder))
            print(f'Removed {wasabi_folder} from {os.getcwd()}')

        print(f'All files to be downloaded: {len(all_files_wasabi_dir)}')

        with ThreadPoolExecutor(max_workers=max_workers_process) as pool:
            list(tqdm(pool.map(self.download_single_file,
                               all_files_wasabi_dir,
                               repeat(os.path.join(os.getcwd(), download_to_dir)),
                               repeat(file_type)),
                      total=len(all_files_wasabi_dir)))


if __name__ == '__main__':
    ...
