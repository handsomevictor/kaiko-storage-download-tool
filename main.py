from download_wasabi import WasabiVictorTool
from download_aws_s3 import AwsS3VictorTool


def main_wasabi():
    params_init = {
        'bucket_name': 'trades-data',
        'end_point_url': 'https://s3.us-east-2.wasabisys.com',
        'aws_arn': 'iam::100000052685:user/zhenning.li'
    }
    wasabi_folder = 'tick_csv/v1/gz_v1/bbst/BTC30DEC2226000C/2022_10'

    tool = WasabiVictorTool(**params_init)

    # ------- Store all file names in sub folder / More often used -------
    tool.store_file_names_subfolder(wasabi_subfolder_name=wasabi_folder,
                                    download_to_file_dir=f'all_files_in_{wasabi_folder.split("/")[-1]}.txt')

    # ------- Download all files in the bucket / Usually takes a long time -------
    # tool.store_all_file_names(download_to_file_dir='all_file_names.txt')

    params_download = {
        'download_to_dir': 'database_wasabi_mfa',
        'remove_name_file': True,
        'file_type': 'csv.gz',
        'max_workers_process': 30,
        'wasabi_folder': wasabi_folder,
    }
    tool.download_files(**params_download)


def main_aws_s3():
    bucket_name = 'dumps-kaiko'
    folder_name = 'markets/aggregated_trades/'

    download_files_server = AwsS3VictorTool(bucket_name, login=True)
    download_files_server.download_all_file_names_in_folder(folder_name)
    download_files_server.download_files_from_s3_concurrent(max_workers=30,
                                                            file_type='csv.gz')


if __name__ == '__main__':
    # main_wasabi()
    main_aws_s3()

