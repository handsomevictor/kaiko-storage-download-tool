from download_wasabi import WasabiVictorTool
from download_aws_s3 import AwsS3VictorTool


def main_wasabi():
    params_init = {
        'bucket_name': 'indices-backfill',
        # 'bucket_name': 'indices-data',
        'end_point_url': 'https://s3.us-east-2.wasabisys.com',
        'aws_arn': 'iam::100000052685:user/zhenning.li'
    }

    # wasabi_folder = 'index/v1/kk_pr_dogeusd/real_time/2023/06/'
    wasabi_folder = 'index_v1/v1/extensive/kk_pr_pythusdt/real_time/PT5S/'

    tool = WasabiVictorTool(**params_init)

    # ------- Store all file names in sub folder / More often used -------
    tool.store_file_names_subfolder(wasabi_subfolder_name=wasabi_folder,
                                    download_to_file_dir=f'all_files_in_{wasabi_folder.split("/")[-1]}.txt')

    # ------- Download all files in the bucket / Usually takes a long time -------
    # tool.store_all_file_names(download_to_file_dir='all_file_names.txt')

    params_download = {
        'download_to_dir': 'database_wasabi_mfa',  # download to this folder in root or other dir, can be changed
        'remove_name_file': True,
        'file_type': 'csv.gz',
        'max_workers_process': 40,
        'wasabi_folder': wasabi_folder,
    }
    tool.download_files(**params_download)


def main_aws_s3():
    bucket_name = 'dumps-kaiko'
    folder_name = 'pachira/smuc_full_order_book/cb/btcusd/2023/03'

    download_files_server = AwsS3VictorTool(bucket_name, login=True)
    download_files_server.download_all_file_names_in_folder(folder_name)
    download_files_server.download_files_from_s3_concurrent(max_workers=7,
                                                            file_type='csv.gz')


if __name__ == '__main__':
    main_wasabi()
    # main_aws_s3()
