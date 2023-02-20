# This script aims to upload and download data on AWS S3 and Wasabi.
Download logic:
1. Download all names of a sub folder in a bucket
2. Download data files to local, delete txt file that contains the names
3. If data in csv.gz format, unzip and delete the original file


# Requirements:
1. Install AWS CLI
2. Configure AWS CLI, and then check dir ~/.aws (macOS) if the credentials and config files are there, check content
   The content should contain 2 files:
   1. config
   2. credentials

   Config should have 2 sections:
   1. [default]
   2. [profile wasabi]

   Credentials should have 2 sections:
   1. [default]
   2. [wasabi]
   
   This configuration is important, because in script it will directly use the AWS CLI to access.

3. Find files location on wasabi / AWS S3 (Sub folder etc.)
4. Open terminal and cd to the root folder, run `pip install -r requirements.txt`

# How to run:
## Wasabi
1. Run the script - download_wasabi.py, modify the function default value, etc. Prepare your mfa token (6 digits)
   More specifically, modify the following:

   ```python
    params_init = {
        'bucket_name': 'trades-data',
        'end_point_url': 'https://s3.us-east-2.wasabisys.com',
        'aws_arn': 'iam::100000052685:user/zhenning.li'
    }
    ```
2. Check the result stored in ~/database_wasabi_mfa


# AWS S3
For AWS S3, I didn't use random generated encypted key, so it's easier to run, but also easier to be hacked.
A correct configuration is a must, this is stricter than Wasabi.

1. Run the script - download_aws_s3.py, only modify the bucket name and folder name is enough.
   ```python
    bucket_name = 'dumps-kaiko'
    folder_name = 'markets/aggregated_trades/'
    ```
2. Prepare your mfa token (6 digits)
3. Check the result stored in ~/database_aws_mfa

