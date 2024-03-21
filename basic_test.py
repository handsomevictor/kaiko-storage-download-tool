"""
Only check for all files in the download folder:

For RP and PP:
1. Check if the total number of rows equal to 12 * 60 * 24
2. Check if the next timestamp
"""

import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# use os.join
dir = os.path.join(os.getcwd(), 'database_wasabi_mfa', 'index_v1')

# if PR_RR_check_report.txt exists, delete it
if os.path.exists('PR_RR_check_report.txt'):
    os.remove('PR_RR_check_report.txt')
    print('PR_RR_check_report.txt deleted')

with open('PR_RR_check_report.txt', 'a') as f:
    f.write('Check for PR and RR:\n')


def check_total_no_of_rows():
    for root, dirs, files in os.walk(dir):
        for file in tqdm(files):
            if file == '.DS_Store':
                continue
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)

                # check if the file has a total rows of 12 * 60 * 24
                if df.shape[0] != 12 * 60 * 24:
                    with open('PR_RR_check_report.txt', 'a') as f:
                        f.write(f'Checking total number of rows:\n')
                        f.write(f'file: {file_path}\n')
                        f.write(f'number of rows: {df.shape[0]}\n')
                        f.write(f'-----------------------------------\n')
                    # print(f'file: {file_path}')
                    # print(df.shape)
    with open('PR_RR_check_report.txt', 'a') as f:
        f.write(f'Check for PR and RR done\n')


def check_timestamp_5s():
    for root, dirs, files in os.walk(dir):
        for file in tqdm(files):
            if file == '.DS_Store':
                continue
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)
                df['intervalStart'] = pd.to_datetime(df['intervalStart'])
                df['intervalEnd'] = pd.to_datetime(df['intervalEnd'])

                # check if the time difference between two rows is 5s
                df['time_diff'] = df['intervalStart'].diff()
                df = df.iloc[1:]
                df['time_diff'] = df['time_diff'].dt.total_seconds()
                if len(df[df['time_diff'] != 5]) > 0:
                    with open('PR_RR_check_report.txt', 'a') as f:
                        f.write(f'Checking time difference:\n')
                        f.write(f'file: {file_path}\n')
                        f.write(f'-----------------------------------\n')
                    # print(df[df['time_diff'] != 5])
                    # print(f'file: {file_path}')


def check_duplicate():
    for root, dirs, files in os.walk(dir):
        for file in tqdm(files):
            if file == '.DS_Store':
                continue
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)

                # check duplicate
                if df.duplicated().any():
                    with open('PR_RR_check_report.txt', 'a') as f:
                        f.write(f'Checking duplicate:\n')
                        f.write(f'file: {file_path}\n')
                        f.write(f'{df[df.duplicated(keep=False)].sort_values(by=["intervalStart", "intervalEnd"])}\n')
                        f.write(f'-----------------------------------\n')
                    # print(f'file: {file_path}')
                    # print(df[df.duplicated(keep=False)].sort_values(by=['intervalStart', 'intervalEnd']))
    with open('PR_RR_check_report.txt', 'a') as f:
        f.write(f'Check duplicates for PR and RR done\n')


def check_zvb():
    for root, dirs, files in os.walk(dir):
        for file in tqdm(files):
            if file == '.DS_Store':
                continue
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)
                number_of_nulls = len(df[df['price'].isnull()])
                print(f'file: {file}, number of nulls: {number_of_nulls}, percentage: {number_of_nulls / df.shape[0]}')
                if number_of_nulls > 0:
                    percentage = number_of_nulls / df.shape[0]
                    with open('PR_RR_check_report.txt', 'a') as f:
                        f.write(f'Checking zvb:\n')
                        f.write(f'file: {file_path}\n')
                        f.write(f'number of nulls: {number_of_nulls}\n')
                        f.write(f'percentage: {percentage}\n')
                        f.write(f'-----------------------------------\n')
    with open('PR_RR_check_report.txt', 'a') as f:
        f.write(f'Check zvb for PR and RR done\n')


def check_special_file():
    file = '/Users/zhenningli/Documents/GitHub/kaiko-storage-download-tool/database_wasabi_mfa/index/v1/kk_pr_shibusd/real_time/2023/05/' \
           'kk_pr_shibusd_real_time_2023-05-22.csv'
    df = pd.read_csv(file)
    df['intervalStart'] = pd.to_datetime(df['intervalStart'])
    df['intervalEnd'] = pd.to_datetime(df['intervalEnd'])
    df['time_diff'] = df['intervalStart'].diff().dt.total_seconds()

    # sort based on intervalStart
    df = df.sort_values(by=['intervalStart', 'intervalEnd'])
    print(df.shape)
    print(df[df['time_diff'] != 5])

    # print duplicate
    print(df[df['intervalStart'].duplicated()])
    df.to_csv('new_csv_file.csv')
    # print(df)



def ma123(dir):
    df = pd.read_csv(dir)
    df['intervalStart'] = pd.to_datetime(df['intervalStart'])
    df['intervalEnd'] = pd.to_datetime(df['intervalEnd'])
    # sort it
    df = df.sort_values(by=['intervalEnd'])
    df['time_diff'] = df['intervalEnd'].diff().dt.total_seconds()
    # print(df.head(30))
    # df.to_csv('new_csv_file.csv')
    # print(df[df['time_diff'] != 5])
    print(df.shape)


def process_pepe_sort():
    dir = os.path.join(os.getcwd(), 'database_wasabi_mfa', 'v1', 'kk_pr_pepeusd', 'real_time', 'PT5S')
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file == '.DS_Store':
                continue
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                date = file.split('_')[-1].split('.')[0]
                df = pd.read_csv(file_path)
                df['intervalStart'] = pd.to_datetime(df['intervalStart'])
                df['intervalEnd'] = pd.to_datetime(df['intervalEnd'])
                # sort it
                df = df.sort_values(by=['intervalEnd'])

                # don't include the colume named: detail
                df = df[['intervalStart', 'intervalEnd', 'price', 'parameters']]

                # # the price shouldn't be in scientific notation
                # df['price'] = df['price'].apply(lambda x: '%.20f' % x)

                new_name = '_'.join(file_path.split('_')[0:-4]) + f'_{date}.csv'
                df.to_csv(new_name, index=False)
                print(f'file: {file_path}, new_name: {new_name}')

                df['time_diff'] = df['intervalEnd'].diff().dt.total_seconds()
                print(f'file: {file}, {df.shape[0] / (24 * 60 * 12)}')
                print(df.shape[0] <= (24 * 60 * 12))




if __name__ == '__main__':
    check_total_no_of_rows()
    check_timestamp_5s()
    check_duplicate()
    check_zvb()
    # check_special_file()
    # ma123()
    # process_pepe_sort()
