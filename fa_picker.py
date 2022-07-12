import pandas as pd
import numpy as np
import os
from os import path
# import custom library
import fa
# define the target directory
target_path = fa.OUTPUT_PATH + 'sectorwisefa/'
print(target_path)

# get the list of files present in the directory
all_folder = os.listdir(target_path)

# display the files list
# fa.display_list(all_folder)

# pick the file of interest
#   print(all_folder[18])
target_folder = target_path + all_folder[18] + '/'
file_list = os.listdir(target_folder)
# print(file_list[6])

for files in file_list:
    print("\n\nSector ", files)
    # open as a dataframe 
    df = fa.get_dataframe(files,target_folder)
    # print(df.head())
    # print(df.columns)
    # all column names
    # ['Unnamed: 0', 'status', 'security_name', 'market_cap', 'year_high',
    #        'year_low', 'current_price', 'current_price_var', 'pe', 'book_value',
    #        'dividend_yield', 'roce', 'roe', 'bse_link', 'bse_sector',
    #        'face_value']
    roe_mean = round(df['roe'].mean(),2)
    roe_std = round(df['roe'].std(),2)
    print("Mean: {}, Std dev: {}".format(roe_mean, roe_std))
    print("1 SD above the mean is: {}".format(roe_mean + roe_std))
    # one_sd = roce_mean + roce_std
    print(df[df['roe']>roe_mean])

