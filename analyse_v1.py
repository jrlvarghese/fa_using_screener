import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from os import path
import urllib.request
from lxml import html
import requests
import time
from bs4 import BeautifulSoup
import xlwt 
from xlwt import Workbook
import re
import fa

# =============================================================================
#   HEADERS
# 0        Security Code
# 1        Security Id
# 2        Security Name
# 3        Status
# 4        Group
# 5        Face Value
# 6        ISIN No
# 7        Industry
# 8        Instrument
# =============================================================================


# url for screener is 'https://www.screener.in/company/'
# siteUrl = 'https://www.screener.in/company/'

# Get input from user to check whether you have to download data 'd'
# or to analyse data
option = input("""Press 'd' for downloading data.
                \nPress 'a' for analysing data.
                \nPress 'p' for parsing data.
                \nPress 'c' for capwise file generation.
                \nPress 'k' for getting all companies with current price less than book value.
                \nPress 'l' for PE and ROCE based analysis\n""")
if(option == 'd'):
    print("You selected download data.\n")
    # Generate a df for analysis from the file containing list of equities in bse
    df = fa.get_dataframe('bse_scrips_list.csv',fa.DATA_PATH)
    print(df)
    # Select dataframe where instrument is Equity
    dfEq = df[df['Instrument']=='Equity']
    fileName = fa.get_filename()
    # append download path for directing file
    fileName = fa.DOWNLOAD_PATH + fileName    
    # Open log file and get the index
    beginIndex = fa.get_log()
    # If beginIndex is at the last index - then notify that previous download session was complete
    if(beginIndex==(len(dfEq)-1)):
        print("You have completed downloading.\n")
        beginIndex = 0

    if(beginIndex!=0):
        resumeStatus = input('Do you want to resume from where you left ('+str(beginIndex)+')\n')
        if(resumeStatus == 'y'):
            print('Resuming from ', str(beginIndex))
        else:
            print('Starting from begining...')
            beginIndex = 0
            print('Data willl be saved at: ',fileName)
            # columnNames = ['status','security_name','market_cap','year_high','year_low','current_price',
            #                     'current_price_var','pe','book_value','dividend_yield','roce','roe','bse_link','bse_sector']
            # fa.create_csv_file('csv',columnNames)
    total = len(dfEq)
    for i in range(beginIndex,total):
        print("\n\nGetting data, {} of {}.".format(i+1,total))
        # # Get the response from the webpage with security id
        xf = dfEq.iloc[i]
        # print(xf['Security Id'])
        data = fa.parse_screener_data(xf)
        fa.csv_write_data(fileName,data)
        # print(data)
        fa.write_log(i+1)
        
elif(option == 'a'):
    print("You opted to analyse data.\n")
    # Get list of files for analysis which is downloaded earlier
    fileList = os.listdir(fa.OUTPUT_PATH)
    print("Please select a file from the list.")
    # Display list of files with indexes
    fa.display_list(fileList)
    # Get the input parameter for selecting a file
    selection = int(input("Type the number corresponding to filename:\t"))
    print("you selected ",fileList[selection])
    # Get dataframe from the selected file
    file_to_read = fa.OUTPUT_PATH + fileList[selection]
    d = pd.read_csv(file_to_read,na_values=['na','nan'])
    d = d.fillna(0)
    d.current_price_var = d.current_price_var.astype('float')
    # Get list of sectors from the dataframe
    sectorList = fa.get_group(d,'bse_sector')
    # For analysing data based on the sector
    print("Please select the sector to analyse")
    # Display all sectors
    fa.display_list(sectorList)
    # Get input for selecting a sector
    selection = int(input("Type the number corresponding to sector:\t"))
    print("Selected sector: ",sectorList[selection])
    selectedDf = d[d['bse_sector']==sectorList[selection]]
    # selectedDf = selectedDf[selectedDf['current_price_var']<60]
    selectedDf['pb'] = selectedDf['current_price']/selectedDf['book_value']
    # columns_to_print = ['security_name','pe','book_value','pb','market_cap','year_high','year_low','current_price','current_price_var']
    columns_to_print = ['security_name','pe','book_value','current_price','pb','current_price_var','market_cap','year_high','year_low']
    df = selectedDf[columns_to_print].sort_values('pb')
    # for index,row in df.iterrows():
    #     print(df.loc[index].values)
    pb_file = fa.OUTPUT_PATH+'pb_based.csv'
    df.to_csv(pb_file)
elif(option == 'p'):
    print("Opted for parsing data.\nWill generate an excel file with sectorwise data")
    # Get the list of all files which are present in download path
    fileList = os.listdir(fa.DOWNLOAD_PATH)
    print("Please select a file from the list.")
    # Display list of files with indexes
    fa.display_list(fileList)
    # Get the input parameter for selecting a file
    selection = int(input("Type the number corresponding to filename:\t"))
    print("you selected ",fileList[selection])

    # Read dataframe from the selected file from the downloads folder
    file_to_read = fa.DOWNLOAD_PATH + fileList[selection]
    # set dtype dictionary
    dtype_dict = {'status':np.int32,'security_name':str,'bse_sector':str,'market_cap':np.float64,'year_high':np.float64,'year_low':np.float64,'current_price':np.float64,
                    'current_price_var':np.float64,'pe':np.float64,'book_value':np.float64,'dividend_yield':np.float64,'roce':np.float64,'roe':np.float64,'bse_link':str,'face_value':np.float64}
    # columnNames=['status','security_name','bse_sector','market_cap','year_high','year_low','current_price','current_price_var',
    #                             'pe','book_value','dividend_yield','roce','roe','bse_link','face_value']
    data = pd.read_csv(file_to_read,dtype=dtype_dict,na_values='na')
    # print(data.columns)
    df = data[data['status']==200]
    # print(df.head())
    sectorList = fa.get_group(df,'bse_sector')
    # sheet_name_list = map(fa.validSheetName,sectorList)
    # print(sheet_name_list)
    # file_to_write = fa.get_filename('xls')
    date_folder = fileList[selection].split('.')[0]+'/'
    date_path = fa.OUTPUT_PATH + 'sectorwisefa/' + date_folder
    os.mkdir(date_path)
    print("Folder to write {}".format(date_path))

    # with pd.ExcelWriter(file_to_write) as writer:
    #     for sector in sectorList:
    #         print("Parsing sector "+sector)
    #         selected_df = df[df['bse_sector']==sector]
    #         print(type(selected_df['pe'].iloc[0]))
    #         sheet_name = fa.validSheetName(sector)
    #         selected_df.to_excel(writer,sheet_name=sheet_name,columns=['security_name','bse_sector','market_cap','year_high','year_low','current_price','current_price_var',
    #                             'pe','book_value','dividend_yield','roce','roe','bse_link','face_value'],float_format="%.2f")
    #                 #             columnNames = ['status','security_name','market_cap','year_high','year_low','current_price',
    #                 # 'current_price_var','pe','book_value','dividend_yield','roce','roe','bse_link','bse_sector','face_value']

    for sector in sectorList:
        print("\nParsing sector "+sector)
        selected_df = df[df['bse_sector']==sector]
        file_name = date_path + fa.validSheetName(sector)+'.csv'
        print("Saving {} data on {}.".format(sector,file_name))
        selected_df.to_csv(file_name)


elif(option=='c'):
    print("Segregating companies to cap wise data largecap, midcap, smallcap")

    fileList = os.listdir(fa.OUTPUT_PATH)
    print("Please select a file from the list.")
    # Display list of files with indexes
    fa.display_list(fileList)
    # Get the input parameter for selecting a file
    selection = int(input("Type the number corresponding to filename:\t"))
    print("you selected ",fileList[selection])
    # display_list(fileList)
    file_to_read = fa.OUTPUT_PATH + fileList[selection]
    
    
    cap_list = ['SMALL','MID','LARGE']
    file_to_write = fa.get_filename('xls')
    file_to_write = fa.OUTPUT_PATH + 'CAPWISE_' + fileList[selection].split('.')[0] + '.xls'
    print("Writing cap wise data to {} ".format(file_to_write))
    with pd.ExcelWriter(file_to_write) as writer:
        for cap in cap_list:
            print("Parsing capwise data for {}CAP ".format(cap))
            cap_df = fa.get_capwise_data(file_to_read,cap)
            # sheet_name = fa.validSheetName(sector)
            cap_df.to_excel(writer,sheet_name=cap)
elif(option=='k'):
    print("Evaluate all companies with cmp less than the book value")
    fileList = os.listdir(fa.OUTPUT_PATH)
    print("Please select a file from the list.")
    # Display list of files with indexes
    fa.display_list(fileList)
    # Get the input parameter for selecting a file
    selection = int(input("Type the number corresponding to filename:\t"))
    print("you selected ",fileList[selection])
    # display_list(fileList)
    file_to_read = fa.OUTPUT_PATH + fileList[selection]
    df = pd.read_csv(file_to_read,na_values=['nan','na'])
    # Remove all na
    # df.fillna(0,inplace=True)
    df.dropna(inplace=True)
    df.market_cap = df.current_price.astype('float')
    df.book_value = df.book_value.astype('float')
    # print(df.head())
    # print(df.columns)
    df_select = df[df['current_price']<df['book_value']]
    # print(df_select['security_name'])
    df_select.to_csv('cmp_less_book.csv',columns=['security_name','current_price','book_value','bse_sector','year_high'])
    # for index,row in df.iterrows():
    #     if row['current_price'] < row['book_value']:
    #         print(row['security_name'],": c:",row['current_price']," b:",row['book_value'])
    #         # print(type(row['current_price']))
    print(df_select)
elif(option=='l'):
    print("\n\nAll companies with pe < sector median and roce > sector median")
    sectorwise_path = fa.OUTPUT_PATH + 'sectorwisefa/'
    # Get only directories from the sectorwise analysis path
    dirs = [os.path.join(sectorwise_path, f) for f in os.listdir(sectorwise_path) if os.path.isdir(os.path.join(sectorwise_path, f))]
    # Make a selection dialogue and get the selection
    print("\nPlease select a file:")
    fa.display_list(dirs)
    selection = int(input("Type the number corresponding to filename:\t"))
    print("you selected ",dirs[selection])
    sectorwise_files = os.listdir(dirs[selection])
    # create an empty dataframe to accomodate all data
    final_df = pd.DataFrame(columns=['security_name','pe','roce'])
    file_name = fa.OUTPUT_PATH + 'PE_ROCE_BASED_' + dirs[selection].split('/')[-1] + '.xls'
    with pd.ExcelWriter(file_name) as writer: 
        # loop through the target directory
        for i in range(0,len(sectorwise_files)):
            sector_index = i
            sector_name = sectorwise_files[sector_index].split(".")[0]
            if sector_name == '_':
                sector_name = 'misc'
            # print(sectorwise_files[sector_index])
            print("\n\nYou selected {} for evaluation.".format(sector_name))
            df = pd.read_csv(dirs[selection]+'/'+sectorwise_files[sector_index],usecols=['security_name','pe','roce','bse_sector'])
            # select dataframe with pe value more than 0
            df = df[df['pe']>0]
            # estimate the median
            median = df.median()
            df = df[df['pe']<median['pe']]
            df = df[df['roce']>median['roce']]
            # sort the dataframe in descending order
            df = df.sort_values(by=['pe'], ascending=False)
            # print(df.head())
            # print(df.tail())
            final_df = final_df.append(df)
            df.to_excel(writer, sheet_name=sector_name)
    print(final_df.head())
    # file_name = fa.OUTPUT_PATH + 'PE_ROCE_BASED_' + dirs[selection].split('/')[-1] + '.xls'
    print("\n\nData will be saved in {} ".format(file_name))
    file_name = fa.OUTPUT_PATH + 'PE_ROCE_BASED_ALL' + dirs[selection].split('/')[-1] + '.xls'
    print("\n\nAll data will be saved in {} ".format(file_name))
    final_df.to_excel(file_name)
    
    

    
    