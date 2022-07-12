import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from os import path
import urllib.request
from lxml import html
import requests
import time
import random
import datetime
from bs4 import BeautifulSoup
import xlwt 
from xlwt import Workbook
import re
import csv

DATA_PATH = 'datafiles/'
DOWNLOAD_PATH = 'downloads/'
LOG_PATH = 'log/'
LOG_FILE = 'log/log.dat'
SCREENER_URL = 'https://www.screener.in/company/'
OUTPUT_PATH = 'analysisoutput/'

BSE_DATA_FILE = DATA_PATH + 'bse_scrips_list.csv'

# LARGE CAP IS ABOVE 20000 CRORE
LARGE_CAP = 20000
# MID CAP -- ABOVE 5000 CRORE AND BELOW 20000 CRORE
MID_CAP = 5000
# SMALL CAP -- LESS THAN 5000 CRORE
SMALL_CAP = 0

#######################################################################
def isNumber(x):
    if((ord(x)>=46)and(ord(x)<58)):
        return True
    else:
        return False

# Function used to convert the numbers to float values 
# before converting to float check it's not an empty string
def validateAndConvert(x):
    x = x.replace(" ","")
    try:
        return float(x)
    except IndexError:
        return 'na'
    except:
        return 0

def listToString(l):
    #print("length: ",len(l))
    # initialize an empty string
    strng = ""
    if(len(l)!=0):
        for ele in l:
            if(isNumber(ele)):
                strng += ele
        return strng
    else:
        return '0'

def validSheetName(sheetName):
    sheetName = re.sub('[^0-9a-zA-Z]+', '_', sheetName)
    return sheetName


def writeHeaders(sheet):
    sheet.write(0,0,'COMPANY_SYMBOL')
    sheet.write(0,1,'P/E')
    sheet.write(0,2,'SECTOR')
    sheet.write(0,3,'INDUSTRY')
    sheet.write(0,4,'SECURITY_GROUP')
    sheet.write(0,5,'MARKTE_CAP')
    sheet.write(0,6,'52_WEEK_HIGH')
    sheet.write(0,7,'52_WEEK_LOW')
    sheet.write(0,8,'CURRENT_PRICE')
    sheet.write(0,9,'CURRENT_PRICE_VAR%')
    sheet.write(0,10,'DIVIDEND_YIELD%')
    sheet.write(0,11,'ROCE%')
    sheet.write(0,12,'ROE%')
    sheet.write(0,13,'SALES GROWTH%')


def writeContentsData(sheet,dataList):
    print('\n************\nWriting into file...')
    # LOOP THROUGH EACH CONTENT IN allData
    for rowIndex in range(0,len(dataList)):
        for colIndex in range(0,len(dataList[0])):
            sheet.write(rowIndex+1,colIndex,dataList[rowIndex][colIndex])
    # Clear all data after writing
    dataList.clear()
    
def writeContents(sheet,companySymbol,pe,sector,industry,securityGroup):
    print('Writing into file...')
    i=0
    rowIndex = 0
    for i in range(0, len(pe)):
        rowIndex = i+1
        sheet.write(rowIndex,0,companySymbol[i])
        sheet.write(rowIndex,1,pe[i])
        sheet.write(rowIndex,2,sector[i])
        sheet.write(rowIndex,3,industry[i])
        sheet.write(rowIndex,4,securityGroup[i])
    companySymbol.clear()
    pe.clear()
    sector.clear()
    industry.clear()
    securityGroup.clear()

# function to generate dataframe from given path and file name
def get_dataframe(fName, path):
    fileToOpen = path+fName
    data = pd.read_csv(fileToOpen)
    return pd.DataFrame(data)

def parse_data(xf):
    # response = requests.get(searchUrl)
    headers = {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0'}
    # Search with 'Security Id' (which is like RIL in case of Reliance Industries)
    searchUrl = SCREENER_URL + xf['Security Id']
    response = requests.get(searchUrl, stream=True, headers=headers)
    print("\n\n>>>>>>\nGetting data from ", searchUrl)

    # If we are not getting a response using the 'Security Id' try with 'Security Code'
    if(response.status_code == 404):
        searchUrl = SCREENER_URL + str(xf['Security Code'])
        print("Get data using 'Security Code', ",searchUrl)
        response = requests.get(searchUrl, stream=True, headers=headers)
    
    # If we have a successful response
    if(response.status_code == 200):
        # Parse the response using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        # GET THE COMPANY PROFILE
        # comp_profile = soup.find(id="company-profile")
        # print("\nCompany profile: ", comp_profile.get_text())
        # # print(soup)

        # GET COMPANY RATIOS
        section_tag = soup.section
        rows_ratios = section_tag.contents
        rows_section = section_tag.findAll('li')
        # print(rows_section[0].get_text())

        marketCap = validateAndConvert(rows_section[0].get_text().split('\n')[5].replace(',',''))
        currentPrice = validateAndConvert(rows_section[1].get_text().split('\n')[5].replace(',',''))
        yearHigh = validateAndConvert(rows_section[2].get_text().split('\n')[3].split('/')[0].replace(',',''))
        yearLow = validateAndConvert(rows_section[2].get_text().split('\n')[3].split('/')[1].replace(',',''))
        # RELATION BETWEEN CURRENT PRICE AND 52 WEEKS CHART
        try:
            currentPriceVar = (currentPrice - yearLow)*100/(yearHigh - yearLow)
        except ZeroDivisionError:
            currentPriceVar = 0

        currentPriceVar = round(currentPriceVar,2)
        peRatio = validateAndConvert(rows_section[4].get_text().split('\n')[4].replace(',',''))
        dividYeild = validateAndConvert(rows_section[5].get_text().split('\n')[4].replace(',',''))
        roce = validateAndConvert(rows_section[6].get_text().split('\n')[4].replace(',',''))
        roe = validateAndConvert(rows_section[7].get_text().split('\n')[4].replace(',',''))
        salesGrowth = validateAndConvert(rows_section[8].get_text().split('\n')[4].replace(',',''))

        #""GET THE SECTOR OF THE COMPANY""
        peer_tag = soup.find(id="peers")
        sector = peer_tag.small.get_text().split('\n')[3]
        industry = peer_tag.small.get_text().split('\n')[8]

        data = {}
        data['status'] = response.status_code
        data['securityName'] = xf['Security Name']
        data['bseSector'] = xf['Industry']
        data['sector'] = sector
        data['industry'] = industry
        data['marketCap'] = marketCap
        data['yearHigh'] = yearHigh
        data['yearLow'] = yearLow
        data['cmp'] = currentPrice
        data['currentPriceVar'] = currentPriceVar
        data['peRatio'] = peRatio
        data['dividYield'] = dividYeild
        data['roce'] = roce
        data['roe'] = roe
        data['salesGrowth'] = salesGrowth
    elif(response.status_code==404):
        data = {}
        data['status'] = response.status_code
        data['securityName'] = xf['Security Name']
        data['bseSector'] = xf['Industry']
        data['sector'] = 'na'
        data['industry'] = 'na'
        data['marketCap'] = 'na'
        data['yearHigh'] = 'na'
        data['yearLow'] = 'na'
        data['cmp'] = 'na'
        data['currentPriceVar'] = 0
        data['peRatio'] = 0
        data['dividYield'] = 0
        data['roce'] = 0
        data['roe'] = 0
        data['salesGrowth'] = 0
    
    return data

    # Return the collected data as a dictionary
    # model of dictionary should contain following paramters:
    # status, securityName, sector, industry, marketcap, yearHigh, yearLow, currentPriceVar, peRatio, dividYield, roce, roe, salesgrowth

    # print("Sector: ", sector)
    # print("Industry: ", industry)
    # print("Market cap: ", marketCap)
    # print("52 Weeks HIGH: ", yearHigh)
    # print("52 Weeks LOW: ", yearLow)
    # print("Current price: ", currentPrice, "\tCurrent price variation: ", currentPriceVar,"%")
    # print("PE Ratio: ", peRatio)
    # print("Dividend Yeild: ", dividYeild,"%")
    # print("ROCE: ", roce,"%")
    # print("ROE: ", roe,"%")
    # print("Sales Growth: ", salesGrowth,"%")
def handle_IndexError(d,index):
    try:
        return d[index]
    except IndexError:
        return 'na'
def parse_screener_data(security_df):
    # Search with 'Security Id' (which is like RIL in case of Reliance Industries)
    searchUrl = SCREENER_URL + security_df['Security Id']
    print(">>>>>>\nGetting data from ", searchUrl)
    response = get_response(searchUrl)
    
    # If we are not getting a response using the 'Security Id' try with 'Security Code'
    if(response.status_code == 404):
        searchUrl = SCREENER_URL + str(security_df['Security Code'])
        print("Get data using 'Security Code', ",searchUrl)
        response = get_response(searchUrl)
    t_wait = random.randint(1,4)
    # Wait for particular time
    time.sleep(t_wait)
    # initialise a dictionary for storing data temporarily
    data = {}
    # following data will be the same irrespective of the response from web
    data['status'] = response.status_code
    data['security_name'] = security_df['Security Name']
    data['bse_sector'] = security_df['Industry']

    # if we have successful request from the web
    if(response.status_code == 200):
        # Parse the response using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        # Since company data is included in the id top get data with id=top
        id_top = soup.find(id="top")
        # # from top id get paragraph where company profile is mentioned
        # comp_profile = id_top.p.get_text()
        
        # get top ratios of the company
        id_top_ratios = soup.find(id="top-ratios")
        top_ratios = id_top_ratios.findAll('li')
        # print(top_ratios)
        # Get market cap from top-ratios
        # function validateAndConvert will remove commas and non numeric values from the data
        market_cap = validateAndConvert(handle_IndexError(top_ratios[0].get_text().split(),3).replace(',',''))
        current_price = validateAndConvert(handle_IndexError(top_ratios[1].get_text().split(),3).replace(',',''))
        year_high = validateAndConvert(handle_IndexError(top_ratios[2].get_text().split(),4).replace(',',''))
        year_low = validateAndConvert(handle_IndexError(top_ratios[2].get_text().split(),6).replace(',',''))
        pe = validateAndConvert(handle_IndexError(top_ratios[3].get_text().split(),2).replace(',',''))
        # inorder to avoid ZeroDivisionError, using error handling method calculate following
        try:
            current_price_var = round((current_price - year_low)*100/(year_high - year_low),2)
        except ZeroDivisionError:
            current_price_var = 0
        book_value = validateAndConvert(handle_IndexError(top_ratios[4].get_text().split(),3).replace(',',''))
        dividend_yield = validateAndConvert(handle_IndexError(top_ratios[5].get_text().split(),2).replace(',',''))
        roce = validateAndConvert(handle_IndexError(top_ratios[6].get_text().split(),1).replace(',',''))
        roe = validateAndConvert(handle_IndexError(top_ratios[7].get_text().split(),1).replace(',',''))
        face_value = validateAndConvert(handle_IndexError(top_ratios[8].get_text().split(),3).replace(',',''))

        # print(market_cap,current_price,year_high,year_low,pe,book_value,dividend_yield,roe,face_value)
        # For getting bse links for the company
        all_a = id_top.findAll('a')
        bse_link = all_a[1].get('href')
        # top_ratios = top_ratios.split()
        # print(top_ratios[0])

        
        # data['sector'] = sector
        # data['industry'] = industry
        data['market_cap'] = market_cap
        data['year_high'] = year_high
        data['year_low'] = year_low
        data['current_price'] = current_price
        data['current_price_var'] = current_price_var
        data['pe'] = pe
        data['book_value'] = book_value
        data['dividend_yield'] = dividend_yield
        data['roce'] = roce
        data['roe'] = roe
        data['face_value'] = face_value
        # data['sales_growth'] = sales_growth
        data['bse_link'] = bse_link
        # for key in data:
        #     print(key,": ",data[key])
    else:
        # if the request was not successful return na values with
        data['market_cap'] = 'na'
        data['year_high'] = 'na'
        data['year_low'] = 'na'
        data['current_price'] = 'na'
        data['current_price_var'] = 'na'
        data['pe'] = 'na'
        data['book_value'] = 'na'
        data['roce'] = 'na'
        data['roe'] = 'na'
        data['face_value'] = 'na'
        data['bse_link'] = 'na'
    for key in data:
        print(key,": ",data[key])
    
    return data

def get_response(url):
    headers = {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0'}
    response = requests.get(url, stream=True, headers=headers)
    return response


    

def get_log():
    # file_name = fa.LOG_PATH+'log.dat'
    if(path.exists(LOG_FILE)):
        # print("File exists")
        log_f = open(LOG_FILE,'r')
        beginIndex = int(log_f.read())
        log_f.close()
    else:
        print("No log file found!!")
        beginIndex = 0
    
    return beginIndex

def write_log(i):
    if(path.exists(LOG_FILE)):
        # print("File exists")
        log_f = open(LOG_FILE,'w')
        # print('Loging ',i)
        log_f.write(str(i))
        log_f.close()
    else:
        print("No such file, so creating a log file and writing log.")
        log_f = open(LOG_FILE,'w')
        log_f.write(str(i))
        log_f.close()

def csv_append(fileName, data):
    columnNames = ['status','securityName','bseSector','sector','industry','marketCap','yearHigh','yearLow','cmp',
                    'currentPriceVar','peRatio','dividYield','roce','roe','salesGrowth']
    try:
        f = open(fileName, 'a')
        writer = csv.DictWriter(f,fieldnames = columnNames)
        writer.writerow(data)
    except IOError:
        print("IOError")

def csv_write_data(file_name, data):
    columnNames = ['status','security_name','market_cap','year_high','year_low','current_price',
                    'current_price_var','pe','book_value','dividend_yield','roce','roe','bse_link','bse_sector','face_value']
    try:
        # if path exists write files in append mode
        if(os.path.exists(file_name)):
            f = open(file_name,'a')
            writer = csv.DictWriter(f,fieldnames=columnNames)
            writer.writerow(data)
        else:
            f = open(file_name,'w')
            writer = csv.DictWriter(f,fieldnames=columnNames)
            writer.writeheader()
            writer.writerow(data)
    except IOError:
        print("IOError")

def get_filename(fileType='csv',date='today'):
    """
    Function to generate file name based on date
    Input: type of file to be generated either as csv or xls, default is csv
    Output: a file name based on date with the defined filetype
    """
    if(date=='today'):
        dateNow = datetime.datetime.now()
    
    if(fileType == 'csv'):
        return dateNow.strftime('%Y') + dateNow.strftime('%B') + dateNow.strftime('%d')+'.csv'
    elif(fileType == 'xls'):
        return dateNow.strftime('%Y') + dateNow.strftime('%B') + dateNow.strftime('%d')+'.xls'

def create_csv_file(fileType,header):
    dateNow = datetime.datetime.now()
    if(fileType == 'csv'):
        fileName = OUTPUT_PATH + dateNow.strftime('%Y') + dateNow.strftime('%B') + dateNow.strftime('%d')+'.csv'
        # Create a file and write headers
        try:
            f = open(fileName,'w')
            writer = csv.DictWriter(f,fieldnames=header)
            writer.writeheader()
        except IOError:
            print("IOError")

    # elif(fileType == 'xls'):
    #     fileName = OUTPUT_PATH + dateNow.strftime('%Y') + dateNow.strftime('%B') + dateNow.strftime('%d')+'.csv'

def get_group(df,columnName):
    """
    Input a dataframe, and a column name which have to be grouped
    Output a list of strings which is grouped, 
    for example if it contains sector with multiple occurance it will return one sector
    """
    df = df.sort_values(by=columnName)
    allSector = df[columnName]
    temp = None
    sectorList = []
    for r in allSector:
        if(r!=temp and type(r)==str):
            sectorList.append(r)
        temp = r
    return sectorList

def display_list(data):
    """
    Function to display a list with indexes,
    Input: a list or series
    Output: return nothing, just display data with index
    """
    for i in range(0,len(data)):
        print(i,": ",data[i])

def get_capwise_data(file,cap):
    # display_list(fileList)
    # file_to_read = OUTPUT_PATH + "2020October04.csv"
    # df = pd.read_csv(file_to_read,header=0,dtype={'marketCap':float},na_values=['nan'])
    # Read csv file with na as na_values
    df = pd.read_csv(file,na_values=['nan','na'])
    # Remove all na
    # df.fillna(0,inplace=True)
    df.dropna(inplace=True)
    df.market_cap = df.market_cap.astype('float')
    if(cap == 'LARGE'):
        df = df[df['market_cap']>=LARGE_CAP]
        return df
    elif(cap == 'MID'):
        df = df[df['market_cap']>=MID_CAP]
        df = df[df['market_cap']<LARGE_CAP]
        return df
    elif(cap == 'SMALL'):
        df = df[df['market_cap']<MID_CAP]
        return df
    else:
        print("INPUT IS NOT VALID")
    



if __name__ == "__main__":
    print("Testing ")
    # fileList = os.listdir(OUTPUT_PATH)
    # # display_list(fileList)
    # file_to_read = OUTPUT_PATH + "2020October04.csv"
    
    # df = get_capwise_data(file_to_read,'SMALL')
    # print(df.head())
    # print(df.tail())
    # Generate a df for analysis from the file containing list of equities in bse
    # df = pd.read_csv(BSE_DATA_FILE)
    # # Select dataframe where instrument is Equity
    # dfEq = df[df['Instrument']=='Equity']
    # target_security = dfEq.iloc[25]
    # data = parse_screener_data(target_security)
    # for key in data:
    #     print(key,': ',data[key])

    # names = ["jeril"]
    # print(handle_IndexError(names,1))
    # bse_url = "https://www.bseindia.com/stock-share-price/hindustan-organic-chemicals-ltd/HOCL/500449/"
    # response = get_response(bse_url)
    # # convert the response text to a html parser
    # soup = BeautifulSoup(response.text, "html.parser")
    # # target_id = soup.find(id='getquoteheader')
    # # tr = target_id.findAll('td')
    # print(soup)

    sectorwise_path = OUTPUT_PATH + 'sectorwisefa/'
    # Get only directories from the sectorwise analysis path
    dirs = [os.path.join(sectorwise_path, f) for f in os.listdir(sectorwise_path) if os.path.isdir(os.path.join(sectorwise_path, f))]
    print(dirs[1])
    # for files in os.listdir(dirs[1]):
    #     df = pd.read_csv(dirs[1] + '/' + files)
    #     print(df.head())
    sectorwise_files = os.listdir(dirs[1])

    """Index(['Unnamed: 0', 'status', 'security_name', 'market_cap', 'year_high',
       'year_low', 'current_price', 'current_price_var', 'pe', 'book_value',
       'dividend_yield', 'roce', 'roe', 'bse_link', 'bse_sector',
       'face_value'],"""


    for i in range(0,len(sectorwise_files)):
        sector_index = i
        # print(sectorwise_files[sector_index])
        print("\n\nYou selected {} for evaluation.".format(sectorwise_files[sector_index]))
        df = pd.read_csv(dirs[1]+'/'+sectorwise_files[sector_index],usecols=['security_name','pe','roce','bse_sector'])
        # select dataframe with pe value more than 0
        df = df[df['pe']>0]
        # estimate the median
        median = df.median()
        df = df[df['pe']<median['pe']]
        df = df[df['roce']>median['roce']]
        # sort the dataframe in descending order
        df = df.sort_values(by=['pe'], ascending=False)
        print(df.head())
        # print(df.tail())
        # # print(df.head())
        # # print("\nStandard deviation\n",df.std())
        # # print("\nMean\n",df.mean().values)
        # pe_mean = df.mean().values[0]
        # pe_std = df.std().values[0]

        # print("\nAll companies with PE less than mean PE",df[df['pe']<=pe_mean])
        # print("\nCompanies with PE less than std PE",df[df['pe']<=pe_std])
