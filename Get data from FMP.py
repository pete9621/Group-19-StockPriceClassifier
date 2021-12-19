import fmpsdk
from dotenv import load_dotenv
from dotenv import dotenv_values
import os
import pandas as pd
import numpy as np


load_dotenv()
apikey = os.environ.get("apikey")

#Get list of symbols
symbollistdf = pd.DataFrame(fmpsdk.symbols_list(apikey))

def get_balance_sheets(filepath):
    balance_sheets = []
    no_of_companies = symbollistdf.shape[0]
    i=0
    while i < no_of_companies:
        print(f"percent complete: {i/no_of_companies}")
        balance_sheets.extend(fmpsdk.balance_sheet_statement(apikey,symbollistdf['symbol'][i],limit=0,period="quarter"))
        i += 1

    balanceSheetdf = pd.DataFrame(balance_sheets)
    balanceSheetdf.to_csv(filepath)

def get_income_statements(filepath):
    income_statements = []
    no_of_companies = symbollistdf.shape[0]
    i=0
    #had to split up, was running into errors with size. forgot to remove the header at appending of each chunk, manually delete them
    while i < no_of_companies/4:
        print(f"percent complete: {i/no_of_companies}")
        income_statements.extend(fmpsdk.income_statement(apikey,symbollistdf['symbol'][i],limit=0,period="quarter"))
        i += 1
    income_statementsdf = pd.DataFrame(income_statements)
    income_statementsdf.to_csv(filepath)
    income_statements = []
    del income_statementsdf 


    while i < 2*no_of_companies/4:
        print(f"percent complete: {i/no_of_companies}")
        income_statements.extend(fmpsdk.income_statement(apikey,symbollistdf['symbol'][i],limit=0,period="quarter"))
        i += 1
    income_statementsdf = pd.DataFrame(income_statements)
    income_statementsdf.to_csv(filepath,mode='a')
    income_statements = []
    del income_statementsdf 

    while i < 3*no_of_companies/4:
        print(f"percent complete: {i/no_of_companies}")
        income_statements.extend(fmpsdk.income_statement(apikey,symbollistdf['symbol'][i],limit=0,period="quarter"))
        i += 1
    income_statementsdf = pd.DataFrame(income_statements)
    income_statementsdf.to_csv(filepath,mode='a')
    income_statements = []
    del income_statementsdf 

    while i < no_of_companies:
        print(f"percent complete: {i/no_of_companies}")
        income_statements.extend(fmpsdk.income_statement(apikey,symbollistdf['symbol'][i],limit=0,period="quarter"))
        i += 1
    income_statementsdf = pd.DataFrame(income_statements)
    income_statementsdf.to_csv(filepath,mode='a')
    income_statements = []
    del income_statementsdf 

def get_company_profiles(filepath):
    company_profiles = []
    no_of_companies = symbollistdf.shape[0]
    i=0
    while i < no_of_companies:
        print(f"percent complete: {i/no_of_companies}")
        company_profiles.extend(fmpsdk.company_profile(apikey,symbollistdf['symbol'][i]))
        i += 1

    company_profilesdf = pd.DataFrame(company_profiles)
    company_profilesdf.to_csv(filepath)

def get_cash_flows(filepath):
    cash_flows = []
    no_of_companies = symbollistdf.shape[0]
    i=0
    while i < no_of_companies:
        print(f"percent complete: {i/no_of_companies}")
        cash_flows.extend(fmpsdk.cash_flow_statement(apikey,symbollistdf['symbol'][i],limit=0,period="quarter"))
        i += 1

    cash_flowsdf = pd.DataFrame(cash_flows)
    cash_flowsdf.to_csv(filepath)

def get_stock_prices(filepath):
    stock_prices = []
    no_of_companies = symbollistdf.shape[0]
    i=0
    while i < no_of_companies:
        print(f"percent complete: {i/no_of_companies}")
        stock_tmp = fmpsdk.historical_price_full(apikey,symbollistdf['symbol'][i],series_type="line")
        for stock in stock_tmp:
            stock["symbol"] = symbollistdf['symbol'][i]
        stock_prices.extend(stock_tmp)
        i += 1
    stock_pricesdf = pd.DataFrame(stock_prices)
    stock_pricesdf.to_csv(filepath)

get_income_statements(r"C:\Users\epete\Documents\IncomeStatements1.csv")
get_company_profiles(r"C:\Users\epete\Documents\CompanyProfiles.csv")
get_balance_sheets(r"C:\Users\epete\Documents\BalanceSheets1.csv")
get_cash_flows(r"C:\Users\epete\Documents\CashFlows.csv")
get_stock_prices(r"C:\Users\epete\Documents\StockPrices.csv")