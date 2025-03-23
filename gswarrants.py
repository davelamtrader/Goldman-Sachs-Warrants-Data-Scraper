import requests
from bs4 import BeautifulSoup
import csv
import json
import re
import time
from datetime import date, datetime, timedelta
import pandas as pd
import os
import multiprocessing
from multiprocessing import Manager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException, ElementNotInteractableException, ElementClickInterceptedException

pd.options.display.width=None
pd.options.display.max_columns=None
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_colwidth', 100)


def get_market_value_and_total_moneyflow(chrome_path):
    url = 'https://www.gswarrants.com.hk/en/market/warrant-cbbc-data'
    webdriver_service = Service(chrome_path)
    chrome_options = Options()
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    driver.get(url)
    time.sleep(1)
    accept = driver.find_element(By.XPATH, '//*[@id="opening-disclaimer"]/div/div/div[1]/div/div[1]/div[2]/button')
    accept.click()
    time.sleep(1)
    table = driver.find_element(By.ID, 'market-data-table2')
    header_elements = table.find_elements(By.TAG_NAME, 'th')[1:]
    dates = [str(datetime.today().year) + '-' + h.text for h in header_elements]
    warrant_mktval_row = table.find_elements(By.TAG_NAME, 'tr')[1]
    warrant_mktval_elements = warrant_mktval_row.find_elements(By.TAG_NAME, 'td')[1:]
    warrant_mktval_list = [int(e.text.split('\n')[0].replace(',', '')) for e in warrant_mktval_elements]
    warrant_mktval_chg_list = [e.text.split('\n')[2][1:-1] for e in warrant_mktval_elements]
    cbbc_mkt_val_row = table.find_elements(By.TAG_NAME, 'tr')[2]
    cbbc_mktval_elements = cbbc_mkt_val_row.find_elements(By.TAG_NAME, 'td')[1:]
    cbbc_mktval_list = [int(e.text.split('\n')[0].replace(',', '')) for e in cbbc_mktval_elements]
    cbbc_mktval_chg_list = [e.text.split('\n')[2][1:-1] for e in cbbc_mktval_elements]
    warrant_mflow_row = table.find_elements(By.TAG_NAME, 'tr')[3]
    warrant_mflow_elements = warrant_mflow_row.find_elements(By.TAG_NAME, 'td')[1:]
    warrant_mflows = [float(e.text.replace('\n', '')) for e in warrant_mflow_elements]
    cbbc_mflow_row = table.find_elements(By.TAG_NAME, 'tr')[4]
    cbbc_mflow_elements = cbbc_mflow_row.find_elements(By.TAG_NAME, 'td')[1:]
    cbbc_mflows = [float(e.text.replace('\n', '')) for e in cbbc_mflow_elements]

    head = ['date', 'warrant_market_value (M)', 'warrant_mktval_chg %', 'cbbc_market_value (M)', 'cbbc_mktval_chg %', 'warrant_moneyflow (M)', 'cbbc_moneyflow (M)']
    d1 = [dates[0], warrant_mktval_list[0], warrant_mktval_chg_list[0], cbbc_mktval_list[0], cbbc_mktval_chg_list[0], warrant_mflows[0], cbbc_mflows[0]]
    d2 = [dates[1], warrant_mktval_list[1], warrant_mktval_chg_list[1], cbbc_mktval_list[1], cbbc_mktval_chg_list[1], warrant_mflows[1], cbbc_mflows[1]]
    d3 = [dates[2], warrant_mktval_list[2], warrant_mktval_chg_list[2], cbbc_mktval_list[2], cbbc_mktval_chg_list[2], warrant_mflows[2], cbbc_mflows[2]]
    d4 = [dates[3], warrant_mktval_list[3], warrant_mktval_chg_list[3], cbbc_mktval_list[3], cbbc_mktval_chg_list[3], warrant_mflows[3], cbbc_mflows[3]]
    d5 = [dates[4], warrant_mktval_list[4], warrant_mktval_chg_list[4], cbbc_mktval_list[4], cbbc_mktval_chg_list[4], warrant_mflows[4], cbbc_mflows[4]]

    sub = 'metrics/market value and moneyflow'
    os.makedirs(sub, exist_ok=True)
    filepath = os.path.join(sub, f'market_value_and_total_moneyflow_{dates[0]}.csv')
    df = pd.DataFrame(data=[d5, d4, d3, d2, d1], columns=head)
    df.to_csv(filepath, index=False)
    print(df)
    return df


def get_top20moneyflow_1d(chrome_path):
    url = 'https://www.gswarrants.com.hk/en/market/warrant-cbbc-moneyflow'
    webdriver_service = Service(chrome_path)
    chrome_options = Options()
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    driver.get(url)
    time.sleep(1)
    accept = driver.find_element(By.XPATH, '//*[@id="opening-disclaimer"]/div/div/div[1]/div/div[1]/div[2]/button')
    accept.click()
    time.sleep(1)

    type_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[2]/div')
    type_button.click()
    all_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-mflow_wtypeSelected"]/div[2]/div/div[3]/button')
    all_button.click()
    time.sleep(1)
    day_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[3]/div')
    day_button.click()
    d1_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-daySelected"]/div[2]/div/div[1]/button')
    d1_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    rises = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    inflow_values = [float(r.text.split('\n', 1)[0][1:]) for r in rises]
    rank_values = [i for i in range(1, 21)]
    current_date = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[2]/div[2]').text[-10:]

    in_head = ['rank', 'name', 'inflow (M)']
    in_dict_list = []
    for rank, name, inflow in zip(rank_values, names_values, inflow_values):
        row_values = [rank, name, inflow]
        single_dict = dict(zip(in_head, row_values))
        in_dict_list.append(single_dict)

    sign_dict = {'rise': '+', 'drop': '-'}
    hsi_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].text
    hsi_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].get_attribute('class')[-4:]
    hsi_long_val = sign_dict[hsi_long_sign] + hsi_long
    hsi_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].text
    hsi_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].get_attribute('class')[-4:]
    hsi_short_val = sign_dict[hsi_short_sign] + hsi_short

    stocks_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].text
    stocks_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].get_attribute('class')[-4:]
    stocks_long_val = sign_dict[stocks_long_sign] + stocks_long
    stocks_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].text
    stocks_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].get_attribute('class')[-4:]
    stocks_short_val = sign_dict[stocks_short_sign] + stocks_short
    stats_dict = {'hsi long pos': hsi_long_val, 'hsi short pos:': hsi_short_val, 'stocks long pos': stocks_long_val, 'stocks short pos': stocks_short_val}

    #################################################################
    outflow_button = driver.find_element(By.XPATH, "//label[@class=\'btn btn-outline-secondary\']")
    outflow_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    drops = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    outflow_values = [float(d.text.split('\n', 1)[0][1:]) for d in drops]
    rank_values = [i for i in range(1, 21)]

    out_head = ['rank', 'name', 'outflow (M)']
    out_dict_list = []
    for rank, name, outflow in zip(rank_values, names_values, outflow_values):
        row_values = [rank, name, outflow]
        single_dict = dict(zip(out_head, row_values))
        out_dict_list.append(single_dict)

    result_dict = {'top20inflow_1d': in_dict_list, 'top20outflow_1d': out_dict_list, 'stats': stats_dict}
    sub = 'metrics/top 20 moneyflow'
    os.makedirs(sub, exist_ok=True)
    filepath = os.path.join(sub, f'top20_moneyflow_1d_{current_date}.json')
    with open(filepath, 'w') as json_file:
        json.dump(result_dict, json_file)

    print(result_dict)
    return result_dict


def get_top20moneyflow_5d(chrome_path):
    url = 'https://www.gswarrants.com.hk/en/market/warrant-cbbc-moneyflow'
    webdriver_service = Service(chrome_path)
    chrome_options = Options()
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    driver.get(url)
    time.sleep(1)
    accept = driver.find_element(By.XPATH, '//*[@id="opening-disclaimer"]/div/div/div[1]/div/div[1]/div[2]/button')
    accept.click()
    time.sleep(1)

    type_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[2]/div')
    type_button.click()
    all_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-mflow_wtypeSelected"]/div[2]/div/div[3]/button')
    all_button.click()
    time.sleep(1)
    day_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[3]/div')
    day_button.click()
    d5_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-daySelected"]/div[2]/div/div[5]/button')
    d5_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    rises = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    inflow_values = [float(r.text.split('\n', 1)[0][1:]) for r in rises]
    rank_values = [i for i in range(1, 21)]
    current_date = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[2]/div[2]').text[-10:]

    in_head = ['rank', 'name', 'inflow (M)']
    in_dict_list = []
    for rank, name, inflow in zip(rank_values, names_values, inflow_values):
        row_values = [rank, name, inflow]
        single_dict = dict(zip(in_head, row_values))
        in_dict_list.append(single_dict)

    sign_dict = {'rise': '+', 'drop': '-'}
    hsi_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].text
    hsi_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].get_attribute('class')[-4:]
    hsi_long_val = sign_dict[hsi_long_sign] + hsi_long
    hsi_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].text
    hsi_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].get_attribute('class')[-4:]
    hsi_short_val = sign_dict[hsi_short_sign] + hsi_short

    stocks_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].text
    stocks_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].get_attribute('class')[-4:]
    stocks_long_val = sign_dict[stocks_long_sign] + stocks_long
    stocks_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].text
    stocks_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].get_attribute('class')[-4:]
    stocks_short_val = sign_dict[stocks_short_sign] + stocks_short
    stats_dict = {'hsi long pos': hsi_long_val, 'hsi short pos:': hsi_short_val, 'stocks long pos': stocks_long_val, 'stocks short pos': stocks_short_val}

    #################################################################
    outflow_button = driver.find_element(By.XPATH, "//label[@class=\'btn btn-outline-secondary\']")
    outflow_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    drops = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    outflow_values = [float(d.text.split('\n', 1)[0][1:]) for d in drops]
    rank_values = [i for i in range(1, 21)]

    out_head = ['rank', 'name', 'outflow (M)']
    out_dict_list = []
    for rank, name, outflow in zip(rank_values, names_values, outflow_values):
        row_values = [rank, name, outflow]
        single_dict = dict(zip(out_head, row_values))
        out_dict_list.append(single_dict)

    result_dict = {'top20inflow_5d': in_dict_list, 'top20outflow_5d': out_dict_list, 'stats': stats_dict}
    sub = 'metrics/top 20 moneyflow'
    os.makedirs(sub, exist_ok=True)
    filepath = os.path.join(sub, f'top20_moneyflow_5d_{current_date}.json')
    with open(filepath, 'w') as json_file:
        json.dump(result_dict, json_file)

    print(result_dict)
    return result_dict


def get_top20moneyflow_10d(chrome_path):
    url = 'https://www.gswarrants.com.hk/en/market/warrant-cbbc-moneyflow'
    webdriver_service = Service(chrome_path)
    chrome_options = Options()
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    driver.get(url)
    time.sleep(1)
    accept = driver.find_element(By.XPATH, '//*[@id="opening-disclaimer"]/div/div/div[1]/div/div[1]/div[2]/button')
    accept.click()
    time.sleep(1)

    type_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[2]/div')
    type_button.click()
    all_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-mflow_wtypeSelected"]/div[2]/div/div[3]/button')
    all_button.click()
    time.sleep(1)
    day_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[3]/div')
    day_button.click()
    d10_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-daySelected"]/div[2]/div/div[6]/button')
    d10_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    rises = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    inflow_values = [float(r.text.split('\n', 1)[0][1:]) for r in rises]
    rank_values = [i for i in range(1, 21)]
    current_date = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[2]/div[2]').text[-10:]

    in_head = ['rank', 'name', 'inflow (M)']
    in_dict_list = []
    for rank, name, inflow in zip(rank_values, names_values, inflow_values):
        row_values = [rank, name, inflow]
        single_dict = dict(zip(in_head, row_values))
        in_dict_list.append(single_dict)

    sign_dict = {'rise': '+', 'drop': '-'}
    hsi_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].text
    hsi_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].get_attribute('class')[-4:]
    hsi_long_val = sign_dict[hsi_long_sign] + hsi_long
    hsi_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].text
    hsi_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].get_attribute('class')[-4:]
    hsi_short_val = sign_dict[hsi_short_sign] + hsi_short

    stocks_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].text
    stocks_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].get_attribute('class')[-4:]
    stocks_long_val = sign_dict[stocks_long_sign] + stocks_long
    stocks_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].text
    stocks_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].get_attribute('class')[-4:]
    stocks_short_val = sign_dict[stocks_short_sign] + stocks_short
    stats_dict = {'hsi long pos': hsi_long_val, 'hsi short pos:': hsi_short_val, 'stocks long pos': stocks_long_val, 'stocks short pos': stocks_short_val}

    #################################################################
    outflow_button = driver.find_element(By.XPATH, "//label[@class=\'btn btn-outline-secondary\']")
    outflow_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    drops = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    outflow_values = [float(d.text.split('\n', 1)[0][1:]) for d in drops]
    rank_values = [i for i in range(1, 21)]

    out_head = ['rank', 'name', 'outflow (M)']
    out_dict_list = []
    for rank, name, outflow in zip(rank_values, names_values, outflow_values):
        row_values = [rank, name, outflow]
        single_dict = dict(zip(out_head, row_values))
        out_dict_list.append(single_dict)

    result_dict = {'top20inflow_10d': in_dict_list, 'top20outflow_10d': out_dict_list, 'stats': stats_dict}
    sub = 'metrics/top 20 moneyflow'
    os.makedirs(sub, exist_ok=True)
    filepath = os.path.join(sub, f'top20_moneyflow_10d_{current_date}.json')
    with open(filepath, 'w') as json_file:
        json.dump(result_dict, json_file)

    print(result_dict)
    return result_dict


def get_top20moneyflow_20d(chrome_path):
    url = 'https://www.gswarrants.com.hk/en/market/warrant-cbbc-moneyflow'
    webdriver_service = Service(chrome_path)
    chrome_options = Options()
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    driver.get(url)
    time.sleep(1)
    accept = driver.find_element(By.XPATH, '//*[@id="opening-disclaimer"]/div/div/div[1]/div/div[1]/div[2]/button')
    accept.click()
    time.sleep(1)

    type_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[2]/div')
    type_button.click()
    all_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-mflow_wtypeSelected"]/div[2]/div/div[3]/button')
    all_button.click()
    time.sleep(1)
    day_button = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[1]/div[3]/div')
    day_button.click()
    d20_button = driver.find_element(By.XPATH, '//*[@id="custom-modal-daySelected"]/div[2]/div/div[7]/button')
    d20_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    rises = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    inflow_values = [float(r.text.split('\n', 1)[0][1:]) for r in rises]
    rank_values = [i for i in range(1, 21)]
    current_date = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[2]/div[2]').text[-10:]

    in_head = ['rank', 'name', 'inflow (M)']
    in_dict_list = []
    for rank, name, inflow in zip(rank_values, names_values, inflow_values):
        row_values = [rank, name, inflow]
        single_dict = dict(zip(in_head, row_values))
        in_dict_list.append(single_dict)

    sign_dict = {'rise': '+', 'drop': '-'}
    hsi_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].text
    hsi_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[0].get_attribute('class')[-4:]
    hsi_long_val = sign_dict[hsi_long_sign] + hsi_long
    hsi_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].text
    hsi_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[1].get_attribute('class')[-4:]
    hsi_short_val = sign_dict[hsi_short_sign] + hsi_short

    stocks_long = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].text
    stocks_long_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[2].get_attribute('class')[-4:]
    stocks_long_val = sign_dict[stocks_long_sign] + stocks_long
    stocks_short = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].text
    stocks_short_sign = driver.find_elements(By.CLASS_NAME, 'mflow-box__value')[3].get_attribute('class')[-4:]
    stocks_short_val = sign_dict[stocks_short_sign] + stocks_short
    stats_dict = {'hsi long pos': hsi_long_val, 'hsi short pos:': hsi_short_val, 'stocks long pos': stocks_long_val, 'stocks short pos': stocks_short_val}

    #################################################################
    outflow_button = driver.find_element(By.XPATH, "//label[@class=\'btn btn-outline-secondary\']")
    outflow_button.click()
    time.sleep(1)

    chart = driver.find_element(By.XPATH, '//*[@id="mm-0"]/div[1]/main/div[2]/div[3]/div[2]/div[2]/div[1]/div/div[2]')
    names = chart.find_elements(By.CLASS_NAME, 'money-flow-chart__text')
    names_values = [n.text for n in names]
    drops = driver.find_elements(By.CLASS_NAME, 'money-flow-chart__item')
    outflow_values = [float(d.text.split('\n', 1)[0][1:]) for d in drops]
    rank_values = [i for i in range(1, 21)]

    out_head = ['rank', 'name', 'outflow (M)']
    out_dict_list = []
    for rank, name, outflow in zip(rank_values, names_values, outflow_values):
        row_values = [rank, name, outflow]
        single_dict = dict(zip(out_head, row_values))
        out_dict_list.append(single_dict)

    result_dict = {'top20inflow_20d': in_dict_list, 'top20outflow_20d': out_dict_list, 'stats': stats_dict}
    sub = 'metrics/top 20 moneyflow'
    os.makedirs(sub, exist_ok=True)
    filepath = os.path.join(sub, f'top20_moneyflow_20d_{current_date}.json')
    with open(filepath, 'w') as json_file:
        json.dump(result_dict, json_file)

    print(result_dict)
    return result_dict


def get_underlyings_volatility():
    sub = 'underlyings volatility'
    os.makedirs(sub, exist_ok=True)
    code_url = 'https://www.gswarrants.com.hk/hk/data/udata.txt'
    code_response = requests.get(code_url)
    codes_data = code_response.json()['data'] if code_response.status_code == 200 else None
    raw_codes = [item['ucode'] for item in codes_data] if codes_data is not None else []
    codes = [c for c in raw_codes if c.isnumeric() == True]

    for code in codes:
        url = f'https://www.gswarrants.com.hk/tc/data/chart/hvChart/ucode/{code}'
        response = requests.get(url)
        data = response.json()['mainData'] if response.status_code == 200 and 'mainData' in list(response.json().keys()) else None
        sdate = response.json()['sdate'] if response.status_code == 200 and 'sdate' in list(response.json().keys()) else datetime.today().strftime('%Y-%m-%d')

        if type(data) == list:
            symbol_path = os.path.join(sub, code)
            os.makedirs(symbol_path, exist_ok=True)
            filepath = os.path.join(symbol_path, f'{code}_volatility_{sdate}.json')
            with open(filepath, 'w') as json_file:
                json.dump(data, json_file)
            print(f'Successfully fetch historical volatility data of {code}!')
        else:
            print(f'Code:{response.status_code} | Some error occurs when fetching volatility data of {code}.')


if __name__ == "__main__":

    today = datetime.today().strftime('%Y%m%d')
    yesterday = (datetime.today() - timedelta(days=2)).strftime('%Y%m%d')
    chrome_path = r'C:\Users\user\Downloads\chromedriver-win32\chromedriver.exe'
    rootdir = 'C:\\Users\\user\\Documents\\#Coding\\GS\\'

    get_market_value_and_total_moneyflow(chrome_path)
    get_top20moneyflow_1d(chrome_path)
    get_top20moneyflow_5d(chrome_path)
    get_top20moneyflow_10d(chrome_path)
    get_top20moneyflow_20d(chrome_path)
    get_underlyings_volatility()
