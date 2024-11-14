import time
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

today = datetime.today()
ten_years_ago = today - timedelta(days=3650)

def get_options():  # funkcija sto gi zima site opcii od listat izdavaci, vrakja niza od izdavaci bez broj/ne se hartija od vrednost
    currencies = [
        'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN',
        'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL',
        'BSD', 'BTN', 'BWP', 'BYN', 'BZD', 'CAD', 'CDF', 'CHF', 'CLP', 'CNY',
        'COP', 'CRC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EGP',
        'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'FOK', 'GBP', 'GEL', 'GGP', 'GHS',
        'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF',
        'IDR', 'ILS', 'IMP', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY',
        'KES', 'KGS', 'KHR', 'KID', 'KMF', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK',
        'LBP', 'LKR', 'LRD', 'LSL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK',
        'MNT', 'MOP', 'MRU', 'MUR', 'MVR', 'MWK', 'MXN', 'MYR', 'MZN', 'NAD',
        'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP',
        'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD',
        'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLE', 'SLL', 'SOS', 'SRD', 'SSP',
        'STN', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD',
        'TVD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS', 'VES', 'VND',
        'VUV', 'WST', 'XAF', 'XCD', 'XDR', 'XOF', 'XPF', 'YER', 'ZAR', 'ZMW', 'ZWL'
    ]

    url = 'https://www.mse.mk/mk/stats/symbolhistory/STB'
    response = requests.get(url)
    parse = BeautifulSoup(response.content, 'html.parser')
    select_element = parse.find("select", {"id": "Code"})
    options = select_element.find_all("option")
    option_values = [option.get("value") for option in options]
    no_num_val = [value for value in option_values if not re.search(r'\d', value)]
    no_curr_val = [option for option in no_num_val if option not in currencies]
    print(no_curr_val)

    return no_curr_val

def fetch_data_for_year_block(opt, from_date, to_date):
    data = {
        "FromDate": from_date.strftime("%d.%m.%Y"),
        "ToDate": to_date.strftime("%d.%m.%Y"),
        "Code": opt
    }
    url = f'https://www.mse.mk/mk/stats/symbolhistory/{opt}'
    response = requests.post(url, data=data, timeout=10)
    parse = BeautifulSoup(response.content, 'html.parser')
    table = parse.find('table')
    if not table:
        return None
    rows = table.find_all('tr')
    headers = [header.text.strip() for header in rows[0].find_all('th')]
    headers.insert(0, "Шифра на компанија")
    data = []
    for row in rows[1:]:
        cells = row.find_all('td')
        row_data = [cell.text.strip() for cell in cells]
        row_data.insert(0, opt)
        data.append(row_data)
    return pd.DataFrame(data, columns=headers)

def fetch_all_data_for_opt(opt):
    collected_data = []
    to_date = today
    while to_date > ten_years_ago:
        from_date = to_date - timedelta(days=364)
        df = fetch_data_for_year_block(opt, from_date, to_date)
        if df is not None:
            collected_data.append(df)
        to_date -= timedelta(days=364)
    if collected_data:
        return pd.concat(collected_data, ignore_index=True)
    return None

def format_mk_price(number):
    num_str = str(number)

    parts = num_str.split(".")

    if len(parts) > 1:
        port = parts.pop()
        port = ',' + port
        parts[-1] = parts[-1].replace('.', ',')
        return '.'.join(parts) + port

    # Za sega e vaka posto ne raboti
    return number

def format_prices(df):
    if len(df.columns) >= 3:
        df.iloc[:, -2] = df.iloc[:, -2].apply(format_mk_price)
        df.iloc[:, -1] = df.iloc[:, -1].apply(format_mk_price)
    return df

def get_table_values():
    opts = get_options()
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(fetch_all_data_for_opt, opts)
    all_data = pd.concat([df for df in results if df is not None], ignore_index=True)
    all_data = format_prices(all_data)
    all_data.to_csv('mk_stock_data1.csv', index=False, encoding='utf-8-sig')

if __name__ == "__main__":

    start_time = time.time()
    get_table_values()
    execution_time = time.time() - start_time
    print(f"Execution time: {execution_time:.2f} seconds")
