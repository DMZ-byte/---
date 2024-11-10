import locale

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

today = datetime.today()
one_year_ago = today - timedelta(days=364)

from_date = one_year_ago.strftime("%d.%m.%Y")
to_date = today.strftime("%d.%m.%Y")


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
    # Print or store the values
    print(no_curr_val)

    return no_curr_val


def format_price(price):
    try:
        # Attempt to convert the price to float and format
        formatted_price = f"{float(price):,.2f}"  # Comma as thousands separator, dot as decimal
        # Replace commas with dots and vice versa to match Macedonian formatting style
        return formatted_price.replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        # If conversion fails, return the original value
        return price


def add_dates_filter(opt, last_date):
    end_date = last_date - timedelta(
        days=3640)  # funkcija sto go zima posledniot datum dostapen za izdavac opt i dodava se dodeka ne stigne do pred 10 godini
    while last_date >= end_date:
        fromm_date = last_date - timedelta(days=364)
        too_date = last_date

        data = {
            "FromDate": fromm_date.strftime("%d.%m.%Y"),
            "ToDate": too_date.strftime("%d.%m.%Y"),
            "Code": opt
        }

        url = f'https://www.mse.mk/mk/stats/symbolhistory/{opt}'
        print(fromm_date, too_date)
        response = requests.post(url, data=data, timeout=10)
        parse = BeautifulSoup(response.content, 'html.parser')
        table = parse.find('table')
        if table:
            rows = table.find_all('tr')
            headers = [header.text.strip() for header in rows[0].find_all('th')]
            headers.insert(0, "Шифра на компанија")
            data = []
            for row in rows[1:]:
                cells = row.find_all('td')
                row_data = [cell.text.strip() for cell in cells]
                row_data.insert(0, opt)
                data.append(row_data)
            dff = pd.DataFrame(data, columns=headers)
            dff.to_csv('mk_stock_proba13.csv', mode='a', header=False, index=False, encoding='utf-8-sig')
        last_date -= timedelta(days=364)


def get_table_values(arr):
    count = 0
    # originalna funkcija koja sto za sekoj izdavac gi grebe vrednostite od tabelata vo html-ot pravejki
    # post request koj sto go menuva from, to datumi. Vrednostite gi apendira do csv fajlot sto se imenuva.
    for opt in arr:
        data = {
            "FromDate": from_date,
            "ToDate": to_date,
            "Code": opt
        }

        url = f'https://www.mse.mk/mk/stats/symbolhistory/{opt}'
        response = requests.post(url, data=data)
        parse = BeautifulSoup(response.content, 'html.parser')
        table = parse.find('table')
        if not table:
            print(
                f"Нема табела за {opt}.")
            continue
        rows = table.find_all('tr')
        headers = [header.text.strip() for header in rows[0].find_all('th')]
        headers.insert(0, "Шифра на компанија")
        data = []
        for row in rows[1:]:
            cells = row.find_all('td')
            row_data = [cell.text.strip() for cell in cells]
            row_data.insert(0, opt)
            data.append(row_data)
        df = pd.DataFrame(data, columns=headers)
        if not df.empty:
            last_date = df['Датум'].iloc[-1]
            last_date = datetime.strptime(last_date, "%d.%m.%Y")
            print(f'Last available date for {opt} is {last_date}')
        if count == 0:
            df.to_csv('mk_stock_proba13.csv', mode='a', header=True, index=False, encoding='utf-8-sig')
            add_dates_filter(opt, last_date)
        else:
            df.to_csv('mk_stock_proba13.csv', mode='a', header=False, index=False, encoding='utf-8-sig')
            add_dates_filter(opt, last_date)
        count = count + 1
    return parse


def get_tickers():
    link = 'https://feeds.mse.mk/service/FreeMSEFeeds.svc/ticker/JSON'  # Ovaa funkcija bese probna za koristenje na ticker api od makedonskata berza
    my_apicode = 'MY-API-CODE'
    url = f'{link}/{my_apicode}'
    r = requests.get(url)
    if r.status_code == 200:
        print(r.json()['GetTickerJSONResult'])
        return r.json()
    else:
        print('Neuspesen request')
        return None


def filter_price(number):
    num_str = str(number)
    if '.' in num_str:
        part = num_str.rsplit('.', 1)
        return ','.join(part)
    return num_str


if __name__ == "__main__":
    #arr = get_options()
    #get_table_values(arr)
    data = pd.read_csv("mk_stock_proba13.csv")
    data.iloc[:, -2] = data.iloc[:, -2].apply(lambda x: format_price(x) if pd.notnull(x) else x)
    data.iloc[:, -1] = data.iloc[:, -1].apply(lambda x: format_price(x) if pd.notnull(x) else x)
    data.to_csv("formatter_file.csv", index=False)
    print(data)
