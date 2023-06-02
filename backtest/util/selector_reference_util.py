import os.path
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from backtest.domains.selector_reference import SelectorReference

CSV_REPO_PATH = 'backtest/csvrepo'
GECKO_CSV_PATH = 'coingecko_symbol.csv'
GECKO_SYMBOL_ID_LOOKUP_URL = 'https://www.coingecko.com/en/coins/{id}'
GECKO_COIN_LIST_CSV_PATH = 'coingecko_coin_list.csv'
GECKO_COIN_LIST_API_URL = 'https://api.coingecko.com/api/v3/coins/list'
REGEX_COIN_IMAGE_URL = r'https://assets.coingecko.com/coins/images/(\d+)/.*'


def generate_empty_selector_reference(indexes, symbol='', columns=[]):
    df = pd.DataFrame(columns=columns, index=indexes).fillna(0)
    df = df.rename_axis('date')
    return SelectorReference(symbol=symbol, data=df)


def _search_coingecko_symbol_id(symbol):
    try:
        coin_csv = pd.read_csv(os.path.join(
            CSV_REPO_PATH, GECKO_COIN_LIST_CSV_PATH))
        coin_symbol_id_table = coin_csv.set_index('symbol')
        symbol_id = coin_symbol_id_table.loc[symbol.lower()]['id']
        if not isinstance(symbol_id, str):
            symbol_id = symbol_id[0]
            print('dup symbol symbol_id is :{} SYMBOL : {}'.format(symbol_id, symbol))
        res = requests.get(GECKO_SYMBOL_ID_LOOKUP_URL.format(id=symbol_id))
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, features='lxml')
            twitter_image = soup.find('meta', attrs={'name': 'twitter:image'})
            symbol_image_url = twitter_image['content']
            result = re.search(REGEX_COIN_IMAGE_URL, symbol_image_url)
            if result:
                symbol_id = result.group(1)
                return symbol_id
            else:
                raise Exception
        else:
            raise RequestException
    except FileNotFoundError:
        print("[ERROR] coingeckco_coin_list.csv not found..")
    except RequestException:
        print("[ERROR] requests error of GECKO_SYMBOL_ID_LOOKUP_URL")
    except Exception:
        print("[ERROR] Symbol Not Found..")
    return -1


def get_coingecko_symbol_id(symbol: str) -> int:
    df = None
    try:
        df = pd.read_csv(os.path.join(CSV_REPO_PATH, GECKO_CSV_PATH))
    except FileNotFoundError:
        print("[ERROR] coingeckco_symbol.csv not found..")

    # if already exist
    if isinstance(df, pd.DataFrame):
        symbol_table = df.set_index('symbol')
        try:
            return symbol_table.loc[symbol]['id']
        except KeyError:
            print('symbol value({symbol}) is not found execute search.. '.format(
                symbol=symbol))
            symbol_id = _search_coingecko_symbol_id(symbol)

            df = pd.concat(
                [df, pd.DataFrame([{'symbol': symbol, 'id': symbol_id}])])
            df.to_csv(os.path.join(
                CSV_REPO_PATH, GECKO_CSV_PATH), index=False)
            return symbol_id

            # if not exist
    else:
        symbol_id = _search_coingecko_symbol_id(symbol)
        row = {'symbol': symbol, 'id': symbol_id}
        df = pd.DataFrame([row])
        df.to_csv(os.path.join(
            CSV_REPO_PATH, GECKO_CSV_PATH), index=False)
        return symbol_id
