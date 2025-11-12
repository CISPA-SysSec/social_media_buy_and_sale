import re

import requests
import shutil


def download_image(request_url, save_path):
    try:
        response = requests.get(request_url, stream=True)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        if response.status_code == 200:
            with open('{}'.format(save_path), 'wb') as f:
                response.raw.decode_content = True
                shutil.copyfileobj(response.raw, f)
    except Exception as ex:
        print("Exception occurred in downloading request to image {}".format(ex))


def get_crypto_address_from_line(_line_item):
    try:
        if not _line_item:
            return None
        _splitter = _line_item.split(" ")
        for _s in _splitter:
            is_btc = is_valid_bitcoin_address(_s)
            if is_btc:
                return _s
            is_eth = is_valid_ethereum_address((_s))
            if is_eth:
                return _s
    except:
        pass
    return None


# https://github.com/SLakhani1/Phone-Number-and-Email-Address-Extractor/blob/master/extractor.py
def get_phone_number_from_line(line):
    result = []
    PhnNumCheck = re.compile(r'''
        (\d{3}|\(\d{3}\))    #first three-digit
        (\s|-|\.)?           #separator or space
        (\d{3}|\(\d{3}\))    #second three-digits
        (\s|-|\.)?           #separator or space
        (\d{4}|\(\d{4}\))    #last four-digits
    ''', re.VERBOSE)
    for num in PhnNumCheck.findall(line):
        result.append(num[0] + num[2] + num[4])
    if result:
        return result[0]
    else:
        return None


# This is the most effective solution
def get_email(line):
    result = []
    emailCheck = re.compile(r'''
        [a-zA-Z0-9.+_-]+  #username
        @                 #@ character
        [a-zA-Z0-9.+_-]+  #domain name
        \.                #first .(dot) 
        [a-zA-Z]          #domain type like-- com
        \.?               #second .(dot) for domains like co.in
        [a-zA-Z]*         #second part of domain type like 'in' in  co.in 
    ''', re.VERBOSE)

    for emails in emailCheck.findall(line):
        result.append(emails)
    if len(result) > 0:
        return result[0]
    else:
        return None


def get_url_from_line(line):
    _found_url = []
    try:
        _splitter = line.split(" ")
        for l in _splitter:
            urls = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', l)
            _found_url = _found_url + list(urls)
    except:
        pass
    return list(set(_found_url))


def is_valid_bitcoin_address(_str):
    try:
        regex = "^(bc1|[13])[a-km-zA-HJ-NP-Z1-9]{25,34}$"
        p = re.compile(regex)
        if str is None:
            return False
        return re.search(p, _str)
    except:
        pass
    return None


def is_valid_ethereum_address(_str):
    try:
        regex = r'^(0x)?[0-9a-fA-F]{40}$'
        return re.match(regex, _str)
    except:
        pass
    return None
