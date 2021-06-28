from datetime import datetime
import requests
import json
import hmac
import hashlib
import base64
from urllib.parse import urlencode
import pandas as pd
import time

with open('pairData.json') as json_file:
    pairData = json.load(json_file)
finalCsv = []
for pair in pairData:
    #Get all Accounts of the Current User
    AccessKeyId = ''
    SecretKey = ''
    timestamp = str(datetime.utcnow().isoformat())[0:19]
    params = urlencode({'AccessKeyId': AccessKeyId,
                        'SignatureMethod': 'HmacSHA256',
                        'SignatureVersion': '2',
                        'Timestamp': timestamp,
                        'symbol':pair
                    })
    method = 'GET'
    endpoint = '/v1/order/matchresults'
    base_uri = 'api-aws.huobi.pro'
    pre_signed_text = method + '\n' + base_uri + '\n' + endpoint + '\n' + params
    # print(pre_signed_text)
    hash_code = hmac.new(SecretKey.encode(), pre_signed_text.encode(), hashlib.sha256).digest()
    signature = urlencode({'Signature': base64.b64encode(hash_code).decode()})
    url = 'https://' + base_uri + endpoint + '?' + params + '&' + signature
    # print(url)
    response = requests.request(method, url)
    # print(response.json())
    dtest = pd.DataFrame(data=response.json()['data'])
    for row in response.json()['data']:
        if row['fee-currency'] == pairData[pair]['base']:
            mydir = "BUY"
        else:
            mydir = "SELL"
        lastrow = {
            'TRADE ID':row['trade-id'],
            'ORDER ID':row['order-id'],
            'TAKER/MAKER':row['role'],
            'Ext Trade Ref':"{}_{}_{}".format(row['trade-id'],row['order-id'],row['role']),
            'Base':pairData[pair]['base'],
            'Amount':row['filled-amount'],
            'Term':pairData[pair]['term'],
            'Counter Amount':float(row['price'])*float(row['filled-amount']),
            'Dir':mydir,
            'Dealt Rate':row['price'],
            'Execution Time':str(datetime.fromtimestamp(round(float(row['created-at'])/1000))),
            'FEE':row['filled-fees'],
            'FEE CURRENCY':row['fee-currency']
        }
        finalCsv.append(lastrow)
        # print(row)
    # print(finalCsv)
    
df = pd.DataFrame(data=finalCsv)
df.to_csv(r'./csv return/Huobi-Extracted-Trades-'+str(int(round(time.time() * 1000)))+'.csv', index = False)
print('Extract CSV finished -> '+'Huobi-Extracted-Trades-'+str(int(round(time.time() * 1000)))+'.csv')

