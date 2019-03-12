#!/usr/bin/env python

import os
import sys
import ssl
import json
import urllib2

PUBSUB_SERVICE_URL = 'https://localhost:20000/publish'

ROOT_CERTIFICATE_LOCATION = '../docker/bootstrap/root.crt'


if __name__ == '__main__':
    ctx = ssl.create_default_context(cafile=ROOT_CERTIFICATE_LOCATION)

    if len(sys.argv) > 1 and sys.argv[1] == '--farmers':
        message = {
            'message_type': 'FarmersOrderForecast',
            'scn_slug': 'farmers_slug',
            'payload': {
                'supplier_id': 7,
                'warehouse_id': 444
            }
        }
    elif len(sys.argv) > 1 and sys.argv[1] == '--stocks':
        message = {
          'message_type': 'SonaeStockLevels',
          'scn_slug': 'sonae_slug',
          'warehouse_id': '444',
          'supplier_id': '7',
          'stock_levels_date': '2018-03-06T00:00:00.000Z',
          'payload': {
            'stock_levels': [
              {
                'sku': 'Test_Product_1',
                'stock': 30
              },
              {
                'sku': 'Test_Product_2',
                'stock': 6
              },
              {
                'sku': 'Test_Product_3',
                'stock': 74
              },
              {
                'sku': 'Test_Product_4',
                'stock': 33
              },
              {
                'sku': 'Test_Product_5',
                'stock': 42
              }
            ]
          }
        }
    elif len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
        with open(sys.argv[1], 'r') as f:
            message = json.loads(f.read())
    else:
        message = {
            'message_type': 'SonaeOrderForecast',
            'scn_slug': 'sonae_slug',
            'payload': {
                'supplier_id': 7,
                'warehouse_id': 444
            }
        }

    headers = {
        'Content-Type': 'application/json'
    }

    request = urllib2.Request(PUBSUB_SERVICE_URL, json.dumps(message), headers)

    response = urllib2.urlopen(request, context=ctx)

    print(response.code)
    print('\n'.join(response.readlines()))
