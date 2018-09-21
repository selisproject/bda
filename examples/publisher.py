#!/usr/bin/env python

import sys
import json
import urllib2

PUBSUB_SERVICE_URL = 'http://127.0.0.1:20000/publish'


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--farmers':
        message = {
            'message_type': 'FarmersOrderForecast',
            'scn_slug': 'farmers_slug',
            'payload': {
                'supplier_id': 7,
                'warehouse_id': 444
            }
        }
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

    response = urllib2.urlopen(request)

    print(response.code)
    print('\n'.join(response.readlines()))
