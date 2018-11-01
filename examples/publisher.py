#!/usr/bin/env python

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
