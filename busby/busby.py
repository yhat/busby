import pandas as pd
import json
import csv
import collections
from websocket import create_connection


def flatten(d, parent_key=''):
    items = []
    for k, v in d.items():
        new_key = parent_key + '_' + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)


def value_list(x):
    # if dict put values in list
    if type(x) is dict:
        nested_dict = False
        nested_values = False
        for k, v in x.items():
            if type(v) is dict:
                nested_dict = True
            elif type(v) is list:
                nested_values = True
        if nested_dict:
            flat = flatten(x)
            return list(set(flat.values()))
        elif nested_values:
            flat = sorted({a for v in x.itervalues() for a in v})
            return flat
        else:
            return list(set(x.values()))
    # if string put in list
    elif isinstance(x, basestring):
        return [x]
    else:
        return x


def batch(url, file, output_file, username, apikey):
    """
    Parse a csv file and send through a websocket.

    Parameters
    ----------
    url: string
        the url to send the websocket to, exculding http://
    file: string
        the path to the file you would like to parse
    output_file: string
        the path to the file to save the results
    username: string
        username for authentication
    apikey: string
        apikey for authentication
    """
    df = pd.read_csv(file)

    ws = create_connection("ws://" + url)
    ws.send(json.dumps({'username': username, 'apikey': apikey}))

    results = []

    for index, row in df.iterrows():
        data = row.to_json()
        ws.send(data)
        result = ws.recv()
        result = json.loads(result)
        # remove yhat_id
        del result['yhat_id']
        # change, dict, string, etc into list
        result = value_list(result)
        results.append(result)

    ws.close()

    with open(output_file, 'ab') as f:
        w = csv.writer(f)
        w.writerows(results)
