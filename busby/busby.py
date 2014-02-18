import pandas as pd
import json
import csv
from websocket import create_connection


def flatten(structure, key="", path="", flattened=None):
    if flattened is None:
        flattened = {}
    if type(structure) not in(dict, list):
        flattened[((path + "_") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "%d" % i, "".join(filter(None, [path, key])),
                    flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, "".join(filter(None, [path, key])),
                    flattened)
    return flattened


def value_list(x):
    # if dict put values in list
    if type(x) is dict:
        flat = flatten(x)
        return list(set(flat.values()))
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
        encoded = []
        for s in result:
            if isinstance(s, basestring):
                s = s.encode('utf8')
            encoded.append(s)
        results.append(encoded)

    ws.close()

    with open(output_file, 'ab') as f:
        w = csv.writer(f)
        w.writerows(results)
