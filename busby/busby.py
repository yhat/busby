import sys
import pandas as pd
import json
from websocket import create_connection


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

    ws = create_connection("ws://"+url)
    ws.send(json.dumps({'username': username, 'apikey': apikey}))

    results = []
    
    for index, row in df.iterrows():
        data = row.to_dict()
        ws.send(json.dumps(data))
        result =  ws.recv()
        results.append(json.loads(result))

    ws.close()
    df = pd.DataFrame(results)
    
    try:
        with open(output_file):
            headers = False
    except IOError:
        headers = True

    with open(output_file, 'ab') as f:
        df.to_csv(f, index=False, header=headers)
