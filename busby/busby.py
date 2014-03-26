import pandas as pd
import json
import sys
from websocket import create_connection
from collections import OrderedDict
import os.path

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
    try:
        # Read csv file to DataFrame. No indices, each column is maintained
        # as a column (as opposed to pd.DataFrame.from_csv())
        df = pd.read_csv(file)
    except Exception as e:
        with open(output_file,'w') as f:
            f.write("[ERROR] Could not parse input csv file.\n")
            f.write("[ERROR] Parsing error:\n%s\n" % str(e))
        sys.exit(2)

    # Replicates methodology used in yhat's df_to_json()
    data = df.transpose().to_json(orient='values',date_format='iso')
    data = json.loads(data)
    data = OrderedDict(zip(df.columns,data))
    data = json.dumps(data)

    ws = create_connection("ws://" + url)
    ws.send(json.dumps({'username': username, 'apikey': apikey}))

    ws.send(data)

    result = ws.recv()
    ws.close()
    try:
        result = json.loads(result)
        result_df = pd.DataFrame(result['result'])
        # If mutlifile batch job then concat existing file with current df
        # since to_csv() will overwrite existing file (not append)
        if os.path.isfile(output_file):
            old_df = pd.read_csv(output_file)
            result_df = pd.concat((old_df,result_df))
        result_df.to_csv(output_file,index=False)
    except KeyError: # No 'result' field in response
        with open(output_file,'w') as f:
            f.write("[ERROR] No 'result' field in response! Are you passing valid JSON?\n")
            f.write("[ERROR] Websocket response:\n%s\n" % json.dumps(result,indent=2))
    except ValueError: # Result could not be converted to dataframe
        with open(output_file,'w') as f:
            f.write("[ERROR] Result could not be converted to pandas DataFrame\n")
            f.write("[ERROR] Model result:\n%s\n" % json.dumps(result['result'],indent=2))
    except Exception: # General exception
        with open(output_file,'w') as f:
            f.write("[ERROR] Internal error: Could not process batch request\n")
