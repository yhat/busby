from __future__ import print_function

import csv
import json
import sys

from StringIO import StringIO
from time import sleep
from websocket import create_connection

def parse_rows(delimiter=',', quotechar='"'):
    filereader = csv.DictReader(sys.stdin,
                                delimiter=delimiter,
                                quotechar=quotechar)
    for row in filereader:
        row = {k: cast_element(v) for k, v in row.iteritems()}
        print(json.dumps(row))

def parse_chunk(delimiter=',', quotechar='"'):
    filereader = csv.DictReader(sys.stdin,
                                delimiter=delimiter,
                                quotechar=quotechar)
    chunk_dict = {field: [] for field in filereader.fieldnames}
    for row in filereader:
        for k,v in row.items():
            chunk_dict[k].append(cast_element(v))
    print(json.dumps(chunk_dict))

def cast_element(element):
    for t in int, float:
        try:
            return t(element)
        except:
            continue
    return element

def chunk_by_chunk(in_filename, out_filename, endpoint,
                   chunk_size=1000, delimiter=',', quotechar='"'):
    try:
        num_lines = float(sum(1 for line in open(in_filename)))
        filereader = csv.DictReader(open(in_filename, "r"),
                                    delimiter=delimiter,
                                    quotechar=quotechar)
        chunk_dict = {k: [] for k in filereader.fieldnames}
    except IOError:
        print("Error opening file")
        sys.exit(2)
    ws = create_connection(endpoint)
    filewriter = None
    try:
        i = 0
        for row in filereader:
            for k,v in row.items():
                chunk_dict[k].append(cast_element(v))
            i += 1
            if i >= chunk_size:
                data = json.dumps(chunk_dict)
                print("Sending chunk of size: %s" % i)
                chunk_dict = {k: [] for k in filereader.fieldnames}
                i = 0
                ws.send(data + "\n")
                response = json.loads(ws.recv())
                if "result" in response:
                    response = response["result"]
                    resp_header = response.keys()
                    if filewriter is None:
                        filewriter = csv.DictWriter(open(out_filename, "w"),
                                                    resp_header)
                        filewriter.writeheader()
                    response = map(lambda vals: dict(zip(resp_header, vals)),
                                   zip(*response.values()))
                    for resp in response:
                        filewriter.writerow(resp)
                else:
                    print("Yhat model returned error:")
                    print(json.dumps(response, indent=2))
                    sys.exit(2)
        if i > 0:
            data = json.dumps(chunk_dict)
            print("Sending chunk of size: %s" % i)
            ws.send(data + "\n")
            response = json.loads(ws.recv())
            if "result" in response:
                response = response["result"]
                resp_header = response.keys()
                if filewriter is None:
                    filewriter = csv.DictWriter(open(out_filename, "w"),
                                                resp_header)
                    filewriter.writeheader()
                response = map(lambda vals: dict(zip(resp_header, vals)),
                               zip(*response.values()))
                for resp in response:
                    filewriter.writerow(resp)
            else:
                print("Yhat model returned error:")
                print(json.dumps(response, indent=2))
                sys.exit(2)
    except Exception as e:
        print(e)
        sys.exit(2)
    

def row_by_row(in_filename, out_filename, endpoint,
               delimiter=',', quotechar='"'):
    perc_done = 0.0
    perc_step = 0.05
    try:
	num_lines = float(sum(1 for line in open(in_filename)))
        filereader = csv.DictReader(open(in_filename, "r"), 
                                    delimiter=delimiter,
                                    quotechar=quotechar)
    except IOError:
        print("Error opening file")
        sys.exit(2)
    filewriter = None
    ws = create_connection(endpoint)
    try:
        for i, row in enumerate(filereader):
            # attempt to typecast values
            row = {k: cast_element(v) for k, v in row.iteritems()}
            data = json.dumps(row)
            ws.send(data + "\n")
            response = json.loads(ws.recv())
            if "result" in response:
                response = response["result"]
                if filewriter is None:
                    filewriter = csv.DictWriter(open(out_filename, "w"),
                                                response.keys())
                    filewriter.writeheader()
                filewriter.writerow(response)
            else:
                print(json.dumps(response, indent=2)) 
                sys.exit(2)
            if i / num_lines > perc_done:
                print("%3.0f percent completed" % (perc_done * 100))
                while i / num_lines > perc_done:
                    perc_done += perc_step
        print("100 percent completed")
    except Exception as e:
        print(e)
        sys.exit(2)

