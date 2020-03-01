import websocket
import _thread
import time
import json
import numpy as np

import highest_pos as conn1
import insert as conn2

#DB_PATH = 'DatabaseServer.mdb'
DB_PATH = 'C:\Komax\Data\TopWin\DatabaseServer.mdb'
KOMAX_ID = 'AAAAA'

db_get = conn1.Position(DB_PATH)

def on_message(ws, message):
    received_data = json.loads(message)
    db_insert = conn2.Insert(DB_PATH, received_data['task'])

    result = None
    if 'status' in received_data and received_data['status'] == 2:
        if 'task' in received_data:
            result = db_get.stop_komax('delete') #return 'deleted'
        elif received_data['text'] == 'Requested':
            result = db_get.stop_komax('delete and save') #return dataframe
        db_insert.load_task()

    data_to_send = {
        'status': 2,
        'text': 'Requested',
        'task': result
    }
    return data_to_send



def on_error(ws, error):
    print(error)

def on_close(ws):
    print("Closed")

def on_open(ws): #что происходит при открытии шлюза
    def run(*args):
        while True:
            type, data = db_get.current_wire_pos()
            data_to_send = {
                'status': 1,
                'type': type,
                'text': data
            }
            json_data = json.dumps(data_to_send)
            ws.send(json_data)
            time.sleep(1)

        #ws.close()
        #print("thread_terminating")

    _thread.start_new_thread(run, ())


# websocket.enableTrace(True)


ws = websocket.WebSocketApp(
    "wss://komaxsite.herokuapp.com/komax_app/komax_manager/",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open,
)

ws.run_forever()

