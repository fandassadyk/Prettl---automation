import time
import json
import numpy as np
import pandas as pd
import requests
from urllib.parse import parse_qsl, urljoin, urlparse


import highest_pos as conn1
import insert as conn2


DB_PATH = 'C:\Komax\Data\TopWin\DatabaseServer.mdb'
KOMAX_ID = 'AAAAA'
PRODUCTION = True
if PRODUCTION:
    URL = 'https://komax.prettl.ru/api/v1/komax/'
else:
    URL = 'http://localhost:8000/api/v1/komax/'
KOMAX_NUMBER = 5


db_get = conn1.Position(DB_PATH)
db_insert = conn2.Insert(DB_PATH, None)


def to_normal_int(number):
    type_number = type(number)
    if type_number is np.int64 or type_number is np.int32 or type_number is np.int16 or type_number is np.int8 or type_number is np.int_:
        return int(number)

def to_normal(value):
    type_value = type(value)
    if type_value is str or type_value is int or type_value is float or type_value is bool:
        return value
    elif type_value is np.bool_:
        return bool(value)
    else:
        return to_normal_int(value)

def make_headers(url):
    return {
        'Host': urlparse(url).hostname,
        'User-Agent':  ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36'),
        'Accept': ('text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,'
                   '*/*;q=0.8'),
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
        'Accept-Encoding': 'gzip, deflate, sdch'}



client = requests.session()



#komax_df = pd.DataFrame()
komax_df = pd.read_excel('C:\Komax\Data\TopWin\komax_df_{}.xlsx'.format(KOMAX_NUMBER)) # на комаксе должно быть так
# komax_df = pd.read_excel('C:\\Users\sadyk\PycharmProjects\\forKomax\modules\PrettlNKKomax\komax_modules\komax_df_{}.xlsx'.format(KOMAX_NUMBER))
# komax_df = pd.read_excel('C:\\Users\sadyk\PycharmProjects\\forKomax\modules\PrettlNKKomaxMaster\komax_modules\komax_df_{}.xlsx'.format(KOMAX_NUMBER))

print(komax_df.columns)


idx_to_send = None

# start connection


while True:
    # print('komax_df_5 empty: ', komax_df.empty)

    r = client.get(URL, params={'komax-number': KOMAX_NUMBER}, headers=make_headers(URL))
    print(r.text)

    CSRF_TOKEN = client.cookies['csrftoken']

    status = 1



    if not komax_df.empty:
        if idx_to_send is None:
            idx_to_send = 0

        # to_send = komax_df.iloc[idx_to_send, :].to_dict()
        # for key, value in to_send.items():
        #    to_send[key] = to_normal(value)


        to_send = db_get.current_wire_pos()
        print('current_pos', to_send)
    else:
        to_send = 1

    print(idx_to_send)
    params={
        'status': status,
        'position': json.dumps(to_send),
        'csrfmiddlewaretoken': CSRF_TOKEN,
    }

    req_info_position = client.post(URL, data=params, headers=make_headers(URL))
    print('to_send', to_send)
    print(req_info_position.text)
    Html_file = open("filename.html", "w")
    Html_file.write(req_info_position.text)
    Html_file.close()
    data = req_info_position.json()
    print(data)

    if len(data):
        status = data.get('status', None)
        text = data.get('text', None)
        task = data.get('task', None)
        print(status, text, task)
        if status is not None and status == 2:
            if text is not None and text == 'Requested':
                client.get(URL, params={'komax-number': KOMAX_NUMBER}, headers=make_headers(URL))
                CSRF_TOKEN = client.cookies['csrftoken']


                # находим актуальную position
                # возвращаю все из komax_df, что ниже актуального position
                # удаляю все из БД


                position = db_get.current_wire_pos()
                result = db_get.stop_komax('delete')
                print('deleted')

                #находим позицию, которая сейчас режется
                if position != 1 and komax_df.empty is False:
                    print('position', position)

                    '''idx = komax_df[(komax_df['harness'] == str(position['harness'])) &
                                   (komax_df['wire_number'] == str(position['wire_number'])) &
                                   (komax_df['komax'] == str(position['komax'])) &
                                   (komax_df['wire_color'] == str(position['wire_color'])) &
                                   (komax_df['wire_square'] == str(position['wire_square'])) &
                                   (komax_df['wire_length'] == str(position['wire_length']))
                                   ]['id']  # все что ниже этого индекса, нужно отправить'''

                    idx = komax_df[(komax_df['amount'].astype(str) == str(position['amount'])) &
                                   (komax_df['harness'].astype(str) == str(position['harness'])) &
                                   (komax_df['wire_number'].astype(str) == str(position['wire_number'])) &
                                   (komax_df['komax'].astype(str) == str(position['komax'])) &
                                   (komax_df['wire_color'].astype(str) == str(position['wire_color'])) &
                                   (komax_df['wire_square'].astype(str) == str(position['wire_square'])) &
                                   (komax_df['wire_length'].astype(str) == str(position['wire_length']))
                                   ]['id']  # все что ниже этого индекса, нужно отправить


                    if idx.empty:
                        to_send = komax_df.to_dict() #отправляем весь датафрейм
                        print(komax_df)
                    else:
                        to_send = komax_df[komax_df['id'].astype(int) > idx.array[0].astype(int)].to_dict()
                        print(komax_df[komax_df['id'].astype(int) > idx.array[0].astype(int)])

                else:  # если пустой
                    to_send = {}
                    print(to_send)

                #вырезаем все из датафрейма, что ниже этой позиции
                # dict_df_to_send = komax_df.iloc[idx_to_send:, :].to_dict() if idx_to_send is not None else komax_df.to_dict()


                params = {
                     'status': status,
                     'text': text,
                     'task': json.dumps(to_send),
                     'csrfmiddlewaretoken': CSRF_TOKEN,
                 }
                req_task_send = client.post(URL, data=params, headers=make_headers(URL))
                komax_df = pd.DataFrame()
                idx_to_send = None



            elif task is not None:
                komax_df = pd.DataFrame(task)
                #komax_df.index = pd.Index(komax_df['id'])
                print('Received task {}'.format(komax_df.shape[0]))

                idx_to_send = 0

                # save komax_df to excel
                komax_df.to_excel('komax_df_{}.xlsx'.format(KOMAX_NUMBER))
                print(komax_df)

                komax_df.index = pd.Index(range(komax_df.shape[0]))
                print(komax_df)

                result = db_get.stop_komax('delete')
                # load task
                # db_insert.wire_chart_df = pd.DataFrame.from_dict(komax_df)
                db_insert.wire_chart_df = komax_df
                db_insert.load_task()

                # clean Excel file
                clean_df = komax_df[0:0]
                clean_df.to_excel('komax_df_{}.xlsx'.format(KOMAX_NUMBER))


    if idx_to_send is not None:
        idx_to_send += 1
    if not komax_df.empty and idx_to_send == (komax_df.shape[0] - 1):
        idx_to_send = None
    time.sleep(3)
