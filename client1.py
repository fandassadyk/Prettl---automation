import time
import os
from json import JSONDecodeError, loads, dumps
import numpy as np
import pandas as pd
from requests import Session
import sys
import openpyxl as xl
from openpyxl import load_workbook
from urllib.parse import urlparse
import atexit
import logging
from logging.handlers import RotatingFileHandler
from highest_pos import Position
from insert import Insert


def get_file_logger(logging_level):
    logger = logging.getLogger('{}'.format(__file__.split('.')[0]))
    logger.setLevel(logging_level)

    fh = RotatingFileHandler(
        filename="{}.log".format(__file__.split('.')[0]),
        maxBytes=5*1024*1024,
        backupCount=1,
    )
    fh.setLevel(logging_level)

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)

    return logger

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

class DatabaseConnection:

    def __init__(self, path_to_database: str):
        self.db_get = Position(path_to_database)
        self.db_insert = Insert(path_to_database, None)

    def get_position(self) -> dict:
        return self.db_get.current_wire_pos()

    def delete_all_positions(self):
        self.db_get.stop_komax('delete')

    def insert_df(self, dataframe: pd.DataFrame):
        self.db_insert.wire_chart_df = dataframe
        self.db_get.stop_komax('delete')
        self.db_insert.load_task()

class KomaxClient(Session):
    # Unused var komax-number, should delete?
    KOMAX_NUMBER = 3
    KOMAX_ID = 'AAAAC'

    db_connection = None
    komax_df = pd.DataFrame()
    __idx_to_send = None
    __production = False

    def __init__(self, production=False, path_to_database='C:\Komax\Data\TopWin\DatabaseServer.mdb', path_to_dataframe='C:\Komax\Data\TopWin\komax_df.xlsx'):
        super().__init__()
        self.db_connection = DatabaseConnection(path_to_database)
        self.__production = production
        if self.__production:
            # self.__base_url = 'https://komax.prettl.ru/komax_api/v1/'
            self.__base_url = 'http://localhost:8000/komax_api/v1/'
        else:
            self.__base_url = 'http://localhost:8000/komax_api/v1/'

        if os.path.exists(path_to_dataframe):
            self.komax_df = pd.read_excel(path_to_dataframe)

        atexit.register(self.logout)
        if self.__production:
            logger.info('Initialized session in production {} with URL {}'.format(self.__production, self.__base_url))

    def __get_positions_to_send(self) -> dict:
        position = self.db_connection.get_position()
        self.db_connection.delete_all_positions()
        if position != 1 and not self.komax_df.empty:
            idx = self.komax_df[(self.komax_df['amount'].astype(str) == str(position['amount'])) &
                           (self.komax_df['harness'].astype(str) == str(position['harness'])) &
                           (self.komax_df['wire_number'].astype(str) == str(position['wire_number'])) &
                           (self.komax_df['komax'].astype(str) == str(position['komax'])) &
                           (self.komax_df['wire_color'].astype(str) == str(position['wire_color'])) &
                           (self.komax_df['wire_square'].astype(str) == str(position['wire_square'])) &
                           (self.komax_df['wire_length'].astype(str) == str(position['wire_length']))
                           ]['id']  # все что ниже этого индекса, нужно отправить
            if idx.empty:
                return self.komax_df.to_dict()
            else:
                return self.komax_df[self.komax_df['id'].astype(int) > idx.array[0].astype(int)].to_dict()
        else:
            return pd.DataFrame().to_dict()

    def __raise_server_error_warning(self, response):
        if self.__production:
            logger.info('Response status: {}'.format(response.status_code))
            logger.info('Response text: {}'.format(response.text))
            logger.warning('Server error occured.')
        else:
            raise Warning('Server error. Response status: {}, text: {}'.format(response.status_code, response.text))

    @classmethod
    def __get_headers(cls, url, method="GET"):
        return {
            'Host': urlparse(url).hostname,
            "method": method,
            'content-type': 'application/json',
            "accept-encoding": "gzip, deflate, br, sdch",
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 '
                          '(KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
            'accept': 'application/json',
            'dnt': '1',
            'cache-control': 'max-age=0',
            'pragma': 'no-cache',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'accept-language': 'en-US,en;q=0.9',
        }

    def login(self):
        headers = self.__get_headers(self.__base_url, method='POST')
        params = {
            'Komax': '{}'.format(self.KOMAX_ID)
        }
        response = self.post(self.__base_url + 'komax_login/', data=dumps(params), headers=headers)

        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            self.__raise_server_error_warning(response)

    def logout(self):
        response = self.delete(
            self.__base_url + 'komax_logout/',
            headers=self.__get_headers(self.__base_url, method="DELETE")
        )
        if response.status_code == 200:
            return True
        else:
            self.__raise_server_error_warning(response)

    def __get_position_info(self):
        """
        Get current position on komax from DB
        """
        if self.__production:
            logger.info('Getting position info')

        position = self.db_connection.get_position()
        print('bd_position', position)
        if not self.komax_df.empty:
            idx = self.komax_df[(self.komax_df['amount'].astype(str) == str(position['amount'])) &
                                (self.komax_df['harness'].astype(str) == str(position['harness'])) &
                                (self.komax_df['wire_number'].astype(str) == str(position['wire_number'])) &
                                (self.komax_df['komax'].astype(str) == str(position['komax'])) &
                                (self.komax_df['wire_color'].astype(str) == str(position['wire_color'])) &
                                (self.komax_df['wire_square'].astype(str) == str(position['wire_square'])) &
                                (self.komax_df['wire_length'].astype(str) == str(position['wire_length']))
                                ]['id']

            if idx.empty:
                to_send = 1
            else:
                # отправляем строку из датафрейма
                to_send = self.komax_df[self.komax_df['id'].astype(int) == idx.array[0].astype(int)].to_dict()
        else:
            to_send = 1
        print('to_send', to_send)
        # if not self.komax_df.empty:
        #     if self.__idx_to_send is None:
        #         self.__idx_to_send = 0
        #
        #     to_send = self.komax_df.iloc[self.__idx_to_send, :].to_dict()
        #     for key, value in to_send.items():
        #         to_send[key] = to_normal(value)
        # else:
        #     to_send = 1
        if self.__production:
            logger.info('Position got to send: \n {}'.format(to_send))
        return to_send

    def __send_position_info(self):
        to_send = self.__get_position_info()

        params = {
            'position': dumps(to_send),
        }
        response = self.post(
            self.__base_url + 'position/',
            data=dumps(params),
            headers=self.__get_headers(self.__base_url, method="POST")
        )

        if response.status_code == 201:
            return True
        elif response.status_code == 403:
            return self.login()
        elif response.status_code == 400:
            if self.__production:
                logger.info('Bad position info has been sent')
            else:
                print('bad position info has been sent')
            return False
        else:
            self.__raise_server_error_warning(response)
            return False

    def __get_old_tasks(self):
        """
        Retrieve(and delete) all current tasks in DB

        Should return dict
        """
        if self.__production:
            logger.info('Getting old positions in DB')

        tasks = self.__get_positions_to_send()

        # if self.__idx_to_send:
        #     tasks = self.komax_df.iloc[self.__idx_to_send:, :].to_dict()
        # else:
        #     tasks = self.komax_df.to_dict()


        if self.__production:
            logger.info('Got {} positions in DB: \n {}'.format(len(tasks), tasks))
        return tasks

    def __load_new_tasks(self, tasks=None):
        """
        Load new tasks in DB
        """
        if tasks is not None and type(tasks) is dict and len(tasks):
            self.komax_df = pd.DataFrame(tasks)

            if self.__production:
                logger.info(self.komax_df)

            self.komax_df.to_excel('C:\Komax\Data\TopWin\komax_df.xlsx')
            self.komax_df.index = pd.Index(range(self.komax_df.shape[0]))
            self.db_connection.insert_df(self.komax_df)

            # self.komax_df = pd.DataFrame(tasks)
            # self.__idx_to_send = 0
            if self.__production:
                logger.info('Received task {}'.format(self.komax_df.shape[0]))
            else:
                print('Received task {}'.format(self.komax_df.shape[0]))
        else:
            if self.__production:
                logger.warning('No tasks passed to load_new_tasks view')
            raise Warning("No tasks passed to load_new_tasks view")

    def __get_and_send_old_tasks(self):
        tasks = self.__get_old_tasks()
        params = {
            'text': 'Requested',
            'task': dumps(tasks),
        }
        response = self.put(
            self.__base_url + 'komax_task_personal/',
            data=dumps(params),
            headers=self.__get_headers(self.__base_url, method="PUT")
        )

        if response.status_code == 200:
            self.komax_df = pd.DataFrame()
            # self.__idx_to_send = None
        elif response.status_code == 400:
            pass
        else:
            self.__raise_server_error_warning(response)

    def check_and_load_new_tasks(self):
        response = self.get(self.__base_url + 'komax_task_personal/', headers=self.__get_headers(self.__base_url))

        if self.__production:
            logger.info('Response from requesting new tasks: \n {}'.format(response.text))

        if response.status_code == 200:
            try:
                response_data = response.json()
            except JSONDecodeError as error:
                if self.__production:
                    logger.warning('JSONDecodeError occured: \n {}'.format(error))
                raise error

            text = response_data.get('text', None)
            task = response_data.get('task', None)
            if text:
                if text == 'Requested':
                    if self.__production:
                        logger.info('Master requested old tasks from DB')
                    self.__get_and_send_old_tasks()
                    return 'Sent old tasks'
                elif text == 'Received' and task:
                    if self.__production:
                        logger.info('Master loading new tasks to DB')
                    self.__load_new_tasks(task)
                    return 'Loaded new tasks for {} positions'.format(self.komax_df.shape[0])
        elif response.status_code == 404:
            return 'No new tasks'
        else:
            self.__raise_server_error_warning(response)

    def work(self):
        login_successfull = self.login()
        if self.__production:
            logger.info('Login is {}'.format(login_successfull))
        else:
            print(login_successfull)
        if not login_successfull:
            if self.__production:
                logger.warning('Authentication failed. Check Komax id')
            raise Warning('Authentication failed. Check Komax id')
        while True:
            position_sent = self.__send_position_info()
            if not position_sent:
                if self.__production:
                    logger.warning('Position not sent. Check Komax id and connection with Database')
                raise Warning('Position not sent. Check Komax id and connection with Database')
            time.sleep(2)
            new_tasks_info = self.check_and_load_new_tasks()
            if self.__production:
                logger.info('Info about new tasks: \n {}'.format(new_tasks_info))
            else:
                print(new_tasks_info)
            time.sleep(2)

            # if test:
            #     if self.__idx_to_send is not None:
            #         self.__idx_to_send += 1
            #     if not self.komax_df.empty and self.__idx_to_send == (self.komax_df.shape[0] - 1):
            #         self.__idx_to_send = None


if __name__ == '__main__':
    # logger setup
    logger = get_file_logger(logging.INFO)

    # main work
    client = KomaxClient(production=True)
    client.work()

