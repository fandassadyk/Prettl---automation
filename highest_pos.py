import pyodbc
import pandas as pd
from itertools import chain


class Position:
    def __init__(self, DB_path):
        self.DB_path = DB_path
        self.connStr = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb)};' + r'DBQ={};'.format(self.DB_path) + r'PWD=xamok')
        self.cursor = self.connStr.cursor()


    def current_wire_pos(self):
        """
        Func find the highest position from table JOBS
        JOBS - table in KOMAX database that contains wire cutting order

        :param DB_path: string, path to the database on the computer
        :return dict: dict, contains values of the highest position from table JOBS
        """

        #cursor.execute("SELECT TOP 1 ArticleID from articles")
        self.cursor.execute("SELECT TOP 1 ArticleID from jobs")
        try:
            ArticleID = self.cursor.fetchall()[0][0]
        except:
            ArticleID = None

        dict = self.create_dict(ArticleID)
        return dict



    def create_dict(self, ArticleID):
        '''
        Func creates dict from values of the first row from JOBS table

        :param ArticleID: int, id that used to define row number for creating dict
        :return: dict: dict, contains values of 1 row
        '''

        # columns = ['amount', 'harness', 'marking', 'wire_number', 'wire_square', 'wire_color', 'wire_length',
        #           'wire_seal_1', 'wire_cut_length_1', 'wire_terminal_1',
        #           'wire_seal_2', 'wire_cut_length_2', 'wire_terminal_2'] #name of columns from wire chart
        columns = ['amount', 'harness', 'wire_square', 'wire_color', 'wire_length',
                   'wire_seal_1', 'wire_cut_length_1', 'wire_terminal_1',
                   'wire_seal_2', 'wire_cut_length_2', 'wire_terminal_2']
        dict = {}
        #ArticleID is OK
        if ArticleID is not None:
            # cursor.execute("SELECT [(1-2) WireID], [(1-2) WireLength], [(1) StrippingLength], [(1) SealId], [(1) TerminalID],"
            #               "[(2) StrippingLength], [(2) SealId], [(2) TerminalID] from leadsets")
            self.cursor.execute("SELECT TotalPieces FROM jobs WHERE ArticleID={}".format(ArticleID))
            amount = [self.cursor.fetchall()[0][0]]
            # amount = 64
            self.cursor.execute("SELECT ArticleGroupName FROM articlegroups "
                                "WHERE ArticleGroupID=(SELECT ArticleGroupID FROM articles WHERE ArticleID={})".format(
                ArticleID))
            harness_name = [self.cursor.fetchall()[0][0]]
            square, color = self.square_and_color(ArticleID)
            self.cursor.execute("SELECT [(1-2) WireLength], [(1) SealID], [(1) StrippingLength], [(1) TerminalID], "
                                "[(2) SealID], [(2) StrippingLength], [(2) TerminalID] FROM leadsets "
                                "WHERE ArticleID={}".format(ArticleID))
            row = amount + harness_name + [square] + [color] + list(self.cursor.fetchall()[0])
            for i in range(len(columns)):
                dict[columns[i]] = row[i]
        else:
            for i in range(len(columns)):
                dict[columns[i]] = 0

        return dict


    def square_and_color(self, ArticleID):
        '''
        Func translates color of eng symbols from table WIRES (database) to russian for wire chart

        :param ArticleID: int, ArticlID of the highest position in table JOBS from database
        :return: square: float; color: string, translated color to russian
        '''

        self.cursor.execute("SELECT WireKey FROM wires "
                       "WHERE WireID=(SELECT [(1-2) WireID] FROM leadsets WHERE ArticleID={})".format(ArticleID))
        wire_key = self.cursor.fetchall()[0][0]

        color_dict = {'BLACK': 'Ч', 'WHITE': 'Б', 'BLUE': 'Г' , 'RED': 'К', 'BROWN': 'Кч', 'PINK': 'Р', 'GRAY': 'С', 'YELLOW': 'Ж', 'GREEN': 'З', 'ORANGE': 'О', 'PURPLE': 'Ф'}

        color = ''
        wire_key = wire_key.split()
        if len(wire_key) > 2:
            for i in range(1, len(wire_key)):
                color += color_dict[wire_key[i]]
        else:
            color = color_dict[wire_key[1]]
        return float(wire_key[0]), color


    def stop_komax(self, status):
        '''
        Func uses for stopping work Komax. Deletes all positions from JOBS
        :param status: string,
        :return: df: dataframe with all positions except the first that were in table JOBS; or None
        '''

        # TODO:
        #  add try-except for checking SELECT TOP 1 ArticleID from jobs

        df = self.create_dataframe()
        #self.cursor.execute("DELETE FROM jobs WHERE ArticleID<>(SELECT TOP 1 ArticleID from jobs)")
        self.cursor.execute("DELETE FROM jobs WHERE ArticleID<>(SELECT TOP 1 ArticleID from jobs)")
        if status == 'delete and save':
            return df
        elif status == 'delete': #if we do not need in rows from JOBS
            return None



    def create_dataframe(self):
        '''
        Func creates dataframe with rows (table JOBS) that we choose
        :return: df: dataframe
        '''
        jobs_df = pd.read_sql("SELECT ArticleID, TotalPieces from jobs", self.connStr)
        r, c = jobs_df.shape
        ds = []
        for i in range(1, r): #all position except the first
            ArticleID = jobs_df['ArticleID'][i]
            dict = self.create_dict(ArticleID)
            ds.append(dict)

        #ds = [d1, d2, ...]
        common_dict = {}
        for k in ds[1].keys():  # merge multiple dicts with same key
            common_dict[k] = [common_dict[k] for common_dict in ds]

        df = pd.DataFrame(common_dict)
        return df


    def delete_row(self):
        self.cursor.execute("DELETE DISTINCTROW FROM seals")


    def test(self):
        d1 = {'amount': 80, 'harness': '451041105099-90', 'wire_square': 1.5, 'wire_color': 'КчЖ', 'wire_length': 410,
         'wire_seal_1': 0, 'wire_cut_length_1': 16.0, 'wire_terminal_1': 0, 'wire_seal_2': 0, 'wire_cut_length_2': 3.8,
         'wire_terminal_2': 150}
        d2 = {'amount': 56, 'harness': '451041105099-78', 'wire_square': 1.0, 'wire_color': 'ЖГ', 'wire_length': 350, 'wire_seal_1': 0, 'wire_cut_length_1': 14.0, 'wire_terminal_1': 0, 'wire_seal_2': 0, 'wire_cut_length_2': 6.7, 'wire_terminal_2': 190}
        d3 = {'amount': 23, 'harness': '451041105099-84', 'wire_square': 2.0, 'wire_color': 'ЖГ', 'wire_length': 350, 'wire_seal_1': 0, 'wire_cut_length_1': 14.0, 'wire_terminal_1': 0, 'wire_seal_2': 0, 'wire_cut_length_2': 3.7, 'wire_terminal_2': 200}

        ds = [d1, d2, d3]
        d = {}
        for k in d1.keys():
            d[k] = [d[k] for d in ds]
        #print(d)
        df = pd.DataFrame(d)
        #print(df)




#DB_path = 'C:\Komax\Data\TopWin\DatabaseServer.mdb'
#DB_path = 'C:\Komax\Data\TopWin\DatabaseServerXP.mdb'
DB_path = 'C:\Komax\Data\TopWin\DatabaseServer(1).mdb'


x = Position(DB_path)
#dict = x.current_wire_pos()
#print(dict)
#result = x.stop_komax('delete and save')
#print(result)
#x.test()
x.delete_row()
