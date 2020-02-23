import pyodbc, re
import pandas as pd

"TODO: make variable DB_path unchangeable"


class Position:
    __DB_path = None

    def __init__(self, DB_path):
        """
        :param DB_path: string, path to the database on the computer
        """
        self.__DB_path = DB_path
        self.connStr = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb)};' + r'DBQ={};'.format(DB_path) + r'PWD=xamok')
        self.cursor = self.connStr.cursor()


    def __do_commit(self):
        self.connStr.commit()


    def __close_connection(self):
        self.cursor.close()
        self.connStr.close()


    def current_wire_pos(self):
        """
        Func find the first position from table JOBS
        JOBS - table in KOMAX database that contains wire cutting order

        :return dict: dict, contains values of the highest position from table JOBS
        """
        self.cursor.execute("SELECT TOP 1 ArticleID from jobs")
        try:
            ArticleID = self.cursor.fetchall()[0][0]
        except:
            ArticleID = None

        dict = self.__create_dict(ArticleID)
        return dict


    def __create_dict(self, ArticleID):
        '''
        Func creates dict from values of one row from JOBS table

        :param ArticleID: int, id that used to define row number for creating dict
        :return: dict: dict, contains values of 1 row
        '''

        columns = ['amount', 'harness', 'wire_number', 'wire_square', 'wire_color', 'wire_length',
                   'wire_seal_1', 'wire_cut_length_1', 'wire_terminal_1',
                   'wire_seal_2', 'wire_cut_length_2', 'wire_terminal_2'] #name of columns from wire chart
        dict = {}
        #ArticleID is OK
        if ArticleID is not None:
            self.cursor.execute("SELECT TotalPieces FROM jobs WHERE ArticleID={}".format(ArticleID))
            amount = [self.cursor.fetchall()[0][0]]
            # amount = 64
            self.cursor.execute("SELECT ArticleGroupName FROM articlegroups "
                                "WHERE ArticleGroupID=(SELECT ArticleGroupID FROM articles WHERE ArticleID={})".format(
                ArticleID))
            harness_name = [self.cursor.fetchall()[0][0]]
            wire_number = self.__wire_number(ArticleID)
            square, color = self.__square_and_color(ArticleID)
            self.cursor.execute("SELECT [(1-2) WireLength], "
                                "(SELECT SealKey FROM seals WHERE SealID = [(1) SealID]), "
                                "[(1) StrippingLength], "
                                "(SELECT TerminalKey FROM terminals WHERE TerminalID = [(1) TerminalID]), "
                                "(SELECT SealKey FROM seals WHERE SealID = [(2) SealID]), "
                                "[(2) StrippingLength], "
                                "(SELECT TerminalKey FROM terminals WHERE TerminalID = [(2) TerminalID]) FROM leadsets "
                                "WHERE ArticleID={}".format(ArticleID))
            row = amount + harness_name + [wire_number] + [square] + [color] + list(self.cursor.fetchall()[0])
            for i in range(len(columns)):
                dict[columns[i]] = row[i]
        else:
            for i in range(len(columns)):
                dict[columns[i]] = 0
        return dict


    def __square_and_color(self, ArticleID):
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


    def __wire_number(self, ArticleID):
        '''
        Func finds wire_number in leadsets table from Komax database
        :param ArticleID: int
        :return: wire_number: string or None
        '''
        self.cursor.execute("SELECT [(1-2) InkjetActions] FROM leadsets WHERE ArticleID={}".format(ArticleID))
        inkjet_str = self.cursor.fetchall()[0][0]
        if inkjet_str != 'None':
            m = re.search('<TextNormal>(.*?)<\/TextNormal>', inkjet_str)
            return m.group(1)
        else:
            return None #if wire does not have his own wire_number


    def stop_komax(self, status):
        '''
        Func is using for stopping Komax work. Deletes all positions from JOBS
        :param status: string
        :return: df: dataframe with all positions except the first that were in table JOBS; or None; or string: 'deleted'
        '''

        self.cursor.execute("SELECT TOP 1 ArticleID from jobs")
        try:
            ArticleID = self.cursor.fetchall()[0][0]
        except:
            ArticleID = None


        if ArticleID is not None:
            df = self.__create_dataframe()
            self.cursor.execute("DELETE FROM jobs WHERE JobPos<>(SELECT TOP 1 JobPos from jobs)")
            self.__do_commit()
            if status == 'delete and save':
                return df
            elif status == 'delete': #if we do not need in records from JOBS
                return 'deleted'
        else:
            return None



    def __create_dataframe(self):
        '''
        Func creates dataframe from all records except the first (table JOBS)
        :return: df: dataframe
        '''
        jobs_df = pd.read_sql("SELECT ArticleID, TotalPieces from jobs", self.connStr)
        r, c = jobs_df.shape
        ds = []
        for i in range(1, r): #all position except the first
            ArticleID = jobs_df['ArticleID'][i]
            dict = self.__create_dict(ArticleID)
            ds.append(dict)

        #ds = [d1, d2, ...]
        common_dict = {}
        for k in ds[1].keys():  # merge multiple dicts with same key
            common_dict[k] = [common_dict[k] for common_dict in ds]

        df = pd.DataFrame(common_dict)
        return df





#DB_path = 'C:\Komax\Data\TopWin\DatabaseServer.mdb'
#DB_path = 'C:\Komax\Data\TopWin\DatabaseServer(1).mdb'


bd_connector = Position('C:\Komax\Data\TopWin\DatabaseServer.mdb')
dict = bd_connector.current_wire_pos()
print(dict)

#result = bd_connector.stop_komax('delete and save')
#print(result)
