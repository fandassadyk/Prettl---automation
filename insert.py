import pyodbc
import datetime
import pandas as pd
import os

"TODO: make variable DB_path unchangeable"
"TODO: update func which translates color"


class Insert():
    def __init__(self, DB_path, wire_chart_df):
        """
        :param DB_path: string, path to the database on the computer
        :param wire_chart_df: df
        """
        self.__DB_path = DB_path
        self.connStr = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb)};' + r'DBQ={};'.format(DB_path) + r'PWD=xamok')
        self.cursor = self.connStr.cursor()
        self.wire_chart_df = wire_chart_df
        self.created_time = format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.modified_time = self.created_time
        self.color_dict = {'Ч': 'BLACK', 'Б': 'WHITE', 'Г': 'BLUE', 'К': 'RED', 'Кч': 'BROWN', 'Р': 'PINK', 'С': 'GRAY', 'Ж': 'YELLOW', 'З': 'GREEN', 'О': 'ORANGE', 'Ф': 'PURPLE'}



    def __do_commit(self):
        self.connStr.commit()


    def __close_connection(self):
        self.cursor.close()
        self.connStr.close()


    def insert_into_jobs(self):
        """
        Func inserts records from wire chart to table JOBS in database
        JOBS - table in KOMAX database that contains wire cutting order

        :return:
        """
        self.wire_chart_df = self.__clear_dataframe()
        print(self.wire_chart_df)
        job_key = 1 #constant in DB, maybe need to add batchsize=50
        komax_number_dict = {2: '355.0281'} # must be changed
        self.cursor.execute("SELECT MAX (JobPos) from jobs")
        try:
            job_pos = self.cursor.fetchall()[0][0]
        except:
            job_pos = 0 #the number we start inserting from
        print(job_pos)

        r, c = self.wire_chart_df.shape
        for i in range(r):
            job_pos += 1
            amount = self.wire_chart_df['amount'][i]
            komax = komax_number_dict[self.wire_chart_df['komax'][i]]
            wire_square = self.wire_chart_df['wire_square'][i]
            wire_color = self.wire_chart_df['wire_color'][i]
            wire_length = self.wire_chart_df['wire_length'][i]
            wire_terminal_1 = self.wire_chart_df['wire_terminal_1'][i]
            wire_terminal_2 = self.wire_chart_df['wire_terminal_2'][i]


            wire_key = str(wire_square) + ' ' + self.__color(wire_color)
            self.cursor.execute("SELECT WireID FROM wires WHERE WireKey='{}'".format(wire_key))
            WireID = self.cursor.fetchall()[0][0]
            if wire_terminal_1 != 0:
                self.cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_terminal_1))
                wire_terminal_1 = self.cursor.fetchall()[0][0]
            if wire_terminal_2 != 0:
                self.cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_terminal_2))
                wire_terminal_2 = self.cursor.fetchall()[0][0]


            #code below is better, but brokes if one of the terminals doesn't exist
            '''self.cursor.execute("SELECT WireID FROM wires WHERE WireKey='{}' "
                                "UNION ALL "
                                "SELECT TerminalID FROM terminals WHERE TerminalKey='{}' "
                                "UNION ALL "
                                "SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_key, wire_terminal_1, wire_terminal_2))
            list = self.cursor.fetchall()#list with values WireID, 1 TerminalID, 2 TerminalID
            print(list)
            self.cursor.execute("SELECT ArticleID from leadsets WHERE [(1-2) WireID]={} AND [(1-2) WireLength]={} AND "
                                "[(1) TerminalID]={} AND [(2) TerminalID]={}".format(list[0][0], wire_length, list[1][0], list[2][0]))'''
            self.cursor.execute("SELECT ArticleID from leadsets WHERE [(1-2) WireID]={} AND [(1-2) WireLength]={} AND "
                                "[(1) TerminalID]={} AND [(2) TerminalID]={}".format(WireID, wire_length,
                                                                                     wire_terminal_1, wire_terminal_2))
            ArticleID = self.cursor.fetchall()[0][0] #but what if there is no such ArticleID???

            self.cursor.execute("INSERT INTO jobs (Created, Modified, JobKey, JobPos, "
                           "Creator, ArticleID, TotalPieces) "
                           "VALUES ('{created}', '{modified}', '{jobkey}', {jobpos}, '{creator}', {article_id}, {total_pieces})".
                           format(created=self.created_time, modified=self.modified_time, jobkey=job_key, jobpos=job_pos,
                                  creator=komax, article_id=ArticleID, total_pieces=amount))
        self.__do_commit()


    def __color(self, wire_color):
        '''
        Func translates color of rus symbols from table WIRES (database) to english for wire chart

        :param wire_color: string
        :return: wire_color_eng: string, translated color to english
        '''
        color1, color2 = None, None
        if len(wire_color) == 3:
            if wire_color[0] == 'К':
                color1 = 'BROWN'
                color2 = self.color_dict[wire_color[2]]
            elif wire_color[1] == 'К':
                color1 = self.color_dict[wire_color[0]]
                color2 = 'BROWN'
            wire_color_eng = color1 + ' ' + color2
        elif len(wire_color) == 2:
            if wire_color == 'Кч':
                wire_color_eng = 'BROWN'
            else:
                color1 = self.color_dict[wire_color[0]]
                color2 = self.color_dict[wire_color[1]]
                wire_color_eng = color1 + ' ' + color2
        else:  # can be just len = 1
            wire_color_eng = self.color_dict[wire_color]

        return wire_color_eng

        '''wire_color_eng = ''
        color1, color2 = None, None
        if 'Кч' in wire_color:
            if wire_color != 'Кч':
                if wire_color[0] == 'К':
                    color1 = 'BROWN'
                    color2 = self.color_dict[wire_color[len(wire_color) - 1]]
                if wire_color[len(wire_color) - 1] == 'ч':
                    color1 = self.color_dict[wire_color[0]]
                    color2 = 'BROWN'
                wire_color_eng = color1 + ' ' + color2
            else:
                wire_color_eng = self.color_dict[wire_color]
        elif len(wire_color) == 1:
            wire_color_eng = self.color_dict[wire_color]
        elif len(wire_color) > 1:
            color1 = self.color_dict[wire_color[0]]
            color2 = self.color_dict[wire_color[1]]
            wire_color_eng = color1 + ' ' + color2
        return wire_color_eng'''


    def __clear_dataframe(self):
        '''
        Func replaces nan values and non existing in database terminals with 0
        :return: self.wire_chart_df, clear dataframe
        '''
        #self.wire_chart_df = self.wire_chart_df.fillna(0)
        terminals_df = pd.read_sql("SELECT TerminalKey FROM terminals", self.connStr)
        terminals = terminals_df['TerminalKey'].to_numpy() #terminals which exist in database
        r, c = self.wire_chart_df.shape
        for i in range(r):
            if self.wire_chart_df['wire_terminal_1'][i] not in terminals:
                self.wire_chart_df.loc[i, 'wire_terminal_1'] = 0
            if self.wire_chart_df['wire_terminal_2'][i] not in terminals:
                self.wire_chart_df.loc[i, 'wire_terminal_2'] = 0
        return self.wire_chart_df


def newest(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getmtime)

path = 'C:\Komax\Data\TopWin\КРП'
file = newest(path)
df = pd.read_excel(file, index_col=0)

bd_connecter = Insert('C:\Komax\Data\TopWin\DatabaseServer.mdb', df)
bd_connecter.insert_into_jobs()

