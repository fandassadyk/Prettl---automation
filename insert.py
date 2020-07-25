import pyodbc
import datetime
import pandas as pd
import os, re
import numpy as np
import time

"TODO: what should be article_key?"
"TODO: try to insert into inkjetactions '' value"


class Insert():
    def __init__(self, DB_path, data):
        """
        :param DB_path: string, path to the database on the computer
        :param wire_chart_df: df
        """
        self.__DB_path = DB_path
        self.connStr = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb)};' + r'DBQ={};'.format(DB_path) + r'PWD=xamok')
        self.cursor = self.connStr.cursor()
        if data is None:
            self.wire_chart_df = None
        else:
            self.wire_chart_df = pd.DataFrame.from_dict(data)
            print('df exists')
        print(self.wire_chart_df)
        #self.wire_chart_df = pd.DataFrame.from_dict(data)
        self.created_time = format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.modified_time = self.created_time
        self.color_dict = {'Ч': 'BLACK', 'Б': 'WHITE', 'Г': 'BLUE', 'К': 'RED', 'Кч': 'BROWN', 'Р': 'PINK', 'С': 'GRAY', 'Ж': 'YELLOW', 'З': 'GREEN', 'О': 'ORANGE', 'Ф': 'PURPLE'}
        self.komax_number_dict = {1: '355.0281', 2: '355.0661', 3: '355.1990', 4: '355.2273', 5: '355.2205'}
        self.dds_files_path = 'C:\Komax\Data\WPCS-Data\{}'
        # KomaxColors - файл с основными цветами проводов, которые используются в Преттле
        colors_doc_path = 'C:\Komax\Data\TopWin\KomaxColors.xlsx'
        if os.path.exists(colors_doc_path):
            self.colors = pd.read_excel(colors_doc_path, index_col=0)['Обозначение'].to_numpy()
        else:
            self.colors = ('Б', 'БГ', 'БК', 'Г', 'ГБ', 'ГК', 'Ж', 'ЖГ', 'З', 'ЗК', 'К', 'КБ', 'Кч', 'О', 'ОБ', 'Р', 'РГ', 'С', 'Ф', 'Ч')



    def __do_commit(self):
        self.connStr.commit()


    def __close_connection(self):
        self.cursor.close()
        self.connStr.close()


    def load_task(self):
        print('im alive')
        self.wire_chart_df = self.__clear_dataframe(self.wire_chart_df)
        print('cleared')
        self.__update_harnesses()
        self.__insert_into_jobs()
        print('OKEY')
        #return 'uploaded'


    def __update_harnesses(self):
        """
        Func checks harnesses in database and update them if they are incorrect
        :return:
        """
        #take slice of necessary data for comparing

        krp_df = self.wire_chart_df[['harness', 'wire_number', 'wire_square', 'wire_color', 'wire_length',
                                     'wire_seal_1', 'wire_terminal_1', 'wire_seal_2', 'wire_terminal_2']]
        krp_df = self.__stick_color_and_square(krp_df)

        #create a tow names dict
        r, cols = self.wire_chart_df.shape
        tows = []
        for i in range(r):
            tows.append(self.wire_chart_df['harness'][i])
        self.tow_dict = dict((i, tows.count(i)) for i in tows)

        harness_df = pd.read_sql("SELECT ArticleGroupName FROM articlegroups", self.connStr)
        harnesses = harness_df['ArticleGroupName'].to_numpy()  # harnesses which exist in database

        #create a dataframe of wires for each tow and compare
        for tow in self.tow_dict:
            if tow in harnesses:
                print('Harness ', tow, ' is in DB')
                krp_harness_df = krp_df[krp_df['harness'] == tow]

                self.cursor.execute("SELECT ArticleID FROM articles WHERE ArticleGroupID="
                                    "(SELECT ArticleGroupID FROM articlegroups WHERE ArticleGroupName='{}')".format(tow))
                article_id_list = self.cursor.fetchall()

                #creating dataframe
                database_harness_df = self.__clear_dataframe(self.__create_dataframe(article_id_list))
                database_harness_df = self.__stick_color_and_square(database_harness_df[['harness', 'wire_number', 'wire_square',
                                                                                         'wire_color', 'wire_length',
                                         'wire_seal_1', 'wire_terminal_1', 'wire_seal_2', 'wire_terminal_2']])

                #sorting
                krp_harness_df = krp_harness_df.sort_values(by=['harness', 'wire_number', 'wire_length',
                                         'wire_seal_1', 'wire_terminal_1', 'wire_seal_2', 'wire_terminal_2', 'wire_key'])
                krp_harness_df.index = np.arange(len(krp_harness_df))

                database_harness_df = database_harness_df.sort_values(by=['harness', 'wire_number', 'wire_length',
                                                                'wire_seal_1', 'wire_terminal_1', 'wire_seal_2',
                                                                'wire_terminal_2', 'wire_key'])
                database_harness_df.index = np.arange(len(database_harness_df))

                #print(krp_harness_df.equals(database_harness_df))
                if krp_harness_df.equals(database_harness_df) is False:
                    self.__delete_harness_from_database(tow)
                    self.__insert_into_leadsets_and_articles(tow, self.tow_dict)
                    print('Succesfully inserted into DataBase!')
            else:
                self.__insert_into_leadsets_and_articles(tow, self.tow_dict)
                print('Succesfully inserted into DataBase!')
        self.__do_commit()


    def __delete_harness_from_database(self, tow):
        """
        Func deletes harness from all tables of database (tables: articlegroups, articles, leadsets)
        :param tow: str, harness name which we want to delete
        :return:
        """
        self.cursor.execute("SELECT ArticleID FROM articles WHERE ArticleGroupID="
                            "(SELECT ArticleGroupID FROM articlegroups WHERE ArticleGroupName='{}')".format(tow))
        delete_article_id = []  # список артиклей, которые нужно удалить
        for row in self.cursor:
            delete_article_id.append(row[0])

        print('delete articles ', delete_article_id)

        for article in delete_article_id:
            self.cursor.execute("DELETE FROM articles WHERE ArticleID={}".format(article))
            self.cursor.execute("DELETE FROM leadsets WHERE ArticleID={}".format(article))
        self.cursor.execute("DELETE FROM articlegroups WHERE ArticleGroupName='{}'".format(tow))



    def __insert_into_leadsets_and_articles(self, tow, tow_dict):
        """
        Func inserts records from wire chart to tables articles and leadsets (leadsets contains all characters of wire) in database
        :param tow: str, harness name which we want to delete
        :param tow_dict: dict, {tow: number of wires in the tow}
        :return:
        """

        #inserting into articlegroups
        self.cursor.execute("SELECT MAX (ArticleGroupID) FROM articlegroups")
        #####
        #what if there is no ane records?
        ArticleGroupID = self.cursor.fetchall()[0][0] + 1
        self.cursor.execute("INSERT INTO articlegroups (Created, Modified, ArticleGroupID, ArticleGroupName, "
                           "SuperGroupID, NumberOfUsers) VALUES "
                            "('{}', '{}', {}, '{}', {}, {})".format(self.created_time, self.modified_time, ArticleGroupID,
                                                                    tow, 5, tow_dict[tow]))

        # inserting into articles and leadsets
        self.cursor.execute("SELECT MAX (ArticleID) FROM articles")
        ArticleID = self.cursor.fetchall()[0][0]
        if ArticleID is None:
            ArticleID = 0

        #some constants
        private = '<Private/>'
        production_info = '<ProductionInfo><PostProduction><Auto></Auto><AutoInsertion></AutoInsertion></PostProduction><ProductionKind>Harness</ProductionKind></ProductionInfo>'

        inserting_df = self.wire_chart_df[self.wire_chart_df['harness']==tow]
        inserting_df.index = np.arange(len(inserting_df))
        r = inserting_df.shape[0]
        for_writing_DDS = ''
        for i in range(r):
            print('Row_number', i)
            amount = inserting_df['amount'][i]
            wire_number = inserting_df['wire_number'][i]
            wire_square = inserting_df['wire_square'][i]
            wire_color = inserting_df['wire_color'][i]
            wire_length = inserting_df['wire_length'][i]
            wire_terminal_1 = inserting_df['wire_terminal_1'][i]
            wire_terminal_2 = inserting_df['wire_terminal_2'][i]
            wire_seal_1 = inserting_df['wire_seal_1'][i]
            wire_seal_2 = inserting_df['wire_seal_2'][i]
            wire_cut_length_1 = inserting_df['wire_cut_length_1'][i]
            wire_cut_length_2 = inserting_df['wire_cut_length_2'][i]


            wire_key = str(wire_square) + ' ' + self.__color(wire_color)[0]
            self.cursor.execute("SELECT WireID FROM wires WHERE WireKey='{}'".format(wire_key))
            WireID = self.cursor.fetchall()[0][0]

            if wire_terminal_1 != 0:
                self.cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_terminal_1))
                wire_terminal_1 = self.cursor.fetchall()[0][0]
                wire_pull_off_length1=0
            else:
                wire_pull_off_length1=int(wire_cut_length_1) - 2
            if wire_terminal_2 != 0:
                self.cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_terminal_2))
                wire_terminal_2 = self.cursor.fetchall()[0][0]
                wire_pull_off_length2 = 0
            else:
                wire_pull_off_length2 = int(wire_cut_length_2) - 2
            if wire_seal_1 != 0:
                self.cursor.execute("SELECT SealID FROM seals WHERE SealKey='{}'".format(wire_seal_1))
                wire_seal_1 = self.cursor.fetchall()[0][0]
            if wire_seal_2 != 0:
                self.cursor.execute("SELECT SealID FROM seals WHERE SealKey='{}'".format(wire_seal_2))
                wire_seal_2 = self.cursor.fetchall()[0][0]

            #эта тупая проверка лишь на случай, когда БД хуево сделано. как БД приведем к стандартному виду, 2 строчки ниже нужно убрать
            if wire_square == 1.0:
                wire_square = '1.00'
            self.cursor.execute("SELECT FontID FROM fonts WHERE FontKey = '{}'".format(str(wire_square)))
            FontID = self.cursor.fetchall()[0][0]


            ArticleID += 1
            article_key = '{:08}'.format(ArticleID)
            komax = self.komax_number_dict[int(inserting_df['komax'][i])]
            #komax = 2
            name = str(inserting_df['wire_number'][i]) + ' (' + str(inserting_df['harness'][i]) + ')'
            self.cursor.execute("INSERT INTO articles (Created, Modified, ArticleID, "
                               "ArticleKey, Creator, Name, ArticleGroupID, Private, ProductionInfo) "
                               "VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}')".
                               format(self.created_time, self.modified_time, ArticleID, article_key, komax, name,
                                      ArticleGroupID,
                                      private, production_info))


            #define inkjetactions value
            if wire_number != '' and wire_number is not None and wire_number == wire_number:
                left_distance1, right_distance1 = "%.2f" % 20, "%.2f" % 20
                square = str(wire_square)
                if square == '0.35' or square == '0.5' or square == '0.75' or square == '1.0' or square == '1.00':
                    left_distance2, right_distance2 = "%.2f" % 75, "%.2f" % 75
                elif square == '1.5' or square == '2.5' or square == '4.0' or square == '6.0':
                    left_distance2, right_distance2 = "%.2f" % 85, "%.2f" % 85
                else:
                    left_distance2, right_distance2 = None, None
                wireLength = wire_length
                wire_numb = wire_number
                print(wire_numb, wireLength)
                inkjet_actions_12 = self.__inkjet_actions(wireLength, wire_numb, left_distance1, right_distance1,
                                                   left_distance2, right_distance2)
            else:
                inkjet_actions_12, inkjet_actions_23 = '', ''


            self.cursor.execute(
                "INSERT INTO leadsets (Created, Modified, ArticleID, LeadSetNumber, Pieces, BatchSize, "
                "[(1-2) WireID], [(1-2) WireLength], [(1-2) FontID], [(1-2) InkjetActions], TrimmedTwisterOpenEndNo, "
                "[(1) StrippingLength], [(1) PulloffLength], [(1) SealID], [(1) TerminalID], "
                "[(2) StrippingLength], [(2) PulloffLength], [(2) SealID], [(2) TerminalID]) VALUES "
                "('{}', '{}', {article_id}, {leadsetnumber}, {pieces}, {batchsize}, {wireid12}, {wirelength12}, "
                "{fontid12}, '{inkjetactions12}', {trimmedtwisteropenendno}, "
                "{stripping_length1}, {pull_off_length1}, {seal_id1}, {terminal_id1}, "
                "{stripping_length2}, {pull_off_length2}, {seal_id2}, {terminal_id2})".
                    format(self.created_time, self.modified_time, article_id=ArticleID, leadsetnumber =1,
                           pieces=amount, batchsize=1,
                           wireid12=WireID, wirelength12=wire_length, fontid12=FontID,
                           inkjetactions12=inkjet_actions_12, trimmedtwisteropenendno=4,
                           stripping_length1=wire_cut_length_1, pull_off_length1=wire_pull_off_length1,
                           seal_id1=wire_seal_1, terminal_id1=wire_terminal_1,
                           stripping_length2=wire_cut_length_2, pull_off_length2=wire_pull_off_length2,
                           seal_id2=wire_seal_2, terminal_id2=wire_terminal_2))

            for_writing_DDS += self.__add_1_ARTICLE(article_key, wire_key, wire_length, wire_terminal_1, wire_terminal_2, wire_seal_1, wire_seal_2)

            file = open(self.dds_files_path.format("Article.txt"), "w")
            print('Articles', for_writing_DDS)
            file.write(for_writing_DDS)
            file.close()

            #file = open("Article.txt", "w")
            #self.__write_ARTICLE_dds(file, article_key, wire_key, wire_length, wire_terminal_1, wire_terminal_2, wire_seal_1, wire_seal_2)
            #file.close()


    def __add_1_ARTICLE(self, article_key, wire_key, wire_length, wire_terminal_1, wire_terminal_2, wire_seal_1, wire_seal_2):

        if wire_terminal_1 == 0: wire_terminal_1 = ''
        if wire_terminal_2 == 0: wire_terminal_2 = ''
        if wire_seal_1 == 0: wire_seal_1 = ''
        if wire_seal_2 == 0: wire_seal_2 = ''

        if wire_terminal_1 == '' and wire_terminal_2 == '' and wire_seal_1 == '' and wire_seal_2 == '': #no seals and terminals
            return '\n[NewArticle]' \
                    '\nArticleKey = {article_key}' \
                    '\nNumberOfLeadSets = 1' \
                    '\n' \
                    '\n[NewLeadSet1]' \
                    '\nWireKey = {wire_key}' \
                    '\nWireLength = {wire_length}' \
                   '\n'.format(article_key=article_key, wire_key=wire_key, wire_length=wire_length)

        elif wire_terminal_1 == '' and wire_terminal_2 == '' and (wire_seal_1 == '' or wire_seal_2 == ''): #no terminals, but seals are used
            return '\n[NewArticle]' \
                    '\nArticleKey = {article_key}' \
                    '\nNumberOfLeadSets = 1' \
                    '\n' \
                    '\n[NewLeadSet1]' \
                    '\nWireKey = {wire_key}' \
                    '\nWireLength = {wire_length}' \
                   '\nSealKey = {wire_seal_1},{wire_seal_2}' \
                   '\n'.format(article_key=article_key, wire_key=wire_key, wire_length=wire_length,
                               wire_seal_1=wire_seal_1, wire_seal_2=wire_seal_2)

        elif (wire_terminal_1 != '' or wire_terminal_2 != '') and wire_seal_1 == '' and wire_seal_2 == '': #no seals, but terminals are used
            return '\n[NewArticle]' \
                    '\nArticleKey = {article_key}' \
                    '\nNumberOfLeadSets = 1' \
                    '\n' \
                    '\n[NewLeadSet1]' \
                    '\nWireKey = {wire_key}' \
                    '\nWireLength = {wire_length}' \
                   '\nTerminalKey = {wire_terminal_1},{wire_terminal_2}' \
                   '\n'.format(article_key=article_key, wire_key=wire_key, wire_length=wire_length,
                               wire_terminal_1=wire_terminal_1, wire_terminal_2=wire_terminal_2)

        else:
            return '\n[NewArticle]' \
                    '\nArticleKey = {article_key}' \
                    '\nNumberOfLeadSets = 1' \
                    '\n' \
                    '\n[NewLeadSet1]' \
                    '\nWireKey = {wire_key}' \
                    '\nWireLength = {wire_length}' \
                   '\nSealKey = {wire_seal_1},{wire_seal_2}' \
                   '\nTerminalKey = {wire_terminal_1},{wire_terminal_2}' \
                   '\n'.format(article_key=article_key, wire_key=wire_key, wire_length=wire_length,
                               wire_seal_1=wire_seal_1, wire_seal_2=wire_seal_2,
                               wire_terminal_1=wire_terminal_1, wire_terminal_2=wire_terminal_2)




    def  __write_ARTICLE_dds(self, file, article_key, wire_key, wire_length, wire_terminal_1, wire_terminal_2, wire_seal_1, wire_seal_2):
        """
        Func writes data to Article.dds file
        """
        if wire_terminal_1 == 0: wire_terminal_1 = ''
        if wire_terminal_2 == 0: wire_terminal_2 = ''
        if wire_seal_1 == 0: wire_seal_1 = ''
        if wire_seal_2 == 0: wire_seal_2 = ''


        file.write('[NewArticle] '
                   '\nArticleKey = {article_key} '
                   '\nNumberOfLeadSets = 1 '
                   '\n'
                   '\n[NewLeadSet1]'
                   '\nWireKey = {wire_key}'
                   '\nWireLength = {wire_length}'
                   '\nSealKey = {seal_key_1},{seal_key_2}'
                   '\nTerminalKey = {terminal_key_1},{terminal_key_2}'.
                   format(article_key=article_key, wire_key=wire_key, wire_length=wire_length,
                          seal_key_1=wire_seal_1, seal_key_2=wire_seal_2,
                          terminal_key_1=wire_terminal_1, terminal_key_2=wire_terminal_2))



# --------------------------------------------------------------------------------------------------------
# funcs below for inserting into leadsets

    def __inkjet_actions(self, wireLength, wire_numb, left_distance1, right_distance1, left_distance2, right_distance2):
        if type(wire_numb) is int:
            wire_numb = str(wire_numb)
        if wireLength >= 1000:
            repetition_distance = "%.2f" % 300
            inkjet_actions = '<PrintSet><Left><LeftDistance>' + left_distance1 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Left><LeftDistance>' + left_distance2 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Mid><RepetitionDistanceMin>' + repetition_distance + '</RepetitionDistanceMin><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Mid><Right><RightDistance>' + right_distance1 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right><Right><RightDistance>' + right_distance2 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right></PrintSet>'
        elif wireLength < 1000 and wireLength > 400:
            repetition_distance = "%.2f" % 100
            inkjet_actions = '<PrintSet><Left><LeftDistance>' + left_distance1 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Left><LeftDistance>' + left_distance2 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Mid><RepetitionDistanceMin>' + repetition_distance + '</RepetitionDistanceMin><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Mid><Right><RightDistance>' + right_distance1 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right><Right><RightDistance>' + right_distance2 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right></PrintSet>'
        elif wireLength <= 400 and wireLength > 200:
            inkjet_actions = '<PrintSet><Left><LeftDistance>' + left_distance1 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Left><LeftDistance>' + left_distance2 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Right><RightDistance>' + right_distance1 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right><Right><RightDistance>' + right_distance2 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right></PrintSet>'
        elif wireLength <= 200:
            inkjet_actions = '<PrintSet><Left><LeftDistance>' + left_distance1 + '</LeftDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Left><Right><RightDistance>' + right_distance1 + '</RightDistance><Item no="1"><Rotated>0</Rotated><Data no="1"><TextNormal>' + wire_numb + '</TextNormal></Data></Item></Right></PrintSet>'
        else:
            inkjet_actions = None
        return inkjet_actions



    def __stick_color_and_square(self, df):
        """
        Func creates new column 'wire_key' with sticking values of wire_square and wire_color and delete these two
        columns from dataframe
        :param df: df, dataframe that we want to change
        :return: df
        """
        df['wire_key'] = df.apply(lambda x: self.__wire_key(x['wire_square'], x['wire_color']), axis=1)
        return df.drop(['wire_square', 'wire_color'], axis=1)


    def __create_dataframe(self, article_id_list):
        """
        Func creates dataframe that contains information about wire: 'harness', 'wire_number', 'wire_square', 'wire_color', 'wire_length',
                   'wire_seal_1', 'wire_cut_length_1', 'wire_terminal_1',
                   'wire_seal_2', 'wire_cut_length_2', 'wire_terminal_2'
        :param article_id_list: int, articles that will be in dataframe
        :return: df
        """
        ds = []
        for i in range(len(article_id_list)):
            ds.append(self.__create_dict(article_id_list[i][0]))
        common_dict = {}
        for k in ds[1].keys():  # merge multiple dicts with same key
            common_dict[k] = [common_dict[k] for common_dict in ds]
        return pd.DataFrame(common_dict)


#--------------------------------------------------------------------------------------------------------------------------------------
    def __create_dict(self, ArticleID):
        '''
        Func creates dict from values of one row from LEADSETS table
        :param ArticleID: int, id that used to define row number for creating dict
        :return: dict: dict, contains values of 1 row
        '''

        columns = ['harness', 'wire_number', 'wire_square', 'wire_color', 'wire_length',
                   'wire_seal_1', 'wire_cut_length_1', 'wire_terminal_1',
                   'wire_seal_2', 'wire_cut_length_2', 'wire_terminal_2'] #name of columns from wire chart
        dict = {}
        #ArticleID is OK
        if ArticleID is not None:
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
            row = harness_name + [wire_number] + [square] + [color] + list(self.cursor.fetchall()[0])
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
#----------------------------------------------------------------------------------------------------------------------------------------------



    def __insert_into_jobs(self):
        """
        Func inserts records from wire chart to table JOBS in database
        JOBS - table in KOMAX database that contains wire cutting order
        :return:
        """

        self.cursor.execute("SELECT MAX (JobPos) from jobs")
        job_pos = self.cursor.fetchall()[0][0]
        if job_pos is None:
            job_pos = 0 #the number we start inserting from
        #print(job_pos)

        r, c = self.wire_chart_df.shape
        for_writing_DDS=''
        for i in range(r):
            job_key = self.wire_chart_df['id'][i]
            job_pos += 1
            amount = self.wire_chart_df['amount'][i]
            if amount < 50:
                batchsize = amount
            else:
                batchsize = 50
            komax = self.komax_number_dict[int(self.wire_chart_df['komax'][i])]
            wire_square = self.wire_chart_df['wire_square'][i]
            wire_color = self.wire_chart_df['wire_color'][i]
            wire_length = self.wire_chart_df['wire_length'][i]
            wire_terminal_1 = self.wire_chart_df['wire_terminal_1'][i]
            wire_terminal_2 = self.wire_chart_df['wire_terminal_2'][i]


            wire_key = str(wire_square) + ' ' + self.__color(wire_color)[0]
            self.cursor.execute("SELECT WireID FROM wires WHERE WireKey='{}'".format(wire_key))
            WireID = self.cursor.fetchall()[0][0]
            if wire_terminal_1 != 0:
                self.cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_terminal_1))
                wire_terminal_1 = self.cursor.fetchall()[0][0]
            if wire_terminal_2 != 0:
                self.cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey='{}'".format(wire_terminal_2))
                wire_terminal_2 = self.cursor.fetchall()[0][0]


            self.cursor.execute("SELECT ArticleID from leadsets WHERE [(1-2) WireID]={} AND [(1-2) WireLength]={} AND "
                                "[(1) TerminalID]={} AND [(2) TerminalID]={}".format(WireID, wire_length,
                                                                                     wire_terminal_1, wire_terminal_2))
            ArticleID = self.cursor.fetchall()[0][0]


            self.cursor.execute("INSERT INTO jobs (Created, Modified, JobKey, JobPos, "
                           "Creator, ArticleID, TotalPieces, BatchSize) "
                           "VALUES ('{created}', '{modified}', '{jobkey}', {jobpos}, '{creator}', {article_id}, "
                                "{total_pieces}, {batchsize})".
                           format(created=self.created_time, modified=self.modified_time, jobkey=job_key, jobpos=job_pos,
                                  creator=komax, article_id=ArticleID, total_pieces=amount, batchsize=batchsize))


            self.cursor.execute("SELECT ArticleKey from articles WHERE ArticleID={}".format(ArticleID))
            article_key = self.cursor.fetchall()[0][0] #нужно для записи в файл dds

            #file = open("Job.txt", "w")
            #self.__write_JOB_dds(file, job_key, article_key, batchsize, amount)
            #file.close()

            for_writing_DDS += self.__add_1_JOB(job_key, article_key, batchsize, amount)

            file = open(self.dds_files_path.format("Job.txt"), "w")
            print('Jobs:', for_writing_DDS)
            file.write(for_writing_DDS)
            file.close()



        self.__do_commit()



    def __add_1_JOB(self, job_key, article_key, batchsize, amount):
        return '\n[NewJob]' \
               '\nJob = {job_key}' \
               '\nArticleKey = {article_key}' \
               '\nTotalPieces = {total_pieces}' \
               '\nBatchSize = {batch}' \
               '\nName = "Leadset{job_key}"' \
               '\n'.format(job_key=job_key, article_key=article_key, total_pieces=amount,
                                                        batch=batchsize)




    def __write_JOB_dds(self, file, job_key, article_key, batchsize, amount):
        file.write('[NewJob] '
                   '\nJob = {job_key} '
                   '\nArticleKey = {article_key} '
                   '\nTotalPieces = {total_pieces} '
                   '\nBatchSize = {batch} '
                   '\nName = "Leadset{job_key}"'.format(job_key=job_key, article_key=article_key, total_pieces=amount,
                                                        batch=batchsize))



    def __color_translate(self, wire_color):
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


    def __color(self, wire_color):
        wire_color_eng = self.__color_translate(wire_color)
        # если нужного цвета нет в списке цветов Преттль, берем первый
        res_color = wire_color_eng if (wire_color in self.colors) else wire_color_eng.split()[0]
        return res_color, wire_color_eng
        # возвращаем оба варианта, потому что будет проще, если сравнить их в случае отличия печатать цвет провода в маркировке


    def __wire_key(self, wire_square, wire_color):
        return str(wire_square) + ' ' + self.__color(wire_color)[0]



    def __clear_dataframe(self, df):
        '''
        Func replaces nan values and non existing in database terminals/seals with 0
        :return: wire_chart_df, clear dataframe
        '''
        #self.wire_chart_df = self.wire_chart_df.fillna(0)
        terminals_df = pd.read_sql("SELECT TerminalKey FROM terminals", self.connStr)
        terminals = terminals_df['TerminalKey'].to_numpy() #terminals which exist in database
        seals_df = pd.read_sql("SELECT SealKey FROM seals", self.connStr)
        seals = seals_df['SealKey'].to_numpy()  #seals which exist in database
        r, c = df.shape
        for i in range(r):
            if df['wire_terminal_1'][i] not in terminals:
                df.loc[i, 'wire_terminal_1'] = 0
            if df['wire_terminal_2'][i] not in terminals:
                df.loc[i, 'wire_terminal_2'] = 0
            if df['wire_seal_1'][i] not in seals:
                df.loc[i, 'wire_seal_1'] = 0
            if df['wire_seal_2'][i] not in seals:
                df.loc[i, 'wire_seal_2'] = 0
        return df


def newest(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getmtime)



path = 'C:\Komax\Data\TopWin\КРП'
file = newest(path)
df = pd.read_excel(file, index_col=0)
data = df.to_dict()


#bd_connecter = Insert('C:\Komax\Data\TopWin\DatabaseServer.mdb', data)
#bd_connecter.load_task()
