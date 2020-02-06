import pyodbc
import datetime
import pandas as pd
import time
import os
import sys

start_time = time.time()


def newest(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getmtime)


def contain_word_in(string, *args):
    if args is None or string is None:
        return False
    for list in args:
        for word in list:
            if word is None:
                return False


            if type(word) is str and type(string) is str:
                if word in string:
                    continue
                else:
                    return False
            elif type(word) is int and type(string) is int:
                if word == string:
                    continue
                else:
                    return False
            else:
                return False

    return True


def get_index(data_frame, *args):
    rows, cols = data_frame.shape

    for i in range(cols):
        for index, rows in data_frame.iterrows():
            if not data_frame[i][index] is None:
                if contain_word_in(data_frame[i][index], args):
                    return [i, index]

    return -1, 0


def replace(df, r, *column_stripping_length):
    for arg in column_stripping_length:
        for i in range(r):
            chs = df[arg][i]
            if type(chs) == str:
                chs = chs.replace(',', '.')
                df[arg][i] = float(chs)


def replace_in_square(square):
    if type(square) != int and type(square) != float:
        square = square.replace(',', '.')
        square = float(square)
    square = float('{0:g}'.format(square))
    return square


def color(string, color_dict):
    global color1, color2
    color_end = ''
    if 'Кч' in string:
        if string != 'Кч':
            if string[0] == 'К':
                color1 = 'BROWN'
                color2 = color_dict[string[len(string)-1]]
            if string[len(string)-1] == 'ч':
                color1 = color_dict[string[0]]
                color2 = 'BROWN'
            color_end = color1 + ' ' + color2
        else:
            color_end = color_dict[string]
    elif len(string) == 1:
        color_end = color_dict[string]
    elif len(string) > 1:
        color1 = color_dict[string[0]]
        color2 = color_dict[string[1]]
        color_end = color1 + ' ' + color2
    return color_end


def inkjet_actions(wireLength, wire_numb, left_distance1, right_distance1, left_distance2, right_distance2):
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
        print('Длина неверно введена в КРП или вообще не введена!')
    return inkjet_actions


def terminal_strlength_pulllength(sets, connStr, cursor):
    global TerminalID1, TerminalID2, TerminalID3, pull_off_length1, pull_off_length2, pull_off_length3
    i = 0
    for set in sets:
        i += 1
        print('SET:', set)
        terminal, stripping_length = set[0], set[1]

        pull_off_length, TerminalID = 0, 0
        if terminal != '' and terminal is not None and terminal == terminal:
            df_terminal = pd.read_sql("SELECT TerminalKey FROM terminals WHERE TerminalKey = '{}'".format(terminal),
                                           connStr)
            if df_terminal.empty:
                if stripping_length != '' and stripping_length is not None and stripping_length == stripping_length:
                    pull_off_length = int(stripping_length) - 2
            else:
                pull_off_length = 0
                cursor.execute("SELECT TerminalID FROM terminals WHERE TerminalKey = '{}'".format(terminal))
                TerminalID = cursor.fetchall()[0][0]
                cursor.execute("SELECT StrippingLength FROM terminals WHERE TerminalKey = '{}'".format(terminal))
                stripping_length = cursor.fetchall()[0][0]
        else:
            print('')
            # случай, когда нет наконечника
            if stripping_length != '' and stripping_length is not None and stripping_length == stripping_length:
                pull_off_length = int(stripping_length) - 2

        if i == 1:
            TerminalID1, stripping_length1, pull_off_length1 = TerminalID, stripping_length, pull_off_length
            print('i=1:', TerminalID1, '|',  stripping_length1, '|', pull_off_length1)
        elif i == 2:
            TerminalID2, stripping_length2, pull_off_length2 = TerminalID, stripping_length, pull_off_length
            print('i=2:', TerminalID2, '|', stripping_length2, '|', pull_off_length2)
        else:
            TerminalID3, stripping_length3, pull_off_length3 = TerminalID, stripping_length, pull_off_length
            print('i=3:', TerminalID3, '|', stripping_length3, '|', pull_off_length3)


def fandas_create(ArticleID, ArticleGroupID, r, cols, df, tow_name, color_dict, cursor, connStr, created, modified):
    global TerminalID1, TerminalID2, TerminalID3, pull_off_length1, pull_off_length2, pull_off_length3
    global color2, color1
    for t in range(r):
        if df['harness'][t] == tow_name:
            ArticleID += 1
            article_key = '{:08}'.format(ArticleID)
            # это цвет
            string = df['wire_color'][t]
            color_end = color(string, color_dict)

            # меняем запятые на точки в сечении
            square = df['wire_square'][t]
            #square = replace_in_square(square)

            wire_key = str(square) + ' ' + color_end

            # проверка на дурака
            if '  ' in wire_key:
                wire_key = wire_key.replace('  ', ' ')


            cursor.execute("SELECT WireID FROM wires WHERE WireKey = '{}'".format(wire_key))
            wire_id_new = cursor.fetchall()[0][0]

            if square == 1.0:
                square = '1.00'
            cursor.execute("SELECT FontID FROM fonts WHERE FontKey = '{}'".format(str(square)))
            FontID12 = cursor.fetchall()[0][0]

            wireLength12 = int(df['wire_length'][t])

            if cols > 20: # вместо column_length23 нужно написать название столбца, которое придумает Герман 12.01.2020
                wireLength23 = df[column_length23][t]
                if wireLength23 is not None and wireLength23 == wireLength23:
                    wireLength23 = int(df[column_length23][t])
            else:
                wireLength23 = None


            terminal1 = df['wire_terminal_1'][t]
            terminal2 = df['wire_terminal_2'][t]
            stripping_length1 = df['wire_cut_length_1'][t]
            stripping_length2 = df['wire_cut_length_2'][t]

            if cols > 20: # вместо column_terminal3 нужно написать название столбца, которое придумает Герман 12.01.2020
                terminal3 = df[column_terminal3][t]
                stripping_length3 = df[column_stripping_length3][t]
            else:
                terminal3 = 0
                stripping_length3 = 0

            TerminalID1, TerminalID2, TerminalID3 = 0, 0, 0
            pull_off_length1, pull_off_length2, pull_off_length3 = 0, 0, 0

            if cols > 20:
                sets = [[terminal1, stripping_length1],
                        [terminal2, stripping_length2],
                        [terminal3, stripping_length3]]
            else:
                sets = [[terminal1, stripping_length1],
                        [terminal2, stripping_length2]]

            terminal_strlength_pulllength(sets, connStr, cursor)


            seal1, seal2 = df['wire_seal_1'][t], df['wire_seal_2'][t]

            if seal1 != '' and seal1 is not None and seal1 == seal1:
                df_seal1 = pd.read_sql("SELECT SealID FROM seals WHERE SealKey = '{}'".format(seal1),
                                           connStr)
                if df_seal1.empty:
                    seal_id_new1 = 0
                    terminal_id_new1 = 0
                else:
                    seal_id_new1 = df_seal1['SealID'][0]
            else:
                # случай если армировки нет в таблице
                seal_id_new1 = 0

            if seal2 != '' and seal2 is not None and seal2 == seal2:
                df_seal2 = pd.read_sql("SELECT SealID FROM seals WHERE SealKey = '{}'".format(seal2),
                                           connStr)
                if df_seal2.empty:
                    seal_id_new2 = 0
                    terminal_id_new2 = 0
                else:
                    seal_id_new2 = df_seal2['SealID'][0]
            else:
                # случай если армировки нет в таблице
                seal_id_new2 = 0


            WireID12 = wire_id_new
            SealID1 = seal_id_new1
            SealID2 = seal_id_new2
            WireID23 = WireID12
            FontID23 = FontID12

            name = str(df['wire_number'][t]) + ' (' + str(df['harness'][t]) + ')'
            #creator = '355.0281'
            creator = 'Fandas'
            private = '<Private/>'
            production_info = '<ProductionInfo><PostProduction><Auto></Auto><AutoInsertion></AutoInsertion></PostProduction><ProductionKind>Harness</ProductionKind></ProductionInfo>'
            lead_set_number, pieces, batchsize, trimmedtwisteropenendno = 1, 1, 1, 4
            wire_number = df['wire_number'][t]
            if cols > 20: # # вместо column_wire_number23 нужно написать название столбца, которое придумает Герман 12.01.2020
                wire_number23 = df[column_wire_number23][t]
            else:
                wire_number23 = None


            if wire_number != '' and wire_number is not None and wire_number == wire_number:
                left_distance1, right_distance1 = "%.2f" % 20, "%.2f" % 20
                square = str(square)
                if square == '0.35' or square == '0.5' or square == '0.75' or square == '1.0' or square == '1.00':
                    left_distance2, right_distance2 = "%.2f" % 75, "%.2f" % 75
                elif square == '1.5' or square == '2.5' or square == '4.0' or square == '6.0':
                    left_distance2, right_distance2 = "%.2f" % 85, "%.2f" % 85
                else:
                    left_distance2, right_distance2 = None, None
                wireLength = wireLength12
                wire_numb = wire_number
                print(wire_numb, wireLength)
                inkjet_actions_12 = inkjet_actions(wireLength, wire_numb, left_distance1, right_distance1,
                                                   left_distance2, right_distance2)
                if wireLength23 is not None:
                    wireLength = wireLength23
                    wire_numb = wire_number23
                    inkjet_actions_23 = inkjet_actions(wireLength, wire_numb, left_distance1, right_distance1,
                                                       left_distance2, right_distance2)
                else:
                    inkjet_actions_23 = None
            else:
                inkjet_actions_12, inkjet_actions_23 = None, None


            # проверка на спаривание
            if wireLength23 != '' and wireLength23 is not None and wireLength23 == wireLength23:
                name = str(df[column_wire_number][t]) + ' ' + str(df[column_wire_number23][t]) + ' (' + str(
                    df[column_tow_number][t]) + ')'
                cursor.execute("INSERT INTO articles (Created, Modified, ArticleID, "
                               "ArticleKey, Creator, Name, ArticleGroupID, Private, ProductionInfo) "
                               "VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}')".
                               format(created, modified, ArticleID, article_key, creator, name,
                                      ArticleGroupID,
                                      private, production_info))
                lead_set_typ = 1
                # вот здесь нужно добавить wirelength23, WireID23 = WireID12, TerminalID3, inkjetactions23, FontID23 = FontID12,
                cursor.execute(
                    "INSERT INTO leadsets (Created, Modified, ArticleID, LeadSetNumber, Pieces, BatchSize, "
                    "[(1-2) WireID], [(1-2) WireLength], [(1-2) FontID], [(1-2) InkjetActions], "
                    "[(2-3) WireID], [(2-3) WireLength], [(2-3) FontID], [(2-3) InkjetActions], "
                    "TrimmedTwisterOpenEndNo, LeadSetTyp, "
                    "[(1) StrippingLength], [(1) PulloffLength], [(1) SealID], [(1) TerminalID], "
                    "[(2) StrippingLength], [(2) PulloffLength], [(2) SealID], [(2) TerminalID], "
                    "[(3) StrippingLength], [(3) PulloffLength], [(3) SealID], [(3) TerminalID]) VALUES "
                    "('{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},"
                    "{}, {}, {}, {}, {}, {}, {})".
                        format(created, modified, ArticleID, lead_set_number, pieces, batchsize, WireID12,
                               wireLength12, FontID12, inkjet_actions_12, WireID23,
                               wireLength23, FontID23, inkjet_actions_23,
                               trimmedtwisteropenendno, lead_set_typ,
                               stripping_length1, pull_off_length1, 0, TerminalID1,
                               stripping_length2, pull_off_length2, 0, TerminalID2,
                               stripping_length3, pull_off_length3, 0, TerminalID3))
            # случай обычной загрузки по позициям
            else:
                cursor.execute("INSERT INTO articles (Created, Modified, ArticleID, "
                               "ArticleKey, Creator, Name, ArticleGroupID, Private, ProductionInfo) "
                               "VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}')".
                               format(created, modified, ArticleID, article_key, creator, name,
                                      ArticleGroupID,
                                      private, production_info))
                cursor.execute(
                    "INSERT INTO leadsets (Created, Modified, ArticleID, LeadSetNumber, Pieces, BatchSize, "
                    "[(1-2) WireID], [(1-2) WireLength], [(1-2) FontID], [(1-2) InkjetActions], TrimmedTwisterOpenEndNo, "
                    "[(1) StrippingLength], [(1) PulloffLength], [(1) SealID], [(1) TerminalID], "
                    "[(2) StrippingLength], [(2) PulloffLength], [(2) SealID], [(2) TerminalID]) VALUES "
                    "('{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {})".
                        format(created, modified, ArticleID, lead_set_number, pieces, batchsize,
                               WireID12,
                               wireLength12, FontID12, inkjet_actions_12, trimmedtwisteropenendno,
                               stripping_length1, pull_off_length1, SealID1, TerminalID1,
                               stripping_length2, pull_off_length2, SealID2, TerminalID2))





DB_path = 'C:\Komax\Data\TopWin\DatabaseServer.mdb'


path = 'C:\Komax\Data\TopWin\КРП'
file = newest(path)
df = pd.read_excel(file, index_col=0)
#df.index = pd.Index(range(df.shape[0]))
#df = df.dropna(axis=0, how='all')

def new_articles_leadsets(DB_path, df):
    # работа с БД access
    connStr = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb)};' + r'DBQ={};'.format(DB_path) + r'PWD=xamok')
    cursor = connStr.cursor()

    print(file)

    color_dict = {'Ч': 'BLACK', 'Б': 'WHITE', 'Г': 'BLUE', 'К': 'RED', 'Кч': 'BROWN', 'Р': 'PINK', 'С': 'GRAY', 'Ж': 'YELLOW', 'З': 'GREEN', 'О': 'ORANGE', 'Ф': 'PURPLE'}


    created = format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    modified = created

    r, cols = df.shape
    tows = []
    for i in range(r):
        tows.append(df['harness'][i])
    tow_dict = dict((i, tows.count(i)) for i in tows)

    # заменим запятые на точки в ЧС
    '''if cols > 20:
        replace(df, r, column_stripping_length1, column_stripping_length2, column_stripping_length3) # вместо column_stripping_length3 нужно написать название столбца, которое придумает Герман 12.01.2020
    else:
        replace(df, r, column_stripping_length1, column_stripping_length2)'''

    cursor.execute("SELECT MAX (ArticleGroupID) from articlegroups")
    ArticleGroupID = cursor.fetchall()[0][0]

    # для nameofgroups нужно что-то придумать
    for i in tow_dict:
        the_same = 0
        tow_name = i
        tow_number = tow_dict[i]
        super_group_id = 5

        df_tow_id = pd.read_sql("SELECT ArticleGroupID FROM articlegroups WHERE ArticleGroupName = '{}'".format(tow_name), connStr)

        if df_tow_id.empty:
            ArticleGroupID = ArticleGroupID + 1

            cursor.execute("INSERT INTO articlegroups (Created, Modified, ArticleGroupID, ArticleGroupName, "
                           "SuperGroupID, NumberOfUsers) VALUES ('{}', '{}', {}, '{}', {}, {})".
                           format(created, modified, ArticleGroupID, tow_name, super_group_id, tow_number))

            cursor.execute("SELECT NumberOfGroups FROM articlegroups WHERE SuperGroupID = 1")
            number_of_groups = cursor.fetchall()[0][0]
            number_of_groups += 1
            cursor.execute("UPDATE articlegroups SET NumberOfGroups = {} WHERE SuperGroupID = 1".format(number_of_groups))

            # найдем ArticleID, с которого загонять
            cursor.execute("SELECT MAX(ArticleID) FROM leadsets")
            ArticleID = cursor.fetchall()[0][0]
            if ArticleID == None:
                ArticleID = 1

            fandas_create(ArticleID, ArticleGroupID, r, cols, df, tow_name, color_dict, cursor, connStr, created, modified)
            print('Артикулы успешно загрузились!')



        else:
            cursor.execute("SELECT NumberOfUsers FROM articlegroups WHERE ArticleGroupName = '{}'".format(tow_name))
            number_of_users = cursor.fetchall()[0][0]
            #print('Кол-во позиций существующего жгута:', number_of_users)

            article_group_id = df_tow_id['ArticleGroupID'][0]

            for k in range(r):
                if df['harness'][k] == tow_name:
                    # это цвет
                    string = df['wire_color'][k]
                    color_end = color(string, color_dict)

                    # меняем запятые на точки в сечении
                    square = df['wire_square'][k]
                    #square = replace_in_square(square)

                    wire_key = str(square) + ' ' + color_end

                    if '  ' in wire_key:
                        wire_key = wire_key.replace('  ', ' ')


                    cursor.execute("SELECT WireID FROM wires WHERE WireKey = '{}'".format(wire_key))
                    wire_id_new = cursor.fetchall()[0][0]

                    wireLength12 = int(df['wire_length'][k])
                    if cols > 20:
                        wireLength23 = df[column_length23][k] # вместо column_length23 нужно написать название столбца, которое придумает Герман 12.01.2020
                        if wireLength23 != '' and wireLength23 is not None and wireLength23 == wireLength23:
                            wireLength23 = int(wireLength23)
                    else:
                        wireLength23 = None

                    terminal1 = df['wire_terminal_1'][k]
                    terminal2 = df['wire_terminal_2'][k]
                    if cols > 20:
                        terminal3 = df[column_terminal3][k]
                    else:
                        terminal3 = None


                    seal1 = df['wire_seal_1'][k]
                    seal2 = df['wire_seal_2'][k]

                    if seal1 != '' and seal1 is not None and seal1 == seal1:
                        df_seal1 = pd.read_sql("SELECT SealID FROM seals WHERE SealKey = '{}'".format(seal1), connStr)
                        if df_seal1.empty:
                            seal_id_new1 = 0
                        else:
                            seal_id_new1 = df_seal1['SealID'][0]
                    else:
                        # случай если армировки нет в таблице
                        seal_id_new1 = 0

                    if seal2 != '' and seal2 is not None and seal2 == seal2:
                        df_seal2 = pd.read_sql("SELECT SealID FROM seals WHERE SealKey = '{}'".format(seal2), connStr)
                        if df_seal2.empty:
                            seal_id_new2 = 0
                        else:
                            seal_id_new2 = df_seal2['SealID'][0]
                    else:
                        # случай если армировки нет в таблице
                        seal_id_new2 = 0



                    terminal_id_new1, terminal_id_new2, terminal_id_new3 = 0, 0, 0

                    if df['wire_terminal_1'][k] != '':
                        df_terminal1 = pd.read_sql(
                            "SELECT TerminalKey FROM terminals WHERE TerminalKey = '{}'".format(terminal1), connStr)
                        if not df_terminal1.empty:
                            cursor.execute(
                                "SELECT TerminalID FROM terminals WHERE TerminalKey = '{}'".format(terminal1))
                            terminal_id_new1 = int(cursor.fetchall()[0][0])

                    if df['wire_terminal_2'][k] != '':
                        df_terminal2 = pd.read_sql(
                            "SELECT TerminalKey FROM terminals WHERE TerminalKey = '{}'".format(terminal2), connStr)
                        if not df_terminal2.empty:
                            cursor.execute(
                                "SELECT TerminalID FROM terminals WHERE TerminalKey = '{}'".format(terminal2))
                            terminal_id_new2 = int(cursor.fetchall()[0][0])

                    if cols > 20:
                        if df[column_terminal3][k] != '': # вместо column_terminal3 нужно написать название столбца, которое придумает Герман 12.01.2020
                            df_terminal3 = pd.read_sql(
                                "SELECT TerminalKey FROM terminals WHERE TerminalKey = '{}'".format(terminal3), connStr)
                            if not df_terminal3.empty:
                                cursor.execute(
                                    "SELECT TerminalID FROM terminals WHERE TerminalKey = '{}'".format(terminal3))
                                terminal_id_new3 = int(cursor.fetchall()[0][0])


                    # теперь проверим, совпадают ли наши жгуты
                    if wireLength23 != '' and wireLength23 is not None and wireLength23 == wireLength23: # проверка на спаривание
                        # нет проверок для уплотнителей, поскольку 1ый komax их не может печатать
                        cursor.execute("SELECT ArticleID FROM leadsets WHERE [(1-2) WireID] = {} AND "
                                       "[(1) TerminalID] = {} AND [(2) TerminalID] = {} AND [(3) TerminalID] = {} AND "
                                       "[(1-2) WireLength] = {} AND [(2-3) WireLength] = {}".
                                       format(wire_id_new, terminal_id_new1, terminal_id_new2, terminal_id_new3,
                                              wireLength12, wireLength23))
                        article_id = cursor.fetchall()
                    else:
                        cursor.execute("SELECT ArticleID FROM leadsets WHERE [(1-2) WireID] = {} AND "
                                       "[(1) TerminalID] = {} AND [(2) TerminalID] = {} AND "
                                       "[(1) SealID] = {} AND [(2) SealID] = {} AND"
                                       "[(1-2) WireLength] = {}".
                                       format(wire_id_new, terminal_id_new1, terminal_id_new2,
                                              seal_id_new1, seal_id_new2, wireLength12))
                        article_id = cursor.fetchall()

                    # если не нашли, переходим к следующему жгуту
                    if not article_id:
                        break
                    else:
                        the_same += 1


            if the_same != number_of_users or number_of_users != tow_number:
                # меняем кол-во проводов в жгуте
                cursor.execute("UPDATE articlegroups SET NumberOfUsers={} WHERE ArticleGroupID={}".format(tow_number, article_group_id))
                # находим ArticleID, которые нужно удалить в articles и leadsets
                cursor.execute("SELECT ArticleID FROM articles WHERE ArticleGroupID={}".format(article_group_id))
                delete_article_id = []
                for row in cursor:
                    delete_article_id.append(row[0])
                #print('ArticleID for deleting:', delete_article_id)
                for article in delete_article_id:
                    cursor.execute("DELETE FROM articles WHERE ArticleID={}".format(article))
                    cursor.execute("DELETE FROM leadsets WHERE ArticleID={}".format(article))

                if tow_number <= number_of_users:
                    ArticleID = min(delete_article_id) - 1
                    #print('Начальный ArticleID:', ArticleID)
                else:
                    cursor.execute("SELECT MAX(ArticleID) FROM leadsets")
                    ArticleID = (cursor.fetchall())[0][0]

                cursor.execute("SELECT ArticleGroupID FROM articlegroups WHERE ArticleGroupName='{}'".format(tow_name))
                ArticleGroupID = cursor.fetchall()[0][0]

                fandas_create(ArticleID, ArticleGroupID, r, cols, df, tow_name, color_dict, cursor, connStr, created, modified)
                print('Успешно заменились!')
            else:
                print('Замена не потребовалась!')


    connStr.commit()
    cursor.close()
    connStr.close()

    print("-— %s seconds —-" % (time.time() - start_time))


new_articles_leadsets(DB_path, df)
