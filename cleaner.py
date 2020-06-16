import pyodbc

DB_path = 'C:\Komax\Data\TopWin\DatabaseServer.mdb'
connStr = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb)};' + r'DBQ={};'.format(DB_path) + r'PWD=xamok')
cursor = connStr.cursor()


def clean_leadsets_articles():
    cursor.execute("DELETE FROM articles")
    cursor.execute("DELETE FROM leadsets")
    cursor.execute("DELETE FROM articlegroups WHERE SuperGroupID=5")
    connStr.commit()

def clean_jobs():
    cursor.execute("DELETE FROM jobs")
    connStr.commit()

clean_leadsets_articles()
clean_jobs()

cursor.close()
connStr.close()

print('Successfully cleared!')
print('Press ENTER')
begin = input()
