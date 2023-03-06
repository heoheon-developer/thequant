import pymysql

con = pymysql.connect(
   user = 'heoheon',
   passwd = 'Qhdks5743812!',
   host = 'heoheon.synology.me',
   db = 'mystkdb',
   charset = 'utf8'   
)

mycursor = con.cursor()

query = """
select * from basecode
"""

mycursor.execute(query)

data = mycursor.fetchall()
con.close()

print(data)