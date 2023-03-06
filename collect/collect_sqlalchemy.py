import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://heoheon:Qhdks5743812!@heoheon.synology.me:3306/mystkdb')


query = """ select * from basecode """
basecode = pd.read_sql_query(query, con=engine.connect())


print(basecode)  