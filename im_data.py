
import pandas as pd
from sklearn import datasets
import numpy as np


from datetime import datetime
from sqlalchemy.types import NVARCHAR, Float, Integer, TEXT


import pymysql
from sqlalchemy import create_engine


# 把数据保存到数据库中

# data = pd.read_csv('./data_label1.csv')
def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: Float(precision=2, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: Integer()})
    return dtypedict

data1 = pd.read_csv('./data_label1.csv',dtype=object)
print(data1)

import time #导入时间库用来计算用时

start=time.clock()#计时开始
print(u'\n转换原始数据至0-1矩阵...')


# #建立连接，username替换为用户名，passwd替换为密码，test替换为数据库名，root表示用户名，123为数据库的密码

# 连接数据库

#读取数据库到本地
print(u'-----连接数据库读取数据-----')
user_ = input(u'请输入用户名:\n')
pswd_ = input(u'请输入密码:\n')
dbname_ = input(u'请输入数据库名:\n')
tablename_ = input(u'请输入你要创建的的数据库表名:\n')
conn = create_engine('mysql+pymysql://' + user_ +':'+ pswd_ + '@localhost:3306/'+dbname_,encoding='utf8')

###判断表是否存在

# #写入数据，table_name为表名，‘replace’表示如果同名表存在就替换掉
dtypedict = mapping_df_types(data1)

# print(dtypedict)
# dtypedict['antecedents'] = TEXT(length=500)
# print(rules.shape)
# print(rules.iloc[0,:])

# print(type(rules.iloc[0:2,:]))

data1 = data1.rename_axis('index').reset_index()


for i in range(0, data1.shape[0]):
    de = data1.iloc[i:i+1,:]
    # pd.io.sql.to_sql(rules, "table_name", conn, if_exists='replace')
    de.to_sql(name=tablename_, con=conn, if_exists='append', index=False, dtype=dtypedict)

# de = rules.iloc[70:,:]
# # pd.io.sql.to_sql(rules, "table_name", conn, if_exists='replace')
# de.to_sql(name=tablename_, con=conn, if_exists='append', index=False, dtype=dtypedict)
