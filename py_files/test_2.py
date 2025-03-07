import sqlite3 as sq
import pandas as pd

con = sq.connect('ETH.db')
cur = con.cursor()

m = len(list(cur.execute('select open from ohlc')))

all_ma_names = [f'ma{i}' for i in range(1,101)]

for k in all_ma_names:
    # # clear all ma_x tables before writing
    # z = f'drop table {k}_x'
    # cur.execute(z)
    # con.commit()

    local_ma_names = ['id primary key']
    for l in all_ma_names[all_ma_names.index(k)+1:len(all_ma_names)]:
        local_ma_names.append(f'{k}_{l}')
    
    s = ','.join(local_ma_names)
    ss = f'create table if not exists {k}_x({s})'
    cur.execute(ss)
    con.commit()

    n = len(local_ma_names)-1
    zero = [[i]+[0]*n for i in range(m)]
    q = ','.join(['?']*(n+1))
    # print(zero)
    sss = f'insert into {k}_x values({q})'
    cur.executemany(sss,zero)
    con.commit()