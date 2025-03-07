import sqlite3 as sq

con = sq.connect('ETH.db')
cur = con.cursor()

s = ','.join(['id primary key']+[f'ma{i}' for i in range(1,101)])
ss = f'create table if not exists ma_1m({s})'
cur.execute(ss)
con.commit()

# for clear all
# cur.execute('delete from ma_1m')
# con.commit()

close = list(cur.execute('select close from ohlc'))
close = [i[0] for i in close]

n = len(close)

MA = [[] for i in range(n)]

for k in range(1,101):
    for i in range(n):
        if i+1<k:
            MA[i].append(0)
            continue
        if k == 1:
            MA[i].append(sum(close[0+i:k+i])/k)
        else:
            MA[i].append(sum(close[0+i-1:k+i-1])/k)


id = [[i] for i in range(n)]

data = list(zip(id,MA))
data = [i[0]+i[1] for i in data]

s = ['?' for i in range(101)]
s = ','.join(s)
ss = f'insert into ma_1m values({s})'
cur.executemany(ss, data)
con.commit()
