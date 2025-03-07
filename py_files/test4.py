import sqlite3 as sq

con = sq.connect('ETH.db')
cur = con.cursor()

q_ma_names = cur.execute('PRAGMA table_info("ma_1m")')
ma_names = [i[1] for i in q_ma_names]

q_close = list(cur.execute('select close from ohlc'))
first = q_close[0][0]

close = [0]+[i[0] for i in q_close]
sum_all = []

cur.execute('drop table results_ma_x') # for clear table
cur.execute('create table if not exists results_ma_x(ma_x, profit, profit_p, clear_profit, clear_profit_p, commission, deal_count)')

for i in ma_names:
    q_name = f'PRAGMA table_info("{i}_x")'
    column_name = list(cur.execute(q_name))[1:]
    column_name = [i[1] for i in column_name]

    for k in column_name:
        q_column = f'select id,{k} from {i}_x where {k} != 0'
        column = list(cur.execute(q_column))
        points = [close[l[0]]*l[1] for l in column]

        sum_and_prcnt = sum([(abs(i)*2)*0.001 for i in points[1:-1]]) + abs((points[0]+points[-1])*0.001)
        
        a = sum(points)                 # profit
        b = (sum(points)/first)*100     # profit %
        c = len(points)                 # deal count
        d = sum(points)-sum_and_prcnt   # clear profit
        e = (d/first)*100               # clear profit %
        v = sum_and_prcnt               # commission

        sum_all.append([k,a,b,d,e,v,c])

q_insert = f'insert into results_ma_x values(?,?,?,?,?,?,?)'
cur.executemany('insert into results_ma_x values(?,?,?,?,?,?,?)',sum_all)
con.commit()