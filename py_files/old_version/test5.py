import sqlite3 as sq

total = []

for i in range(1,101):
    db = f'dbs/ma{i}.db'
    con = sq.connect(db)
    cur = con.cursor()

    for k in range(i+1,101):
        q = f'select clear_profit from ma{i}_ma{k}'
        table = list(cur.execute(q))[1:]

        z = [i[0] for i in table]
        win, lose = 0,0
        for d in z:
            if d > 0:
                win += 1
            if d < 0:
                lose += 1
        diff = win-lose
        total.append([f'{i}_{k}',win,lose,diff])

total = sorted(total,key=lambda x: x[3], reverse=False)
print(*total,sep='\n')
# for i in total:
#     if i[1]>0.01:
#         print(i)

