import sqlite3 as sq

con = sq.connect('ETH.db')
cur = con.cursor()

column_names = cur.execute('PRAGMA table_info("ma_1m")')
column_names = [i[1] for i in column_names][1:]

for i in column_names:
    a = column_names.index(i)+1
    b = len(column_names)
    for k in column_names[a:b]:
        
        s_1 = f'select {i},{k} from ma_1m'
        print(f"table {i} and {k}")
        q_full = list(cur.execute(s_1))
        q_part = q_full[column_names.index(k):-column_names.index(k)]

        flag = -1 if (q_part[0][0] - q_part[0][1]) > 0 else 1
        for l in q_part:
            # print(flag)
            n = l[0]-l[1]
            
            if n == 0:
                continue

            diff = -1 if (n/abs(n)) > 0 else 1
            if diff == flag:
                continue
            if diff != flag:
                # print(n, diff, flag)
                flag = diff
                s_2 = f'update {i}_x set {i}_{k} = {flag} where id = {q_full.index(l)}'
                cur.execute(s_2)

con.commit()