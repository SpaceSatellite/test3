import sqlite3 as sq

con = sq.connect('ETH.db')
cur = con.cursor()

q_close = list(cur.execute('select close from ohlc'))
close = [i[0] for i in q_close]

column_names = cur.execute('PRAGMA table_info("ma_1m")')
column_names = [i[1] for i in column_names][1:]

id_open = 0
id_close = 0
deal_open = 0
deal_high = -100000
deal_low = 100000
deal_close = 0
deal_profit = 0
deal_clear_profit = 0
deal_profit_p = 0
deal_clear_profit_p = 0

for i in column_names:
    ss_1 = f'dbs/{i}.db'
    con_2 = sq.connect(ss_1)
    cur_2 = con_2.cursor()

    a = column_names.index(i)+1
    b = len(column_names)
    for k in column_names[a:b]:
        ss_2 = f'create table if not exists {i}_{k}(id_open, id_close, open, high, low, close, profit, clear_profit, kpd, delta_low, delta_high)'
        cur_2.execute(ss_2)
        con_2.commit()

        s_1 = f'select id,{i},{k} from ma_1m'
        print(f"table {i} and {k}")
        q_full = list(cur.execute(s_1))
        q_part = q_full[column_names.index(k):-column_names.index(k)]
        flag = -1 if (q_part[0][1] - q_part[0][2]) > 0 else 1

        for l in q_part:
            id_point = l[0]
            current_point = close[id_point]
            n = l[1]-l[2]
            
            if n == 0:
                continue

            if current_point > deal_high:
                deal_high = current_point

            if current_point < deal_low:
                deal_low = current_point

            diff = -1 if (n/abs(n)) > 0 else 1
            if diff == flag:
                # if current_point > deal_high:
                #     deal_high = current_point

                # if current_point < deal_low:
                #     deal_low = current_point

                continue

            if diff != flag:
                # if current_point > deal_high:
                #     deal_high = current_point

                # if current_point < deal_low:
                #     deal_low = current_point

                s_2 = f'update {i}_x set {i}_{k} = {flag} where id = {q_full.index(l)}'
                cur.execute(s_2)
                con.commit()

                if deal_open == 0:
                    deal_open = current_point
                    deal_low = current_point
                    deal_high = current_point
                    id_open = id_point
                else:
                    if flag == -1:
                        id_close = id_point
                        deal_close = current_point
                        deal_profit = 1 - (deal_close/deal_open)
                        deal_clear_profit = 1 - ((deal_close-deal_close*0.001)/(deal_open+deal_open*0.001))
                        deal_kpd = deal_high/deal_close
                        deal_delta_low = 1 - (deal_low/deal_open)
                        deal_delta_high = 1 - (deal_open/deal_high)
                    else:
                        id_close = id_point
                        deal_close = current_point
                        deal_profit = 1 - 1/(deal_close/deal_open)
                        deal_clear_profit = 1 - 1/((deal_close-deal_close*0.001)/(deal_open+deal_open*0.001))
                        deal_kpd = deal_low/deal_close
                        deal_delta_high = 1 - (deal_low/deal_open)
                        deal_delta_low = 1 - (deal_open/deal_high)

                    # deal_profit = deal_close - deal_open if flag == -1 else deal_open - deal_close
                    # deal_profit_p = (deal_profit/deal_open)*100
                    # deal_clear_profit = deal_profit - (deal_open + deal_close)*0.001
                    # deal_clear_profit_p = (deal_clear_profit/deal_open)*100
                    # deal_lowest_delta = deal_open - deal_low if flag == -1 else deal_high - deal_open
                    # deal_lowest_delta_p = (deal_lowest_delta/deal_open)*100 
                    # deal_lost_income = deal_high - deal_close if flag == -1 else deal_close - deal_low
                    # deal_lost_income_p = (deal_close/deal_high)*100 

                    # ss_3 = f'insert into {i}_{k} values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                    
                    # data = [
                    #     id_open,                    # id свечи открытия
                    #     id_close,                   # id свечи закрытия
                    #     deal_open,                  # котировка открытия
                    #     deal_high,                  # наивысшая точка в сделке
                    #     deal_low,                   # низшая точка в сделке
                    #     deal_close,                 # котировка закрытия
                    #     deal_profit,                # чистая разница открытия и закрытия
                    #     deal_clear_profit,          # прибыль - коммиссия
                    #     deal_profit_p,              # прибыль в процентах
                    #     deal_clear_profit_p,        # прибыль - коммиссия в процентах
                    #     deal_lowest_delta,          # просадка
                    #     deal_lowest_delta_p,        # просадка в процентах
                    #     deal_lost_income,           # сколько недополучили от максимума
                    #     deal_lost_income_p]         # сколько недополучили, но в процентах - эффективность сделки
                    
                    ss_3 = f'insert into {i}_{k} values(?,?,?,?,?,?,?,?,?,?,?)'
                    
                    data = [
                        id_open,
                        id_close,
                        deal_open,
                        deal_high,
                        deal_low,
                        deal_close,
                        deal_profit*100,
                        deal_clear_profit*100,
                        deal_kpd*100,
                        deal_delta_low*100,
                        deal_delta_high*100
                    ]
                    
                    cur_2.execute(ss_3, data)
                    con_2.commit()

                    deal_open = current_point
                    id_open = id_point
                
                    # # id_open = 0
                    # id_close = 0
                    deal_open = 0
                    # deal_high = -100000000
                    # deal_low = 1000000
                    # deal_close = 0
                    # deal_profit = 0
                    # deal_clear_profit = 0
                    # deal_profit_p = 0
                    # deal_clear_profit_p = 0

                flag = diff
            con.commit()
            con_2.commit()
            

con.commit()
con_2.commit()
