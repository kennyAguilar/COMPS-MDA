import sqlite3
conn = sqlite3.connect('comps.db')
conn.row_factory = sqlite3.Row
ids = ['301720020100061043942','301720020101060440175','301720020100550470977','301720020100531034763']
for cid in ids:
    r = conn.execute('SELECT player_id, full_name FROM srw_jugadores WHERE player_id = ? LIMIT 1', (cid,)).fetchone()
    if r:
        print(f'ID: {cid} -> SRW nombre: {r["full_name"]}')
    else:
        print(f'ID: {cid} -> NO existe en SRW')
conn.close()
