import sqlite3
db = sqlite3.connect('comps.db')
db.execute("INSERT OR IGNORE INTO jefaturas (usuario_id, nombre, area) VALUES ('405051', '', 'Kiosko')")
db.commit()
print('Insertado 405051 como Kiosko')
rows = db.execute("SELECT usuario_id, nombre, area FROM jefaturas WHERE area = 'Kiosko'").fetchall()
for r in rows:
    print(f'  [{r[0]}] [{r[1]}] [{r[2]}]')
db.close()
