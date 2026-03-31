import sqlite3
db = sqlite3.connect('d:/COMPS MDA/comps.db')
db.row_factory = sqlite3.Row

print('=== CARGA LOG ===')
for r in db.execute('SELECT * FROM carga_log ORDER BY id').fetchall():
    print(dict(r))

print()
print('=== CONTEOS ===')
for t in ['srw_jugadores', 'cortesias', 'premios']:
    n = db.execute(f'SELECT COUNT(*) c FROM {t}').fetchone()['c']
    print(f'{t}: {n}')

print()
print('=== SCHEMA CORTESIAS ===')
for r in db.execute('PRAGMA table_info(cortesias)').fetchall():
    print(f"  {r['name']} ({r['type']})")
