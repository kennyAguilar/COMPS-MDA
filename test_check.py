import requests
for path in ['/analisis/cortesias', '/analisis/premios', '/analisis/resumen']:
    r = requests.get(f'http://127.0.0.1:5000{path}')
    print(f'{path}: {r.status_code}')

import sqlite3
db = sqlite3.connect('d:/COMPS MDA/comps.db')
db.row_factory = sqlite3.Row

print()
print('=== CORTESIAS ===')
cols = [r['name'] for r in db.execute('PRAGMA table_info(cortesias)').fetchall()]
print('Schema:', cols)
total = db.execute('SELECT COUNT(*) c FROM cortesias').fetchone()['c']
print('Registros:', total)
for r in db.execute('SELECT estado, COUNT(*) as n FROM cortesias GROUP BY estado').fetchall():
    print(f"  Estado: {r['estado']} -> {r['n']}")
print()
print('Muestra:')
for r in db.execute('SELECT fecha_jornada, cliente_id, nombre_cliente, descripcion_cat, descripcion_prod, micros, usuario_id, nombre_usuario FROM cortesias LIMIT 2').fetchall():
    print(dict(r))
print()
vacios = db.execute("SELECT COUNT(*) c FROM cortesias WHERE nombre_usuario = '' OR nombre_usuario IS NULL").fetchone()['c']
con_nombre = db.execute("SELECT COUNT(*) c FROM cortesias WHERE nombre_usuario != ''").fetchone()['c']
print(f'Usuarios vacios: {vacios}')
print(f'Usuarios con nombre: {con_nombre}')
print()
print('=== CATEGORIAS ===')
for r in db.execute('SELECT descripcion_cat, COUNT(*) as n, SUM(micros) as total FROM cortesias GROUP BY descripcion_cat ORDER BY total DESC').fetchall():
    print(f"  {r['descripcion_cat']}: {r['n']} items, ${r['total']:,.0f}")
