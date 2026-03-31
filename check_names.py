import sqlite3
conn = sqlite3.connect('comps.db')
conn.row_factory = sqlite3.Row

r1 = conn.execute("SELECT COUNT(DISTINCT cliente_id) as total FROM cortesias WHERE nombre_cliente IS NULL OR TRIM(nombre_cliente) = ''").fetchone()
r1b = conn.execute("SELECT COUNT(DISTINCT cliente_id) as total FROM cortesias").fetchone()

r2 = conn.execute("SELECT COUNT(DISTINCT player_id) as total FROM srw_jugadores WHERE full_name IS NULL OR TRIM(full_name) = ''").fetchone()
r2b = conn.execute("SELECT COUNT(DISTINCT player_id) as total FROM srw_jugadores").fetchone()

r3 = conn.execute("""SELECT COUNT(DISTINCT p.cliente_id) as total FROM premios p
    LEFT JOIN (SELECT DISTINCT player_id, MAX(full_name) as full_name FROM srw_jugadores GROUP BY player_id) s
    ON p.cliente_id = s.player_id
    WHERE s.full_name IS NULL OR TRIM(s.full_name) = ''""").fetchone()
r3b = conn.execute("SELECT COUNT(DISTINCT cliente_id) as total FROM premios").fetchone()

print(f'=== Cortesias (RrtIformeGeneral) ===')
print(f'Clientes sin nombre: {r1["total"]} de {r1b["total"]} unicos')

print(f'\n=== SRW ===')
print(f'Jugadores sin nombre: {r2["total"]} de {r2b["total"]} unicos')

print(f'\n=== Premios (SGOS) ===')
print(f'Clientes sin match en SRW: {r3["total"]} de {r3b["total"]} unicos')

print(f'\n--- Ejemplo cortesias sin nombre ---')
for r in conn.execute("SELECT DISTINCT cliente_id, nombre_cliente FROM cortesias WHERE nombre_cliente IS NULL OR TRIM(nombre_cliente) = '' LIMIT 10").fetchall():
    print(f'  ID: {r["cliente_id"]} | nombre: [{r["nombre_cliente"]}]')

print(f'\n--- Ejemplo SRW sin nombre ---')
for r in conn.execute("SELECT DISTINCT player_id, full_name FROM srw_jugadores WHERE full_name IS NULL OR TRIM(full_name) = '' LIMIT 10").fetchall():
    print(f'  ID: {r["player_id"]} | nombre: [{r["full_name"]}]')

print(f'\n--- Premios sin nombre en SRW ---')
for r in conn.execute("""SELECT DISTINCT p.cliente_id FROM premios p
    LEFT JOIN (SELECT DISTINCT player_id FROM srw_jugadores) s ON p.cliente_id = s.player_id
    WHERE s.player_id IS NULL LIMIT 10""").fetchall():
    print(f'  ID: {r["cliente_id"]}')

conn.close()
