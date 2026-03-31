import sqlite3
conn = sqlite3.connect('comps.db')

# Completar nombres desde SRW
r1 = conn.execute("""
    UPDATE cortesias SET nombre_cliente = (
        SELECT s.full_name FROM srw_jugadores s
        WHERE s.player_id = cortesias.cliente_id LIMIT 1
    ) WHERE nombre_cliente IS NULL OR TRIM(nombre_cliente) = ''
""")
print(f'Actualizados desde SRW: {r1.rowcount}')

# Los que siguen sin nombre
r2 = conn.execute("""
    UPDATE cortesias SET nombre_cliente = '(Sin registro en SRW)'
    WHERE nombre_cliente IS NULL OR TRIM(nombre_cliente) = ''
""")
print(f'Marcados sin registro: {r2.rowcount}')

conn.commit()

# Verificar
for r in conn.execute("SELECT DISTINCT cliente_id, nombre_cliente FROM cortesias WHERE nombre_cliente LIKE '%Sin registro%' OR nombre_cliente IN (SELECT nombre_cliente FROM cortesias WHERE nombre_cliente LIKE '%RAMOS%' OR nombre_cliente LIKE '%MATO%')").fetchall():
    print(f'  ID: {r[0]} -> {r[1]}')

conn.close()
