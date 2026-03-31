import sqlite3
db = sqlite3.connect('comps.db')
db.row_factory = sqlite3.Row

print("=== CORTESIAS coin_in=0, area MDA, feb 2026 ===")
rows = db.execute("""
    SELECT c.cliente_id, c.nombre_cliente, c.fecha_jornada, c.micros, c.usuario_id
    FROM cortesias c
    LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
    WHERE c.fecha_jornada LIKE '2026-02%'
    AND j.area = 'MDA'
    AND NOT EXISTS (
        SELECT 1 FROM srw_jugadores s
        WHERE s.player_id = c.cliente_id
        AND s.gaming_date = c.fecha_jornada
        AND s.coin_in > 0
    )
""").fetchall()

for r in rows:
    cid = r["cliente_id"]
    fj = r["fecha_jornada"]
    nombre = r["nombre_cliente"]
    print(f"\n  {fj} | ID:[{cid}] | {nombre} | micros:{r['micros']}")

    # Buscar premio exacto por jornada
    premios = db.execute(
        "SELECT cliente_id, fecha_jornada, transferencia_final, tipo_pago FROM premios WHERE cliente_id = ? AND fecha_jornada = ?",
        (cid, fj)
    ).fetchall()
    if premios:
        for p in premios:
            print(f"    PREMIO MATCH: monto={p['transferencia_final']} tipo={p['tipo_pago']}")
    else:
        # Buscar premio en cualquier fecha
        premios2 = db.execute(
            "SELECT cliente_id, fecha_jornada, transferencia_final, tipo_pago FROM premios WHERE cliente_id = ?",
            (cid,)
        ).fetchall()
        if premios2:
            print(f"    NO premio en {fj}, pero tiene en otras fechas:")
            for p in premios2:
                print(f"      fecha={p['fecha_jornada']} monto={p['transferencia_final']}")
        else:
            print(f"    NO tiene premios en NINGUNA fecha")

# Ahora buscar JESSICA directamente
print("\n\n=== BUSQUEDA DIRECTA: JESSICA GATICA ===")
jess = db.execute("SELECT * FROM cortesias WHERE nombre_cliente LIKE '%JESSICA%GATICA%'").fetchall()
for r in jess:
    print(f"  cortesia: ID:[{r['cliente_id']}] fecha={r['fecha_jornada']} micros={r['micros']}")

jess_id = jess[0]["cliente_id"] if jess else None
if jess_id:
    print(f"\n  Buscando premios para ID:[{jess_id}]")
    pp = db.execute("SELECT * FROM premios WHERE cliente_id = ?", (jess_id,)).fetchall()
    if pp:
        for p in pp:
            print(f"    premio: fecha={p['fecha_jornada']} monto={p['transferencia_final']} tipo={p['tipo_pago']}")
    else:
        print("    NINGÚN premio encontrado")
        # Buscar con LIKE
        print(f"\n  Buscando premios con LIKE '%{jess_id[-6:]}%'")
        pp2 = db.execute("SELECT cliente_id, fecha_jornada, transferencia_final FROM premios WHERE cliente_id LIKE ?", (f"%{jess_id[-6:]}%",)).fetchall()
        for p in pp2:
            print(f"    premio: ID:[{p['cliente_id']}] fecha={p['fecha_jornada']} monto={p['transferencia_final']}")

# Revisar el (Sin registro en SRW) de 2026-02-03
print("\n\n=== CORTESIA '(Sin registro en SRW)' 2026-02-03 ===")
sin = db.execute("SELECT * FROM cortesias WHERE fecha_jornada = '2026-02-03' AND nombre_cliente LIKE '%Sin registro%'").fetchall()
for r in sin:
    print(f"  ID:[{r['cliente_id']}] micros={r['micros']} usuario={r['usuario_id']}")
    pp = db.execute("SELECT * FROM premios WHERE cliente_id = ? AND fecha_jornada = '2026-02-03'", (r['cliente_id'],)).fetchall()
    print(f"  Premios exactos: {len(pp)}")
    for p in pp:
        print(f"    monto={p['transferencia_final']} tipo={p['tipo_pago']}")

db.close()
