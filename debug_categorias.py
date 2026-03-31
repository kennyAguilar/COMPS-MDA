import sqlite3
db = sqlite3.connect('comps.db')
db.row_factory = sqlite3.Row

print("=== CATEGORIAS_NIVEL ===")
rows = db.execute("SELECT * FROM categorias_nivel").fetchall()
for r in rows:
    print(f"  categoria=[{r['categoria']}] porcentaje={r['porcentaje']}")

print("\n=== PLAYER_LEVEL distintos en SRW ===")
rows = db.execute("SELECT DISTINCT player_level, COUNT(*) as cnt FROM srw_jugadores GROUP BY player_level ORDER BY cnt DESC").fetchall()
for r in rows:
    print(f"  level=[{r['player_level']}] registros={r['cnt']}")

print("\n=== Ejemplo jugador con cortesias ===")
rows = db.execute("""
    SELECT s.player_id, s.full_name, s.player_level,
           SUM(s.coin_in) as total_coin,
           COUNT(DISTINCT s.gaming_date) as dias,
           (SELECT SUM(c.micros) FROM cortesias c WHERE c.cliente_id = s.player_id) as total_micros
    FROM srw_jugadores s
    WHERE s.player_id IN (SELECT DISTINCT cliente_id FROM cortesias)
    GROUP BY s.player_id
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"  {r['full_name']} | level={r['player_level']} | coin_in={r['total_coin']} | dias={r['dias']} | micros={r['total_micros']}")

print("\n=== Dias totales en el mes (feb 2026) ===")
row = db.execute("SELECT COUNT(DISTINCT gaming_date) as dias FROM srw_jugadores WHERE gaming_date LIKE '2026-02%'").fetchone()
print(f"  Dias con actividad en feb: {row['dias']}")

db.close()
