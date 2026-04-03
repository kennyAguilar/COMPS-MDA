import os
import sqlite3
from io import BytesIO
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, g, send_file
from werkzeug.utils import secure_filename
import pandas as pd

app = Flask(__name__)
app.secret_key = os.urandom(24)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
DB_PATH = os.path.join(BASE_DIR, 'comps.db')

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


# --------------- SQLite helpers ---------------

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS srw_jugadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gaming_date TEXT,
        player_id TEXT,
        full_name TEXT,
        player_level TEXT,
        coin_in REAL DEFAULT 0,
        total_games INTEGER DEFAULT 0,
        promo_in REAL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS cortesias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_jornada TEXT,
        cliente_id TEXT,
        nombre_cliente TEXT,
        descripcion_cat TEXT,
        descripcion_prod TEXT,
        micros REAL DEFAULT 0,
        estado TEXT,
        usuario_id TEXT,
        nombre_usuario TEXT
    );

    CREATE TABLE IF NOT EXISTS premios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_jornada TEXT,
        cliente_id TEXT,
        transferencia_final REAL DEFAULT 0,
        tipo_pago TEXT
    );

    CREATE TABLE IF NOT EXISTS mesas_puntos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_operacion TEXT,
        cliente_id TEXT,
        cliente_nombre TEXT,
        puntos REAL DEFAULT 0,
        coin_in_puntos REAL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS carga_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tabla TEXT,
        archivo TEXT,
        filas INTEGER,
        fecha_carga TEXT
    );

    CREATE TABLE IF NOT EXISTS jefaturas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id TEXT UNIQUE,
        nombre TEXT,
        area TEXT
    );

    CREATE TABLE IF NOT EXISTS categorias_nivel (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        categoria TEXT UNIQUE,
        porcentaje REAL DEFAULT 0
    );
    """)
    db.close()


def cargar_jefaturas():
    """Carga Jefatura.xlsx (Hoja1 y Hoja2) en las tablas jefaturas y categorias_nivel."""
    filepath = os.path.join(BASE_DIR, 'Jefatura.xlsx')
    if not os.path.exists(filepath):
        return

    db = sqlite3.connect(DB_PATH)

    # Hoja1: jefaturas
    df1 = pd.read_excel(filepath, sheet_name='Hoja1', header=0)
    df1.columns = ['usuario_id', 'nombre', 'area']
    df1['usuario_id'] = df1['usuario_id'].astype(str).str.strip()
    df1['nombre'] = df1['nombre'].fillna('')
    df1['area'] = df1['area'].fillna('')

    db.execute("DELETE FROM jefaturas")
    df1.to_sql('jefaturas', db, if_exists='append', index=False)

    # Hoja2: categorias_nivel
    df2 = pd.read_excel(filepath, sheet_name='Hoja2', header=0)
    df2.columns = ['categoria', 'porcentaje']
    df2 = df2.dropna(subset=['categoria'])
    df2['porcentaje'] = pd.to_numeric(df2['porcentaje'], errors='coerce').fillna(0)

    db.execute("DELETE FROM categorias_nivel")
    df2.to_sql('categorias_nivel', db, if_exists='append', index=False)

    db.commit()
    db.close()


# --------------- ETL: cargar archivos Excel ---------------

def limpiar_player_id(val):
    """Limpia IDs de cliente quitando 'x' de prefijo/sufijo."""
    if pd.isna(val):
        return None
    s = str(val).strip().strip('x')
    return s if s else None


def cargar_srw(filepath):
    df = pd.read_excel(filepath, header=None, skiprows=3)
    # Quitar primera columna vacía
    df = df.iloc[:, 1:]
    df.columns = [
        'gaming_date', 'player_id', 'full_name', 'player_level',
        'coin_in', 'rec_cin', 'coin_out', 'rec_cout',
        'jackpot_amount', 'promo_in', 'promo_out', 'prom_jugado',
        'win_loss_mda', 'win_loss_mda_rec', 'bill_in',
        'total_games', 'total_egm_points'
    ]

    # Conservar solo columnas requeridas
    df = df[['gaming_date', 'player_id', 'full_name', 'player_level',
             'coin_in', 'total_games', 'promo_in']]

    df = df.dropna(subset=['player_id'])
    df['player_id'] = df['player_id'].astype(str).str.strip()

    # gaming_date ya viene por jornada del sistema casino (9am-8am = 1 día)
    df['gaming_date'] = pd.to_datetime(df['gaming_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.dropna(subset=['gaming_date'])

    for c in ['coin_in', 'total_games', 'promo_in']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    return df


def cargar_cortesias(filepath):
    df = pd.read_excel(filepath, header=None, skiprows=8)
    # Mapear por índice (archivo tiene celdas fusionadas)
    df = df.rename(columns={
        6: 'fecha_jornada', 7: 'cliente_id', 10: 'nombre_cliente',
        14: 'descripcion_cat', 16: 'descripcion_prod', 19: 'micros',
        22: 'estado', 28: 'usuario_id', 29: 'nombre_usuario'
    })
    cols = ['fecha_jornada', 'cliente_id', 'nombre_cliente',
            'descripcion_cat', 'descripcion_prod', 'micros',
            'estado', 'usuario_id', 'nombre_usuario']
    df = df[cols]

    # Filtrar solo QUEMADO
    df = df[df['estado'] == 'QUEMADO']

    df = df.dropna(subset=['cliente_id'])
    df['cliente_id'] = df['cliente_id'].astype(str).str.strip()
    df['fecha_jornada'] = pd.to_datetime(df['fecha_jornada'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['micros'] = pd.to_numeric(df['micros'], errors='coerce').fillna(0)
    df['usuario_id'] = df['usuario_id'].astype(str).str.replace(r'\.0$', '', regex=True)
    # nombre_usuario: conservar todos, incluso vacíos
    df['nombre_usuario'] = df['nombre_usuario'].fillna('')

    return df


def cargar_premios(filepath):
    df = pd.read_excel(filepath, header=1)
    df.columns = [
        'fecha', 'maquina', 'id_mensaje', 'cliente_id',
        'monto_transferido', 'propina', 'transferencia_final',
        'slot_attendant', 'monto_slot_atten', 'validador',
        'monto_validador', 'tipo_pago', 'ingreso_cawa'
    ]

    # Filtrar solo Jackpot HP y Progressive Jackpot HP
    df = df[df['tipo_pago'].isin(['Jackpot HP', 'Progressive Jackpot HP'])]

    df = df.dropna(subset=['cliente_id'])
    df['cliente_id'] = df['cliente_id'].astype(str).str.strip().str.strip('x')
    df['transferencia_final'] = pd.to_numeric(df['transferencia_final'], errors='coerce').fillna(0)

    # Convertir fecha a jornada (antes de 9am = día anterior)
    df['fecha_dt'] = pd.to_datetime(df['fecha'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['fecha_dt'])
    df['fecha_jornada'] = df['fecha_dt'].apply(
        lambda dt: (dt - pd.Timedelta(hours=9)).strftime('%Y-%m-%d')
    )

    return df[['fecha_jornada', 'cliente_id', 'transferencia_final', 'tipo_pago']]


def cargar_mesas_puntos(filepath):
    df = pd.read_excel(filepath, header=None, skiprows=2)
    # Columna 0 vacía, headers reales en índices 1..11
    df = df.iloc[:, 1:]
    df.columns = [
        'fecha_operacion', 'sesion_id', 'mesa_id', 'juego',
        'cliente_id', 'cliente_nombre', 'hora_inicio', 'hora_fin',
        'tpo_jugado', 'ap_promedio', 'puntos'
    ]

    df = df[['fecha_operacion', 'cliente_id', 'cliente_nombre', 'puntos']]
    df = df.dropna(subset=['cliente_id'])
    df['cliente_id'] = df['cliente_id'].astype(str).str.strip()
    df['fecha_operacion'] = pd.to_datetime(df['fecha_operacion'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['puntos'] = pd.to_numeric(df['puntos'], errors='coerce').fillna(0)
    df['coin_in_puntos'] = df['puntos'] * 1000
    return df


# --------------- Rutas ---------------

@app.route('/')
def index():
    db = get_db()
    log = db.execute("SELECT * FROM carga_log ORDER BY fecha_carga DESC").fetchall()

    stats = {}
    for tabla in ['srw_jugadores', 'cortesias', 'premios', 'mesas_puntos']:
        row = db.execute(f"SELECT COUNT(*) as cnt FROM {tabla}").fetchone()
        stats[tabla] = row['cnt']

    return render_template('index.html', log=log, stats=stats)


ALLOWED_EXTENSIONS = {'.xls', '.xlsx'}


def allowed_file(filename):
    return os.path.splitext(filename)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/cargar', methods=['POST'])
def cargar_datos():
    db = get_db()
    resultados = []

    file_map = {
        'archivo_srw': ('srw_jugadores', cargar_srw),
        'archivo_cortesias': ('cortesias', cargar_cortesias),
        'archivo_premios': ('premios', cargar_premios),
        'archivo_mesas_puntos': ('mesas_puntos', cargar_mesas_puntos),
    }

    try:
        for field, (tabla, etl_fn) in file_map.items():
            f = request.files.get(field)
            if not f or f.filename == '':
                continue
            if not allowed_file(f.filename):
                flash(f'Archivo no válido: {f.filename}. Solo .xls y .xlsx', 'error')
                continue

            filename = secure_filename(f.filename)
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            f.save(filepath)

            db.execute(f"DELETE FROM {tabla}")
            df = etl_fn(filepath)
            df.to_sql(tabla, db, if_exists='append', index=False)
            db.execute(
                "INSERT INTO carga_log (tabla, archivo, filas, fecha_carga) VALUES (?,?,?,?)",
                (tabla, filename, len(df), datetime.now().isoformat())
            )
            resultados.append(f"{tabla}: {len(df)} filas cargadas ({filename})")

        if resultados:
            # Actualizar nombres en cortesias desde SRW
            db.execute("""
                UPDATE cortesias SET nombre_cliente = (
                    SELECT s.full_name FROM srw_jugadores s
                    WHERE s.player_id = cortesias.cliente_id LIMIT 1
                ) WHERE nombre_cliente IS NULL OR TRIM(nombre_cliente) = ''
            """)
            # Los que siguen sin nombre, marcar como (Sin registro)
            db.execute("""
                UPDATE cortesias SET nombre_cliente = '(Sin registro en SRW)'
                WHERE nombre_cliente IS NULL OR TRIM(nombre_cliente) = ''
            """)
            db.commit()
            flash(' | '.join(resultados), 'success')
        else:
            flash('No se seleccionó ningún archivo.', 'error')

    except Exception as e:
        db.rollback()
        flash(f'Error al cargar datos: {str(e)}', 'error')

    return redirect(url_for('index'))


def build_date_filter(col, anio, mes):
    """Construye cláusula WHERE y params para filtro año/mes."""
    conditions = []
    params = []
    if anio:
        conditions.append(f"SUBSTR({col}, 1, 4) = ?")
        params.append(anio)
    if mes:
        conditions.append(f"SUBSTR({col}, 6, 2) = ?")
        params.append(mes)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


def get_anios_meses(db):
    """Obtiene años y meses disponibles de las 3 tablas."""
    rows = db.execute("""
        SELECT DISTINCT fecha FROM (
            SELECT SUBSTR(gaming_date, 1, 7) as fecha FROM srw_jugadores WHERE gaming_date IS NOT NULL
            UNION
            SELECT SUBSTR(fecha_jornada, 1, 7) FROM cortesias WHERE fecha_jornada IS NOT NULL
            UNION
            SELECT SUBSTR(fecha_jornada, 1, 7) FROM premios WHERE fecha_jornada IS NOT NULL
            UNION
            SELECT SUBSTR(fecha_operacion, 1, 7) FROM mesas_puntos WHERE fecha_operacion IS NOT NULL
        ) ORDER BY fecha
    """).fetchall()
    anios = sorted(set(r['fecha'][:4] for r in rows))
    meses_num = sorted(set(r['fecha'][5:7] for r in rows))
    return anios, meses_num


@app.route('/analisis/cortesias')
def analisis_cortesias():
    db = get_db()
    anio = request.args.get('anio', '')
    mes = request.args.get('mes', '')
    anios, meses_disp = get_anios_meses(db)

    cw, cp = build_date_filter('c.fecha_jornada', anio, mes)
    cw_solo, cp_solo = build_date_filter('fecha_jornada', anio, mes)
    sw, sp = build_date_filter('gaming_date', anio, mes)
    mw_cort, mp_cort = build_date_filter('m.fecha_operacion', anio, mes)

    # Cortesías por jugador con su coin-in total (SRW + mesas)
    resumen = db.execute(f"""
        SELECT
            c.cliente_id,
            c.nombre_cliente,
            COUNT(c.id) as total_cortesias,
            SUM(c.micros) as monto_cortesias,
            COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0) as total_coin_in,
            COALESCE(s.total_promo_in, 0) as total_promo_in,
            COALESCE(s.total_games, 0) as total_games,
            COALESCE(s.player_level, '-') as player_level,
            CASE WHEN (COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0)) > 0
                 THEN ROUND(SUM(c.micros) * 100.0 / (COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0)), 4)
                 ELSE 0 END as pct_cortesia_coin_in
        FROM cortesias c
        LEFT JOIN (
            SELECT player_id,
                   SUM(coin_in) as total_coin_in,
                   SUM(promo_in) as total_promo_in,
                   SUM(total_games) as total_games,
                   MAX(player_level) as player_level
            FROM srw_jugadores {sw}
            GROUP BY player_id
        ) s ON c.cliente_id = s.player_id
        LEFT JOIN (
            SELECT cliente_id,
                   SUM(coin_in_puntos) as coin_in_mesas
            FROM mesas_puntos m {mw_cort}
            GROUP BY cliente_id
        ) m ON c.cliente_id = m.cliente_id
        {cw}
        GROUP BY c.cliente_id, c.nombre_cliente
        ORDER BY monto_cortesias DESC
    """, sp + mp_cort + cp).fetchall()

    # Cortesías por categoría
    por_categoria = db.execute(f"""
        SELECT descripcion_cat, COUNT(*) as cantidad,
               SUM(micros) as monto_total
        FROM cortesias {cw_solo}
        GROUP BY descripcion_cat
        ORDER BY monto_total DESC
    """, cp_solo).fetchall()

    # Productos por categoría (para desglose)
    productos_por_cat = {}
    rows = db.execute(f"""
        SELECT descripcion_cat, descripcion_prod, COUNT(*) as cantidad,
               SUM(micros) as monto_total
        FROM cortesias {cw_solo}
        GROUP BY descripcion_cat, descripcion_prod
        ORDER BY descripcion_cat, monto_total DESC
    """, cp_solo).fetchall()
    for r in rows:
        cat = r['descripcion_cat']
        if cat not in productos_por_cat:
            productos_por_cat[cat] = []
        productos_por_cat[cat].append(dict(r))

    # Cortesías por día
    dia_where, dia_params = build_date_filter('fecha_jornada', anio, mes)
    if dia_where:
        dia_where = dia_where + " AND fecha_jornada IS NOT NULL"
    else:
        dia_where = "WHERE fecha_jornada IS NOT NULL"
    por_dia = db.execute(f"""
        SELECT fecha_jornada, COUNT(*) as cantidad,
               SUM(micros) as monto_total
        FROM cortesias {dia_where}
        GROUP BY fecha_jornada
        ORDER BY fecha_jornada
    """, dia_params).fetchall()

    # Totales
    totales = db.execute(f"""
        SELECT COUNT(*) as total_cortesias,
               SUM(micros) as monto_total,
               COUNT(DISTINCT cliente_id) as clientes_unicos
        FROM cortesias {cw_solo}
    """, cp_solo).fetchone()

    total_coin_in_srw = db.execute(f"SELECT SUM(coin_in) as total FROM srw_jugadores {sw}", sp).fetchone()
    mw_total, mp_total = build_date_filter('fecha_operacion', anio, mes)
    total_coin_in_mesas = db.execute(f"SELECT SUM(coin_in_puntos) as total FROM mesas_puntos {mw_total}", mp_total).fetchone()
    total_coin_in_combined = (total_coin_in_srw['total'] or 0) + (total_coin_in_mesas['total'] or 0)

    return render_template('analisis_cortesias.html',
                           resumen=resumen,
                           por_categoria=por_categoria,
                           productos_por_cat=productos_por_cat,
                           por_dia=por_dia,
                           totales=totales,
                           total_coin_in=total_coin_in_combined,
                           anios=anios, meses_disp=meses_disp,
                           anio_actual=anio, mes_actual=mes)


@app.route('/analisis/premios')
def analisis_premios():
    db = get_db()
    anio = request.args.get('anio', '')
    mes = request.args.get('mes', '')
    anios, meses_disp = get_anios_meses(db)

    pw, pp = build_date_filter('p.fecha_jornada', anio, mes)
    pw_solo, pp_solo = build_date_filter('fecha_jornada', anio, mes)
    sw, sp = build_date_filter('gaming_date', anio, mes)
    mw_prem, mp_prem = build_date_filter('m.fecha_operacion', anio, mes)

    # Premios por jugador (coin_in = SRW + mesas)
    por_jugador = db.execute(f"""
        SELECT
            p.cliente_id,
            COALESCE(s.full_name, m.cliente_nombre, '(Sin nombre)') as nombre,
            COALESCE(s.player_level, '-') as player_level,
            COUNT(p.id) as total_premios,
            SUM(p.transferencia_final) as monto_total,
            COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0) as total_coin_in,
            COALESCE(s.total_promo_in, 0) as total_promo_in,
            COALESCE(s.total_games, 0) as total_games,
            CASE WHEN (COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0)) > 0
                 THEN ROUND(SUM(p.transferencia_final) * 100.0 / (COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0)), 4)
                 ELSE 0 END as pct_premio_coin_in
        FROM premios p
        LEFT JOIN (
            SELECT player_id, MAX(full_name) as full_name,
                   MAX(player_level) as player_level,
                   SUM(coin_in) as total_coin_in,
                   SUM(promo_in) as total_promo_in,
                   SUM(total_games) as total_games
            FROM srw_jugadores {sw} GROUP BY player_id
        ) s ON p.cliente_id = s.player_id
        LEFT JOIN (
            SELECT cliente_id, MAX(cliente_nombre) as cliente_nombre,
                   SUM(coin_in_puntos) as coin_in_mesas
            FROM mesas_puntos m {mw_prem}
            GROUP BY cliente_id
        ) m ON p.cliente_id = m.cliente_id
        {pw}
        GROUP BY p.cliente_id
        ORDER BY monto_total DESC
    """, sp + mp_prem + pp).fetchall()

    # Premios por tipo de pago
    por_tipo = db.execute(f"""
        SELECT tipo_pago, COUNT(*) as cantidad,
               SUM(transferencia_final) as monto_total
        FROM premios {pw_solo}
        GROUP BY tipo_pago
        ORDER BY monto_total DESC
    """, pp_solo).fetchall()

    # Premios por día (jornada)
    dia_where, dia_params = build_date_filter('fecha_jornada', anio, mes)
    if dia_where:
        dia_where = dia_where + " AND fecha_jornada IS NOT NULL"
    else:
        dia_where = "WHERE fecha_jornada IS NOT NULL"
    por_dia = db.execute(f"""
        SELECT fecha_jornada, COUNT(*) as cantidad,
               SUM(transferencia_final) as monto_total
        FROM premios {dia_where}
        GROUP BY fecha_jornada
        ORDER BY fecha_jornada
    """, dia_params).fetchall()

    # Totales
    totales = db.execute(f"""
        SELECT COUNT(*) as total_premios,
               SUM(transferencia_final) as monto_total,
               COUNT(DISTINCT cliente_id) as clientes_unicos
        FROM premios {pw_solo}
    """, pp_solo).fetchone()

    return render_template('analisis_premios.html',
                           por_jugador=por_jugador,
                           por_tipo=por_tipo,
                           por_dia=por_dia,
                           totales=totales,
                           anios=anios, meses_disp=meses_disp,
                           anio_actual=anio, mes_actual=mes)


@app.route('/analisis/resumen')
def analisis_resumen():
    db = get_db()
    anio = request.args.get('anio', '')
    mes = request.args.get('mes', '')
    anios, meses_disp = get_anios_meses(db)

    sw, sp = build_date_filter('gaming_date', anio, mes)
    cw, cparam = build_date_filter('fecha_jornada', anio, mes)
    pw, pparam = build_date_filter('fecha_jornada', anio, mes)

    # Filtro mesas para resumen
    mw_res, mp_res = build_date_filter('m.fecha_operacion', anio, mes)
    mw_res_solo, mp_res_solo = build_date_filter('fecha_operacion', anio, mes)

    # Resumen general de jugadores con cortesías + premios (coin_in = SRW + mesas)
    jugadores = db.execute(f"""
        SELECT
            s.player_id,
            s.full_name,
            s.player_level,
            s.total_coin_in + COALESCE(m.coin_in_mesas, 0) as total_coin_in,
            s.total_promo_in,
            s.total_games,
            s.dias_jugados,
            COALESCE(c.total_cortesias, 0) as total_cortesias,
            COALESCE(c.monto_cortesias, 0) as monto_cortesias,
            COALESCE(p.total_premios, 0) as total_premios,
            COALESCE(p.monto_premios, 0) as monto_premios,
            CASE WHEN (s.total_coin_in + COALESCE(m.coin_in_mesas, 0)) > 0
                 THEN ROUND((COALESCE(c.monto_cortesias,0) + COALESCE(p.monto_premios,0)) * 100.0 / (s.total_coin_in + COALESCE(m.coin_in_mesas, 0)), 4)
                 ELSE 0 END as pct_total_coin_in
        FROM (
            SELECT player_id, MAX(full_name) as full_name,
                   MAX(player_level) as player_level,
                   SUM(coin_in) as total_coin_in,
                   SUM(promo_in) as total_promo_in,
                   SUM(total_games) as total_games,
                   COUNT(DISTINCT gaming_date) as dias_jugados
            FROM srw_jugadores {sw} GROUP BY player_id
        ) s
        LEFT JOIN (
            SELECT cliente_id, COUNT(*) as total_cortesias,
                   SUM(micros) as monto_cortesias
            FROM cortesias {cw} GROUP BY cliente_id
        ) c ON s.player_id = c.cliente_id
        LEFT JOIN (
            SELECT cliente_id, COUNT(*) as total_premios,
                   SUM(transferencia_final) as monto_premios
            FROM premios {pw} GROUP BY cliente_id
        ) p ON s.player_id = p.cliente_id
        LEFT JOIN (
            SELECT cliente_id,
                   SUM(coin_in_puntos) as coin_in_mesas
            FROM mesas_puntos m {mw_res}
            GROUP BY cliente_id
        ) m ON s.player_id = m.cliente_id
        WHERE COALESCE(c.total_cortesias, 0) > 0 OR COALESCE(p.total_premios, 0) > 0
        ORDER BY total_coin_in DESC
    """, sp + cparam + pparam + mp_res).fetchall()

    # KPIs globales (coin_in = SRW + mesas)
    kpis_srw = db.execute(f"""
        SELECT
            COALESCE(SUM(coin_in), 0) as total_coin_in,
            COALESCE(SUM(promo_in), 0) as total_promo_in,
            COALESCE(SUM(total_games), 0) as total_games,
            COUNT(DISTINCT player_id) as jugadores_srw
        FROM srw_jugadores {sw}
    """, sp).fetchone()
    kpis_mesas = db.execute(f"""
        SELECT COALESCE(SUM(coin_in_puntos), 0) as total_coin_in_mesas
        FROM mesas_puntos {mw_res_solo}
    """, mp_res_solo).fetchone()
    kpis_cort = db.execute(f"""
        SELECT COALESCE(SUM(micros), 0) as total_cortesias,
               COUNT(DISTINCT cliente_id) as clientes_cortesias
        FROM cortesias {cw}
    """, cparam).fetchone()
    kpis_prem = db.execute(f"""
        SELECT COALESCE(SUM(transferencia_final), 0) as total_premios,
               COUNT(DISTINCT cliente_id) as clientes_premios
        FROM premios {pw}
    """, pparam).fetchone()
    # Combinar en dict compatible
    kpis = {
        'total_coin_in': kpis_srw['total_coin_in'] + kpis_mesas['total_coin_in_mesas'],
        'total_promo_in': kpis_srw['total_promo_in'],
        'total_games': kpis_srw['total_games'],
        'jugadores_srw': kpis_srw['jugadores_srw'],
        'total_cortesias': kpis_cort['total_cortesias'],
        'clientes_cortesias': kpis_cort['clientes_cortesias'],
        'total_premios': kpis_prem['total_premios'],
        'clientes_premios': kpis_prem['clientes_premios'],
    }

    return render_template('analisis_resumen.html', jugadores=jugadores, kpis=kpis,
                           anios=anios, meses_disp=meses_disp,
                           anio_actual=anio, mes_actual=mes)


@app.route('/api/cortesias-dia')
def api_cortesias_dia():
    db = get_db()
    rows = db.execute("""
        SELECT fecha_jornada as fecha, SUM(micros) as monto
        FROM cortesias WHERE fecha_jornada IS NOT NULL
        GROUP BY fecha_jornada ORDER BY fecha_jornada
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/coin-in-dia')
def api_coin_in_dia():
    db = get_db()
    rows = db.execute("""
        SELECT gaming_date as fecha, SUM(coin_in) as monto
        FROM srw_jugadores WHERE gaming_date IS NOT NULL
        GROUP BY gaming_date ORDER BY gaming_date
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/premios-tipo')
def api_premios_tipo():
    db = get_db()
    rows = db.execute("""
        SELECT tipo_pago as tipo, COUNT(*) as cantidad, SUM(transferencia_final) as monto
        FROM premios GROUP BY tipo_pago ORDER BY monto DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/control/invitaciones')
def control_invitaciones():
    db = get_db()
    anio = request.args.get('anio', '')
    mes = request.args.get('mes', '')
    area = request.args.get('area', '')
    jefe = request.args.get('jefe', '')
    anios, meses_disp = get_anios_meses(db)

    # Áreas disponibles
    areas = [r['area'] for r in db.execute(
        "SELECT DISTINCT area FROM jefaturas WHERE area != '' ORDER BY area"
    ).fetchall()]

    # Jefes filtrados por área
    if area:
        jefes_disp = db.execute(
            "SELECT usuario_id, nombre FROM jefaturas WHERE area = ? ORDER BY nombre", (area,)
        ).fetchall()
    else:
        jefes_disp = db.execute(
            "SELECT usuario_id, nombre FROM jefaturas ORDER BY nombre"
        ).fetchall()
    jefes_disp = [(r['usuario_id'], r['nombre']) for r in jefes_disp]

    # Filtros de jefatura para cortesías
    jefe_filter = ""
    jefe_params = []
    if jefe:
        jefe_filter = " AND c.usuario_id = ?"
        jefe_params = [jefe]
    elif area:
        jefe_filter = " AND c.usuario_id IN (SELECT usuario_id FROM jefaturas WHERE area = ?)"
        jefe_params = [area]

    sw, sp = build_date_filter('s.gaming_date', anio, mes)
    cw, cparam = build_date_filter('c.fecha_jornada', anio, mes)
    pw, pparam = build_date_filter('p.fecha_jornada', anio, mes)

    # Días totales del periodo (para % asistencia) — combina SRW + mesas
    sw_solo, sp_solo = build_date_filter('gaming_date', anio, mes)
    mw_solo_dias, mp_solo_dias = build_date_filter('fecha_operacion', anio, mes)
    dias_totales_row = db.execute(f"""
        SELECT COUNT(DISTINCT fecha) as dias FROM (
            SELECT gaming_date as fecha FROM srw_jugadores {sw_solo}
            UNION
            SELECT fecha_operacion as fecha FROM mesas_puntos {mw_solo_dias}
        )
    """, sp_solo + mp_solo_dias).fetchone()
    dias_totales = dias_totales_row['dias'] or 1

    # Porcentaje primario
    prim_row = db.execute(
        "SELECT porcentaje FROM categorias_nivel WHERE categoria = 'Primario'"
    ).fetchone()
    pct_primario = prim_row['porcentaje'] if prim_row else 0

    # Mapeo categoría -> porcentaje
    cat_rows = db.execute(
        "SELECT categoria, porcentaje FROM categorias_nivel WHERE categoria != 'Primario'"
    ).fetchall()
    pct_categoria = {r['categoria']: r['porcentaje'] for r in cat_rows}

    # Construir subconsulta de cortesías con filtro de jefatura
    cw_inner = cw
    if jefe_filter:
        if cw_inner:
            cw_inner = cw_inner + jefe_filter
        else:
            cw_inner = "WHERE 1=1" + jefe_filter

    # Filtro de fecha para mesas_puntos
    mw, mparam = build_date_filter('m.fecha_operacion', anio, mes)
    mw_inner, mparam_inner = build_date_filter('mp.fecha_operacion', anio, mes)
    # Filtros sin alias para subconsultas internas
    sw_plain, sp_plain = build_date_filter('gaming_date', anio, mes)
    mw_plain, mp_plain = build_date_filter('fecha_operacion', anio, mes)

    # --- Query 1: Jugadores SRW (+ coin_in y días de mesas si aplica) ---
    jugadores_srw = db.execute(f"""
        SELECT
            s.player_id,
            MAX(s.full_name) as nombre,
            MAX(s.player_level) as nivel,
            SUM(s.coin_in) + COALESCE(MAX(m.coin_in_mesas), 0) as coin_in_mensual,
            COALESCE(d.dias_combinados, COUNT(DISTINCT s.gaming_date)) as dias_asistidos,
            COALESCE(c.total_cortesias, 0) as total_cortesias,
            COALESCE(c.monto_micros, 0) as monto_micros,
            COALESCE(p.cant_premios, 0) as cant_premios,
            COALESCE(p.monto_premios, 0) as monto_premios
        FROM srw_jugadores s
        LEFT JOIN (
            SELECT cliente_id,
                   COUNT(*) as total_cortesias,
                   SUM(micros) as monto_micros
            FROM cortesias c {cw_inner}
            GROUP BY cliente_id
        ) c ON s.player_id = c.cliente_id
        LEFT JOIN (
            SELECT cliente_id,
                   COUNT(*) as cant_premios,
                   SUM(transferencia_final) as monto_premios
            FROM premios p {pw}
            GROUP BY cliente_id
        ) p ON s.player_id = p.cliente_id
        LEFT JOIN (
            SELECT cliente_id,
                   SUM(coin_in_puntos) as coin_in_mesas
            FROM mesas_puntos m {mw}
            GROUP BY cliente_id
        ) m ON s.player_id = m.cliente_id
        LEFT JOIN (
            SELECT cliente_id, COUNT(DISTINCT fecha) as dias_combinados FROM (
                SELECT player_id as cliente_id, gaming_date as fecha FROM srw_jugadores {sw_plain}
                UNION
                SELECT cliente_id, fecha_operacion as fecha FROM mesas_puntos {mw_plain}
            ) GROUP BY cliente_id
        ) d ON s.player_id = d.cliente_id
        {sw}
        GROUP BY s.player_id
        HAVING COALESCE(c.total_cortesias, 0) > 0
        ORDER BY coin_in_mensual DESC
    """, cparam + jefe_params + pparam + mparam + sp_plain + mp_plain + sp).fetchall()

    # --- Query 2: Jugadores solo de mesas (no están en SRW) con cortesías ---
    sw_excl, sp_excl = build_date_filter('gaming_date', anio, mes)
    # Construir WHERE para mesas-only: siempre excluir players que están en SRW
    mesas_excl = f"mp.cliente_id NOT IN (SELECT DISTINCT player_id FROM srw_jugadores {sw_excl})"
    if mparam_inner:
        # Hay filtro de fecha: WHERE fecha + AND NOT IN
        mw_conditions = []
        if anio:
            mw_conditions.append("SUBSTR(mp.fecha_operacion, 1, 4) = ?")
        if mes:
            mw_conditions.append("SUBSTR(mp.fecha_operacion, 6, 2) = ?")
        mesas_where = "WHERE " + " AND ".join(mw_conditions) + " AND " + mesas_excl
    else:
        mesas_where = "WHERE " + mesas_excl

    jugadores_mesas = db.execute(f"""
        SELECT
            mp.cliente_id as player_id,
            MAX(mp.cliente_nombre) as nombre,
            'MDJ' as nivel,
            SUM(mp.coin_in_puntos) as coin_in_mensual,
            COUNT(DISTINCT mp.fecha_operacion) as dias_asistidos,
            COALESCE(c.total_cortesias, 0) as total_cortesias,
            COALESCE(c.monto_micros, 0) as monto_micros,
            COALESCE(p.cant_premios, 0) as cant_premios,
            COALESCE(p.monto_premios, 0) as monto_premios
        FROM mesas_puntos mp
        LEFT JOIN (
            SELECT cliente_id,
                   COUNT(*) as total_cortesias,
                   SUM(micros) as monto_micros
            FROM cortesias c {cw_inner}
            GROUP BY cliente_id
        ) c ON mp.cliente_id = c.cliente_id
        LEFT JOIN (
            SELECT cliente_id,
                   COUNT(*) as cant_premios,
                   SUM(transferencia_final) as monto_premios
            FROM premios p {pw}
            GROUP BY cliente_id
        ) p ON mp.cliente_id = p.cliente_id
        {mesas_where}
        GROUP BY mp.cliente_id
        HAVING COALESCE(c.total_cortesias, 0) > 0
        ORDER BY coin_in_mensual DESC
    """, cparam + jefe_params + pparam + mparam_inner + sp_excl).fetchall()

    jugadores = list(jugadores_srw) + list(jugadores_mesas)

    # Calcular invitaciones en Python (necesita mapeo de categoría)
    resultados = []
    for j in jugadores:
        nivel = j['nivel'] or ''
        pct_cat = pct_categoria.get(nivel, 0)
        coin_in = j['coin_in_mensual'] or 0
        invitacion_mensual = coin_in * pct_primario * pct_cat
        monto_micros = j['monto_micros'] or 0
        saldo = invitacion_mensual - monto_micros
        dias = j['dias_asistidos'] or 0
        pct_asistencia = round(dias * 100.0 / dias_totales, 1) if dias_totales > 0 else 0

        resultados.append({
            'nombre': j['nombre'],
            'nivel': nivel,
            'dias_asistidos': dias,
            'pct_asistencia': pct_asistencia,
            'cant_premios': j['cant_premios'],
            'monto_premios': j['monto_premios'] or 0,
            'coin_in_mensual': coin_in,
            'total_cortesias': j['total_cortesias'],
            'monto_micros': monto_micros,
            'invitacion_mensual': round(invitacion_mensual),
            'saldo': round(saldo),
            'pct_cat': pct_cat,
        })

    # Cortesías para gráfico de torta
    cw_chart = build_date_filter('c.fecha_jornada', anio, mes)
    chart_where = cw_chart[0] if cw_chart[0] else ""
    chart_params = list(cw_chart[1])

    if area:
        # Sección seleccionada → agrupar por jefatura (nombre del jefe)
        if chart_where:
            chart_where += " AND j.area = ?"
        else:
            chart_where = "WHERE j.area = ?"
        chart_params.append(area)
        chart_rows = db.execute(f"""
            SELECT j.nombre as etiqueta, COUNT(*) as cantidad
            FROM cortesias c
            LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
            {chart_where}
            GROUP BY j.nombre
            ORDER BY cantidad DESC
        """, chart_params).fetchall()
        chart_titulo = f"Cortesías por Jefe — {area}"
    else:
        # Todas las secciones → agrupar por área
        if chart_where:
            chart_where += " AND j.area IS NOT NULL AND j.area != ''"
        else:
            chart_where = "WHERE j.area IS NOT NULL AND j.area != ''"
        chart_rows = db.execute(f"""
            SELECT j.area as etiqueta, COUNT(*) as cantidad
            FROM cortesias c
            LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
            {chart_where}
            GROUP BY j.area
            ORDER BY cantidad DESC
        """, chart_params).fetchall()
        chart_titulo = "Cortesías por Sección"

    chart_labels = [r['etiqueta'] or 'Sin asignar' for r in chart_rows]
    chart_cantidades = [r['cantidad'] for r in chart_rows]

    return render_template('control_invitaciones.html',
                           resultados=resultados,
                           dias_totales=dias_totales,
                           pct_primario=pct_primario,
                           pct_categoria=pct_categoria,
                           chart_labels=chart_labels,
                           chart_cantidades=chart_cantidades,
                           chart_titulo=chart_titulo,
                           anios=anios, meses_disp=meses_disp,
                           areas=areas, jefes_disp=jefes_disp,
                           anio_actual=anio, mes_actual=mes,
                           area_actual=area, jefe_actual=jefe)


@app.route('/auditoria/coinin-cero')
def auditoria_coinin_cero():
    db = get_db()
    anio = request.args.get('anio', '')
    mes = request.args.get('mes', '')
    area = request.args.get('area', '')
    jefe = request.args.get('jefe', '')
    anios, meses_disp = get_anios_meses(db)

    # Áreas disponibles
    areas = [r['area'] for r in db.execute(
        "SELECT DISTINCT area FROM jefaturas WHERE area != '' ORDER BY area"
    ).fetchall()]

    # Jefes filtrados por área
    if area:
        jefes_disp = db.execute(
            "SELECT usuario_id, nombre FROM jefaturas WHERE area = ? ORDER BY nombre", (area,)
        ).fetchall()
    else:
        jefes_disp = db.execute(
            "SELECT usuario_id, nombre FROM jefaturas ORDER BY nombre"
        ).fetchall()
    jefes_disp = [(r['usuario_id'], r['nombre']) for r in jefes_disp]

    # Construir filtros de fecha
    cw, cp = build_date_filter('c.fecha_jornada', anio, mes)

    # Condiciones extra (jefatura)
    extra_conditions = []
    extra_params = []
    if jefe:
        extra_conditions.append("c.usuario_id = ?")
        extra_params.append(jefe)
    elif area:
        extra_conditions.append("c.usuario_id IN (SELECT usuario_id FROM jefaturas WHERE area = ?)")
        extra_params.append(area)

    # Combinar WHERE
    where_parts = []
    all_params = []
    if cw:
        where_parts.append(cw.replace("WHERE ", ""))
        all_params.extend(cp)
    # Jugadores sin coin_in combinado (SRW + Mesas) en esa jornada
    where_parts.append("""(
        NOT EXISTS (
            SELECT 1 FROM srw_jugadores s
            WHERE s.player_id = c.cliente_id
              AND s.gaming_date = c.fecha_jornada
              AND s.coin_in > 0
        )
        AND NOT EXISTS (
            SELECT 1 FROM mesas_puntos m
            WHERE m.cliente_id = c.cliente_id
              AND m.fecha_operacion = c.fecha_jornada
              AND m.coin_in_puntos > 0
        )
    )""")
    if extra_conditions:
        where_parts.extend(extra_conditions)
        all_params.extend(extra_params)

    where_clause = "WHERE " + " AND ".join(where_parts)

    resultados = db.execute(f"""
        SELECT
            c.fecha_jornada as jornada,
            c.cliente_id,
            c.nombre_cliente,
            COALESCE(s.coin_in_dia, 0) + COALESCE(me.coin_in_mesas, 0) as coin_in,
            COUNT(c.id) as cant_cortesias,
            SUM(c.micros) as monto_cortesias,
            COALESCE(p.cant_premios, 0) as cant_premios,
            COALESCE(p.monto_premios, 0) as monto_premios,
            COALESCE(j.nombre, '') as jefe_nombre,
            COALESCE(j.area, '') as jefe_area
        FROM cortesias c
        LEFT JOIN (
            SELECT player_id, gaming_date, SUM(coin_in) as coin_in_dia
            FROM srw_jugadores GROUP BY player_id, gaming_date
        ) s ON c.cliente_id = s.player_id AND c.fecha_jornada = s.gaming_date
        LEFT JOIN (
            SELECT cliente_id, fecha_operacion, SUM(coin_in_puntos) as coin_in_mesas
            FROM mesas_puntos GROUP BY cliente_id, fecha_operacion
        ) me ON c.cliente_id = me.cliente_id AND c.fecha_jornada = me.fecha_operacion
        LEFT JOIN (
            SELECT cliente_id, fecha_jornada,
                   COUNT(*) as cant_premios,
                   SUM(transferencia_final) as monto_premios
            FROM premios GROUP BY cliente_id, fecha_jornada
        ) p ON c.cliente_id = p.cliente_id AND c.fecha_jornada = p.fecha_jornada
        LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
        {where_clause}
        GROUP BY c.fecha_jornada, c.cliente_id
        ORDER BY c.fecha_jornada DESC, monto_cortesias DESC
    """, all_params).fetchall()

    # Datos para gráfico de torta
    cw_chart = build_date_filter('c.fecha_jornada', anio, mes)
    chart_where = cw_chart[0] if cw_chart[0] else ""
    chart_params = list(cw_chart[1])
    # Solo casos coin_in cero (SRW + Mesas)
    coin_zero_cond = """(
        NOT EXISTS (
            SELECT 1 FROM srw_jugadores s
            WHERE s.player_id = c.cliente_id
              AND s.gaming_date = c.fecha_jornada
              AND s.coin_in > 0
        )
        AND NOT EXISTS (
            SELECT 1 FROM mesas_puntos m
            WHERE m.cliente_id = c.cliente_id
              AND m.fecha_operacion = c.fecha_jornada
              AND m.coin_in_puntos > 0
        )
    )"""
    if chart_where:
        chart_where = chart_where.replace("WHERE ", "WHERE " + coin_zero_cond + " AND ")
    else:
        chart_where = "WHERE " + coin_zero_cond

    if area:
        chart_where += " AND j.area = ?"
        chart_params.append(area)
        chart_rows = db.execute(f"""
            SELECT j.nombre as etiqueta, COUNT(*) as cantidad
            FROM cortesias c
            LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
            {chart_where}
            GROUP BY j.nombre
            ORDER BY cantidad DESC
        """, chart_params).fetchall()
        chart_titulo = f"Casos Coin In Cero por Jefe — {area}"
    else:
        chart_where += " AND j.area IS NOT NULL AND j.area != ''"
        chart_rows = db.execute(f"""
            SELECT j.area as etiqueta, COUNT(*) as cantidad
            FROM cortesias c
            LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
            {chart_where}
            GROUP BY j.area
            ORDER BY cantidad DESC
        """, chart_params).fetchall()
        chart_titulo = "Casos Coin In Cero por Sección"

    chart_labels = [r['etiqueta'] or 'Sin asignar' for r in chart_rows]
    chart_cantidades = [r['cantidad'] for r in chart_rows]

    return render_template('auditoria_coinin_cero.html',
                           resultados=resultados,
                           chart_labels=chart_labels,
                           chart_cantidades=chart_cantidades,
                           chart_titulo=chart_titulo,
                           anios=anios, meses_disp=meses_disp,
                           areas=areas, jefes_disp=jefes_disp,
                           anio_actual=anio, mes_actual=mes,
                           area_actual=area, jefe_actual=jefe)


# --------------- Exportar Reportes ---------------

MESES_NOMBRE = {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril','05':'Mayo','06':'Junio',
                '07':'Julio','08':'Agosto','09':'Septiembre','10':'Octubre','11':'Noviembre','12':'Diciembre'}


@app.route('/exportar')
def exportar_reportes():
    db = get_db()
    anios, meses_disp = get_anios_meses(db)
    return render_template('exportar.html', anios=anios, meses_disp=meses_disp)


@app.route('/exportar/generar', methods=['POST'])
def exportar_generar():
    db = get_db()
    anio = request.form.get('anio', '')
    mes = request.form.get('mes', '')
    secciones = request.form.getlist('secciones')

    if not secciones:
        flash('Selecciona al menos una sección.', 'error')
        return redirect(url_for('exportar_reportes'))

    output = BytesIO()
    periodo = f"{anio or 'Todos'}-{MESES_NOMBRE.get(mes, mes) if mes else 'Todos'}"

    with pd.ExcelWriter(output, engine='openpyxl') as writer:

        if 'cortesias' in secciones:
            cw, cp = build_date_filter('c.fecha_jornada', anio, mes)
            sw, sp = build_date_filter('gaming_date', anio, mes)
            mw_exp, mp_exp = build_date_filter('m.fecha_operacion', anio, mes)
            rows = db.execute(f"""
                SELECT c.cliente_id as ID, c.nombre_cliente as Nombre,
                       COUNT(c.id) as Cortesias, SUM(c.micros) as Monto_Cortesias,
                       COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0) as Coin_In,
                       COALESCE(s.player_level, '-') as Nivel,
                       CASE WHEN (COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0)) > 0
                            THEN ROUND(SUM(c.micros) * 100.0 / (COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0)), 4)
                            ELSE 0 END as Pct_Cortesia_CoinIn
                FROM cortesias c
                LEFT JOIN (
                    SELECT player_id, SUM(coin_in) as total_coin_in,
                           MAX(player_level) as player_level
                    FROM srw_jugadores {sw} GROUP BY player_id
                ) s ON c.cliente_id = s.player_id
                LEFT JOIN (
                    SELECT cliente_id, SUM(coin_in_puntos) as coin_in_mesas
                    FROM mesas_puntos m {mw_exp} GROUP BY cliente_id
                ) m ON c.cliente_id = m.cliente_id
                {cw}
                GROUP BY c.cliente_id, c.nombre_cliente
                ORDER BY Monto_Cortesias DESC
            """, sp + mp_exp + cp).fetchall()
            df = pd.DataFrame([dict(r) for r in rows])
            if not df.empty:
                df.to_excel(writer, sheet_name='Cortesias', index=False)

        if 'premios' in secciones:
            pw, pp = build_date_filter('p.fecha_jornada', anio, mes)
            sw, sp = build_date_filter('gaming_date', anio, mes)
            mw_exp, mp_exp = build_date_filter('m.fecha_operacion', anio, mes)
            rows = db.execute(f"""
                SELECT p.cliente_id as ID,
                       COALESCE(s.full_name, m.cliente_nombre, '(Sin nombre)') as Nombre,
                       COALESCE(s.player_level, '-') as Nivel,
                       COUNT(p.id) as Total_Premios,
                       SUM(p.transferencia_final) as Monto_Premios,
                       COALESCE(s.total_coin_in, 0) + COALESCE(m.coin_in_mesas, 0) as Coin_In
                FROM premios p
                LEFT JOIN (
                    SELECT player_id, MAX(full_name) as full_name,
                           MAX(player_level) as player_level,
                           SUM(coin_in) as total_coin_in
                    FROM srw_jugadores {sw} GROUP BY player_id
                ) s ON p.cliente_id = s.player_id
                LEFT JOIN (
                    SELECT cliente_id, MAX(cliente_nombre) as cliente_nombre,
                           SUM(coin_in_puntos) as coin_in_mesas
                    FROM mesas_puntos m {mw_exp} GROUP BY cliente_id
                ) m ON p.cliente_id = m.cliente_id
                {pw}
                GROUP BY p.cliente_id
                ORDER BY Monto_Premios DESC
            """, sp + mp_exp + pp).fetchall()
            df = pd.DataFrame([dict(r) for r in rows])
            if not df.empty:
                df.to_excel(writer, sheet_name='Premios', index=False)

        if 'resumen' in secciones:
            sw, sp = build_date_filter('gaming_date', anio, mes)
            cw, cparam = build_date_filter('fecha_jornada', anio, mes)
            pw, pparam = build_date_filter('fecha_jornada', anio, mes)
            mw_res, mp_res = build_date_filter('m.fecha_operacion', anio, mes)
            rows = db.execute(f"""
                SELECT s.player_id as ID, s.full_name as Nombre, s.player_level as Nivel,
                       s.total_coin_in + COALESCE(m.coin_in_mesas, 0) as Coin_In,
                       s.total_promo_in as Promo_In,
                       s.total_games as Juegos, s.dias_jugados as Dias,
                       COALESCE(c.total_cortesias, 0) as Cortesias,
                       COALESCE(c.monto_cortesias, 0) as Monto_Cortesias,
                       COALESCE(p.total_premios, 0) as Premios,
                       COALESCE(p.monto_premios, 0) as Monto_Premios,
                       CASE WHEN (s.total_coin_in + COALESCE(m.coin_in_mesas, 0)) > 0
                            THEN ROUND((COALESCE(c.monto_cortesias,0) + COALESCE(p.monto_premios,0)) * 100.0 / (s.total_coin_in + COALESCE(m.coin_in_mesas, 0)), 4)
                            ELSE 0 END as Pct_Total_CoinIn
                FROM (
                    SELECT player_id, MAX(full_name) as full_name, MAX(player_level) as player_level,
                           SUM(coin_in) as total_coin_in, SUM(promo_in) as total_promo_in,
                           SUM(total_games) as total_games, COUNT(DISTINCT gaming_date) as dias_jugados
                    FROM srw_jugadores {sw} GROUP BY player_id
                ) s
                LEFT JOIN (
                    SELECT cliente_id, COUNT(*) as total_cortesias, SUM(micros) as monto_cortesias
                    FROM cortesias {cw} GROUP BY cliente_id
                ) c ON s.player_id = c.cliente_id
                LEFT JOIN (
                    SELECT cliente_id, COUNT(*) as total_premios, SUM(transferencia_final) as monto_premios
                    FROM premios {pw} GROUP BY cliente_id
                ) p ON s.player_id = p.cliente_id
                LEFT JOIN (
                    SELECT cliente_id, SUM(coin_in_puntos) as coin_in_mesas
                    FROM mesas_puntos m {mw_res} GROUP BY cliente_id
                ) m ON s.player_id = m.cliente_id
                WHERE COALESCE(c.total_cortesias, 0) > 0 OR COALESCE(p.total_premios, 0) > 0
                ORDER BY Coin_In DESC
            """, sp + cparam + pparam + mp_res).fetchall()
            df = pd.DataFrame([dict(r) for r in rows])
            if not df.empty:
                df.to_excel(writer, sheet_name='Resumen', index=False)

        if 'control_invitaciones' in secciones:
            sw, sp = build_date_filter('s.gaming_date', anio, mes)
            cw, cparam = build_date_filter('c.fecha_jornada', anio, mes)
            ctw, ctparam = build_date_filter('ct.fecha_jornada', anio, mes)
            pw, pparam = build_date_filter('p.fecha_jornada', anio, mes)
            sw_solo, sp_solo = build_date_filter('gaming_date', anio, mes)
            mw_solo_exp, mp_solo_exp = build_date_filter('fecha_operacion', anio, mes)
            mw_exp, mp_exp = build_date_filter('m.fecha_operacion', anio, mes)
            mw_exp_inner, mp_exp_inner = build_date_filter('mp.fecha_operacion', anio, mes)

            dias_t = db.execute(f"""
                SELECT COUNT(DISTINCT fecha) as d FROM (
                    SELECT gaming_date as fecha FROM srw_jugadores {sw_solo}
                    UNION
                    SELECT fecha_operacion as fecha FROM mesas_puntos {mw_solo_exp}
                )
            """, sp_solo + mp_solo_exp).fetchone()['d'] or 1
            prim = db.execute("SELECT porcentaje FROM categorias_nivel WHERE categoria = 'Primario'").fetchone()
            pct_prim = prim['porcentaje'] if prim else 0
            cat_map = {r['categoria']: r['porcentaje'] for r in db.execute("SELECT categoria, porcentaje FROM categorias_nivel WHERE categoria != 'Primario'").fetchall()}

            # Jugadores SRW agrupados por cliente + jefatura
            sw_plain, sp_plain = build_date_filter('gaming_date', anio, mes)
            mw_plain, mp_plain = build_date_filter('fecha_operacion', anio, mes)
            rows_srw = db.execute(f"""
                SELECT s.player_id, MAX(s.full_name) as nombre, MAX(s.player_level) as nivel,
                       SUM(s.coin_in) + COALESCE(MAX(m.coin_in_mesas), 0) as coin_in,
                       COALESCE(d.dias_combinados, COUNT(DISTINCT s.gaming_date)) as dias,
                       COALESCE(c.monto_micros, 0) as micros,
                       COALESCE(c.total_cortesias, 0) as cortesias,
                       COALESCE(p.cant_premios, 0) as premios,
                       COALESCE(p.monto_premios, 0) as monto_premios,
                       COALESCE(c.jefe_nombre, '') as jefe,
                       COALESCE(c.jefe_area, '') as area
                FROM srw_jugadores s
                LEFT JOIN (
                    SELECT ct.cliente_id, ct.usuario_id,
                           COUNT(*) as total_cortesias,
                           SUM(ct.micros) as monto_micros,
                           COALESCE(j.nombre, '') as jefe_nombre,
                           COALESCE(j.area, '') as jefe_area
                    FROM cortesias ct
                    LEFT JOIN jefaturas j ON ct.usuario_id = j.usuario_id
                    {ctw}
                    GROUP BY ct.cliente_id, ct.usuario_id
                ) c ON s.player_id = c.cliente_id
                LEFT JOIN (SELECT cliente_id, COUNT(*) as cant_premios, SUM(transferencia_final) as monto_premios FROM premios p {pw} GROUP BY cliente_id) p ON s.player_id = p.cliente_id
                LEFT JOIN (SELECT cliente_id, SUM(coin_in_puntos) as coin_in_mesas FROM mesas_puntos m {mw_exp} GROUP BY cliente_id) m ON s.player_id = m.cliente_id
                LEFT JOIN (
                    SELECT cliente_id, COUNT(DISTINCT fecha) as dias_combinados FROM (
                        SELECT player_id as cliente_id, gaming_date as fecha FROM srw_jugadores {sw_plain}
                        UNION
                        SELECT cliente_id, fecha_operacion as fecha FROM mesas_puntos {mw_plain}
                    ) GROUP BY cliente_id
                ) d ON s.player_id = d.cliente_id
                {sw} GROUP BY s.player_id, c.usuario_id HAVING COALESCE(c.total_cortesias, 0) > 0
                ORDER BY coin_in DESC
            """, ctparam + pparam + mp_exp + sp_plain + mp_plain + sp).fetchall()

            # Jugadores solo-mesas (no en SRW) agrupados por cliente + jefatura
            sw_excl, sp_excl = build_date_filter('gaming_date', anio, mes)
            mesas_excl = f"mp.cliente_id NOT IN (SELECT DISTINCT player_id FROM srw_jugadores {sw_excl})"
            if mp_exp_inner:
                mw_conds = []
                if anio:
                    mw_conds.append("SUBSTR(mp.fecha_operacion, 1, 4) = ?")
                if mes:
                    mw_conds.append("SUBSTR(mp.fecha_operacion, 6, 2) = ?")
                mesas_where = "WHERE " + " AND ".join(mw_conds) + " AND " + mesas_excl
            else:
                mesas_where = "WHERE " + mesas_excl

            rows_mesas = db.execute(f"""
                SELECT mp.cliente_id as player_id, MAX(mp.cliente_nombre) as nombre,
                       'MDJ' as nivel,
                       SUM(mp.coin_in_puntos) as coin_in,
                       COUNT(DISTINCT mp.fecha_operacion) as dias,
                       COALESCE(c.monto_micros, 0) as micros,
                       COALESCE(c.total_cortesias, 0) as cortesias,
                       COALESCE(p.cant_premios, 0) as premios,
                       COALESCE(p.monto_premios, 0) as monto_premios,
                       COALESCE(c.jefe_nombre, '') as jefe,
                       COALESCE(c.jefe_area, '') as area
                FROM mesas_puntos mp
                LEFT JOIN (
                    SELECT ct.cliente_id, ct.usuario_id,
                           COUNT(*) as total_cortesias,
                           SUM(ct.micros) as monto_micros,
                           COALESCE(j.nombre, '') as jefe_nombre,
                           COALESCE(j.area, '') as jefe_area
                    FROM cortesias ct
                    LEFT JOIN jefaturas j ON ct.usuario_id = j.usuario_id
                    {ctw}
                    GROUP BY ct.cliente_id, ct.usuario_id
                ) c ON mp.cliente_id = c.cliente_id
                LEFT JOIN (SELECT cliente_id, COUNT(*) as cant_premios, SUM(transferencia_final) as monto_premios FROM premios p {pw} GROUP BY cliente_id) p ON mp.cliente_id = p.cliente_id
                {mesas_where}
                GROUP BY mp.cliente_id, c.usuario_id HAVING COALESCE(c.total_cortesias, 0) > 0
                ORDER BY coin_in DESC
            """, ctparam + pparam + mp_exp_inner + sp_excl).fetchall()

            all_rows = list(rows_srw) + list(rows_mesas)
            data = []
            for r in all_rows:
                ci = r['coin_in'] or 0
                pc = cat_map.get(r['nivel'] or '', 0)
                inv = ci * pct_prim * pc
                mic = r['micros'] or 0
                data.append({
                    'Nombre': r['nombre'], 'Nivel': r['nivel'],
                    'Dias': r['dias'], 'Pct_Asistencia': round(r['dias'] * 100.0 / dias_t, 1),
                    'Premios': r['premios'], 'Monto_Premios': r['monto_premios'] or 0,
                    'Coin_In': ci, 'Invitacion_Max': round(inv),
                    'Cortesias_Cant': r['cortesias'],
                    'Cortesias_Monto': mic, 'Saldo': round(inv - mic),
                    'Area': r['area'], 'Autorizo': r['jefe']
                })
            df = pd.DataFrame(data)
            if not df.empty:
                df.to_excel(writer, sheet_name='Control Invitaciones', index=False)

        if 'auditoria_coinin_cero' in secciones:
            cw, cp = build_date_filter('c.fecha_jornada', anio, mes)
            where_parts = []
            all_params = []
            if cw:
                where_parts.append(cw.replace('WHERE ', ''))
                all_params.extend(cp)
            where_parts.append("""(
                NOT EXISTS (SELECT 1 FROM srw_jugadores s WHERE s.player_id = c.cliente_id AND s.gaming_date = c.fecha_jornada AND s.coin_in > 0)
                AND NOT EXISTS (SELECT 1 FROM mesas_puntos m WHERE m.cliente_id = c.cliente_id AND m.fecha_operacion = c.fecha_jornada AND m.coin_in_puntos > 0)
            )""")
            wc = 'WHERE ' + ' AND '.join(where_parts)
            rows = db.execute(f"""
                SELECT c.fecha_jornada as Jornada, c.cliente_id as ID, c.nombre_cliente as Nombre,
                       COALESCE(sv.coin_in_dia, 0) + COALESCE(me.coin_in_mesas, 0) as Coin_In,
                       COUNT(c.id) as Cortesias, SUM(c.micros) as Monto_Cortesias,
                       COALESCE(p.cant_premios, 0) as Premios,
                       COALESCE(p.monto_premios, 0) as Monto_Premios,
                       COALESCE(j.area, '') as Area, COALESCE(j.nombre, '') as Autorizo
                FROM cortesias c
                LEFT JOIN (SELECT player_id, gaming_date, SUM(coin_in) as coin_in_dia FROM srw_jugadores GROUP BY player_id, gaming_date) sv ON c.cliente_id = sv.player_id AND c.fecha_jornada = sv.gaming_date
                LEFT JOIN (SELECT cliente_id, fecha_operacion, SUM(coin_in_puntos) as coin_in_mesas FROM mesas_puntos GROUP BY cliente_id, fecha_operacion) me ON c.cliente_id = me.cliente_id AND c.fecha_jornada = me.fecha_operacion
                LEFT JOIN (SELECT cliente_id, fecha_jornada, COUNT(*) as cant_premios, SUM(transferencia_final) as monto_premios FROM premios GROUP BY cliente_id, fecha_jornada) p ON c.cliente_id = p.cliente_id AND c.fecha_jornada = p.fecha_jornada
                LEFT JOIN jefaturas j ON c.usuario_id = j.usuario_id
                {wc}
                GROUP BY c.fecha_jornada, c.cliente_id, c.usuario_id
                ORDER BY c.fecha_jornada DESC
            """, all_params).fetchall()
            df = pd.DataFrame([dict(r) for r in rows])
            if not df.empty:
                df.to_excel(writer, sheet_name='Auditoria CoinIn Cero', index=False)

    output.seek(0)
    filename = f"Reporte_COMPS_{periodo}.xlsx"
    return send_file(output, download_name=filename,
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# --------------- Init ---------------

init_db()
cargar_jefaturas()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
