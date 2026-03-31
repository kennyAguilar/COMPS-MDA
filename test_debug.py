import pandas as pd

df = pd.read_excel('d:/COMPS MDA/uploads/RrtIformeGeneral.xlsx', header=None, skiprows=8)
print(f'Filas leidas: {len(df)}')
print(f'Columnas: {len(df.columns)}')
print()

# Check col 22 values
print('Col 22 (estado) sample:')
print(df[22].value_counts())
print()

# Check col 6 (fecha_jornada)
print('Col 6 sample:')
print(df[6].head(3))
print()

# Check col 7 (cliente_id)
print('Col 7 sample:')
print(df[7].head(3))
print()

# Try the full ETL
df2 = df.rename(columns={
    6: 'fecha_jornada', 7: 'cliente_id', 10: 'nombre_cliente',
    14: 'descripcion_cat', 16: 'descripcion_prod', 19: 'micros',
    22: 'estado', 28: 'usuario_id', 29: 'nombre_usuario'
})
cols = ['fecha_jornada', 'cliente_id', 'nombre_cliente',
        'descripcion_cat', 'descripcion_prod', 'micros',
        'estado', 'usuario_id', 'nombre_usuario']
df2 = df2[cols]
print(f'Antes filtro: {len(df2)}')
print(f'Estado QUEMADO: {(df2["estado"] == "QUEMADO").sum()}')
df2 = df2[df2['estado'] == 'QUEMADO']
print(f'Despues filtro: {len(df2)}')
df2 = df2.dropna(subset=['cliente_id'])
print(f'Despues dropna: {len(df2)}')
print()
print('Muestra resultado:')
