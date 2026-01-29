"""
Script para convertir archivos Excel (.xlsx) a CSV sin perder datos.
"""
import pandas as pd
from pathlib import Path

# Archivos a convertir
archivos_excel = ['lote2.xlsx', 'lote3.xlsx', 'lote4.xlsx']

for archivo in archivos_excel:
    excel_path = Path(archivo)

    if not excel_path.exists():
        print(f"[X] No encontrado: {archivo}")
        continue

    # Leer Excel (todas las hojas, pero tomamos la primera)
    df = pd.read_excel(excel_path, sheet_name=0)

    # Nombre del archivo CSV de salida
    csv_path = excel_path.with_suffix('.csv')

    # Guardar como CSV con encoding UTF-8 y delimitador punto y coma
    df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')

    print(f"[OK] {archivo} -> {csv_path.name}")
    print(f"  Filas: {len(df)}, Columnas: {len(df.columns)}")
    print(f"  Columnas: {', '.join(df.columns.tolist())}")
    print()

print("Conversión completada!")
