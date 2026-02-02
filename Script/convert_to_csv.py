"""
Script para convertir archivos Excel (.xlsx) u OpenDocument (.ods) a CSV sin perder datos.

Uso:
    python convert_to_csv.py archivo.xlsx
    python convert_to_csv.py archivo.ods
"""
import sys
import pandas as pd
from pathlib import Path


def convertir_a_csv(archivo):
    """Convierte un archivo XLSX o ODS a CSV."""
    archivo_path = Path(archivo)

    if not archivo_path.exists():
        print(f"[X] Error: No se encontró el archivo '{archivo}'")
        return False

    # Verificar extensión
    extension = archivo_path.suffix.lower()
    if extension not in ['.xlsx', '.ods']:
        print(f"[X] Error: Formato no soportado '{extension}'. Use .xlsx o .ods")
        return False

    try:
        # Leer el archivo según su extensión
        if extension == '.xlsx':
            df = pd.read_excel(archivo_path, sheet_name=0, engine='openpyxl')
        elif extension == '.ods':
            df = pd.read_excel(archivo_path, sheet_name=0, engine='odf')

        # Nombre del archivo CSV de salida
        csv_path = archivo_path.with_suffix('.csv')

        # Guardar como CSV con encoding UTF-8 y delimitador punto y coma
        df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')

        print(f"[OK] {archivo} -> {csv_path.name}")
        print(f"  Filas: {len(df)}, Columnas: {len(df.columns)}")
        print(f"  Columnas: {', '.join(df.columns.tolist())}")
        return True

    except Exception as e:
        print(f"[X] Error al convertir '{archivo}': {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python convert_to_csv.py <archivo.xlsx|archivo.ods>")
        print("\nEjemplos:")
        print("  python convert_to_csv.py lote1.xlsx")
        print("  python convert_to_csv.py lote5.ods")
        sys.exit(1)

    # Convertir cada archivo pasado como argumento
    archivos = sys.argv[1:]
    exitosos = 0
    fallidos = 0

    for archivo in archivos:
        if convertir_a_csv(archivo):
            exitosos += 1
        else:
            fallidos += 1
        print()

    print(f"Conversión completada: {exitosos} exitosos, {fallidos} fallidos")
