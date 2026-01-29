# -*- coding: utf-8 -*-
"""Camino Lote Masivo - Extraccion de direcciones por DNI.

Lee un CSV con DNIs y para cada uno:
1. Click en input de busqueda
2. Ctrl+A + Backspace para limpiar
3. Escribe el DNI
4. Enter para buscar
5. Click derecho en la primera cuenta
click izquiero en copiar 
ahi va a copiar el nombre de la persona, hay qeu validarlo que sea el mismo de ese dni, 
teniendo en cuenta la primer columna que dice nombre de cliente, no hace falta que 
sea igual sino que al menos coincida el nombre o apellido, pueden salir en distinto orden
en caso de que no copie nada, entonces ese numeor se descarta y se coloca como "no creado"
y se sigue con el siguiente numero
6. Click derecho > Seleccionar todo
7. Click derecho > Copiar (direccion)
8. Guarda la direccion en archivo de resultados
9. Click en cerrar
10. Repite

Guarda resultados exitosos y fallos en archivos separados.
"""
from __future__ import annotations
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Forzar UTF-8 en stdout/stderr para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import pyautogui as pg

# Intentar importar pyperclip para clipboard
try:
    import pyperclip
except ImportError:
    pyperclip = None


def get_clipboard() -> str:
    """Obtiene el contenido del portapapeles."""
    if pyperclip:
        try:
            return pyperclip.paste() or ''
        except Exception:
            pass
    # Fallback con tkinter
    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()
        try:
            content = root.clipboard_get()
        except tk.TclError:
            content = ''
        finally:
            root.destroy()
        return content or ''
    except Exception:
        return ''


def clear_clipboard():
    """Limpia el portapapeles."""
    if pyperclip:
        try:
            pyperclip.copy('')
            return
        except Exception:
            pass
    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()
        root.clipboard_clear()
        root.update()
        root.destroy()
    except Exception:
        pass


def load_coords(path: Path) -> dict:
    """Carga las coordenadas desde el JSON."""
    if not path.exists():
        print(f"ERROR: No existe el archivo de coordenadas: {path}")
        sys.exit(1)

    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def click(x: int, y: int, label: str, delay: float = 0.3):
    """Hace click en las coordenadas especificadas."""
    print(f"  Click en {label} ({x}, {y})")
    pg.moveTo(x, y, duration=0.1)
    pg.click()
    time.sleep(delay)


def right_click(x: int, y: int, label: str, delay: float = 0.3):
    """Hace click derecho en las coordenadas especificadas."""
    print(f"  Click derecho en {label} ({x}, {y})")
    pg.moveTo(x, y, duration=0.1)
    pg.rightClick()
    time.sleep(delay)


def type_text(text: str, delay: float = 0.3):
    """Escribe texto caracter por caracter."""
    print(f"  Escribiendo: {text}")
    for char in text:
        pg.typewrite(char, interval=0.05)
    time.sleep(delay)


def load_progress(progress_file: Path) -> set:
    """Carga los DNIs ya procesados."""
    if not progress_file.exists():
        return set()

    processed = set()
    with progress_file.open('r', encoding='utf-8') as f:
        for line in f:
            dni = line.strip().split('\t')[0]
            if dni:
                processed.add(dni)
    return processed


def save_result(results_file: Path, dni: str, direccion: str):
    """Guarda un resultado exitoso."""
    with results_file.open('a', encoding='utf-8') as f:
        # Limpiar la direccion de saltos de linea
        direccion_clean = direccion.replace('\n', ' ').replace('\r', ' ').strip()
        f.write(f"{dni}\t{direccion_clean}\n")


def save_failure(failures_file: Path, dni: str, reason: str):
    """Guarda un fallo."""
    with failures_file.open('a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{dni}\t{reason}\t{timestamp}\n")


def process_dni(dni: str, coords: dict, results_file: Path, failures_file: Path) -> bool:
    """Procesa un DNI individual. Retorna True si fue exitoso."""
    print(f"\n{'='*50}")
    print(f"Procesando DNI: {dni}")
    print(f"{'='*50}")

    try:
        # Paso 1: Click en input de DNI
        input_coords = coords.get('dni_input', {})
        click(input_coords['x'], input_coords['y'], 'Input DNI', 0.3)

        # Paso 2: Ctrl+A para seleccionar todo
        print("  Ctrl+A (seleccionar todo)")
        pg.hotkey('ctrl', 'a')
        time.sleep(0.2)

        # Paso 3: Backspace para borrar
        print("  Backspace (borrar)")
        pg.press('backspace')
        time.sleep(0.2)

        # Paso 4: Escribir DNI
        type_text(dni, 0.3)

        # Paso 5: Enter para buscar
        print("  Enter (buscar)")
        pg.press('enter')
        time.sleep(2.0)  # Esperar a que cargue la busqueda

        # Paso 6: Click en primera cuenta
        first_result = coords.get('first_result', {})
        click(first_result['x'], first_result['y'], 'Primera cuenta', 1.5)  # Esperar a que cargue

        # Paso 7: Click derecho para menu contextual (seleccionar todo)
        right_click_addr = coords.get('right_click_address', {})
        right_click(right_click_addr['x'], right_click_addr['y'], 'Menu contextual', 0.5)

        # Paso 8: Click en "Seleccionar todo"
        select_all_menu = coords.get('select_all_menu', {})
        click(select_all_menu['x'], select_all_menu['y'], 'Seleccionar todo', 0.5)

        # Paso 9: Click derecho para menu de copiar
        right_click_copy = coords.get('right_click_copy', {})
        right_click(right_click_copy['x'], right_click_copy['y'], 'Menu copiar', 0.5)

        # Paso 10: Click en "Copiar"
        clear_clipboard()
        copy_menu = coords.get('copy_menu', {})
        click(copy_menu['x'], copy_menu['y'], 'Copiar', 0.5)

        # Obtener contenido copiado
        time.sleep(0.3)
        direccion = get_clipboard()

        if not direccion.strip():
            print(f"  ADVERTENCIA: No se copio ninguna direccion")
            save_failure(failures_file, dni, "Sin direccion copiada")
        else:
            print(f"  Direccion copiada: {direccion[:100]}...")
            save_result(results_file, dni, direccion)

        # Paso 11: Cerrar ventana
        close_btn = coords.get('close_btn', {})
        click(close_btn['x'], close_btn['y'], 'Cerrar', 0.5)

        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        save_failure(failures_file, dni, str(e))

        # Intentar cerrar la ventana de todos modos
        try:
            close_btn = coords.get('close_btn', {})
            if close_btn.get('x') and close_btn.get('y'):
                click(close_btn['x'], close_btn['y'], 'Cerrar (recovery)', 0.5)
        except Exception:
            pass

        return False


def run(csv_path: Path, coords_path: Path, output_dir: Optional[Path] = None, start_delay: float = 3.0):
    """Ejecuta el proceso masivo."""
    pg.FAILSAFE = True  # Mover mouse a esquina superior izquierda para parar

    # Cargar coordenadas
    coords = load_coords(coords_path)

    # Configurar archivos de salida
    if output_dir is None:
        output_dir = Path('.')
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = output_dir / f'resultados_{timestamp}.tsv'
    failures_file = output_dir / f'fallos_{timestamp}.tsv'

    print(f"Archivo de resultados: {results_file}")
    print(f"Archivo de fallos: {failures_file}")

    # Cargar DNIs ya procesados (por si se retoma)
    processed = load_progress(results_file)
    print(f"DNIs ya procesados: {len(processed)}")

    # Leer CSV
    if not csv_path.exists():
        print(f"ERROR: No existe el archivo CSV: {csv_path}")
        sys.exit(1)

    dnis = []
    with csv_path.open('r', encoding='utf-8', errors='ignore') as f:
        # Detectar delimitador
        sample = f.read(2048)
        f.seek(0)
        delimiter = ';' if sample.count(';') > sample.count(',') else ','

        reader = csv.DictReader(f, delimiter=delimiter)

        # Buscar columna de DNI (puede llamarse DNI, dni, Dni, documento, etc.)
        dni_col = None
        if reader.fieldnames:
            for col in reader.fieldnames:
                if col.lower() in ['dni', 'documento', 'doc', 'nro_documento']:
                    dni_col = col
                    break

        if not dni_col:
            print(f"ERROR: No se encontro columna de DNI en el CSV")
            print(f"Columnas disponibles: {reader.fieldnames}")
            sys.exit(1)

        print(f"Usando columna: {dni_col}")

        for row in reader:
            dni = row.get(dni_col, '').strip()
            if dni and dni not in processed:
                dnis.append(dni)

    total = len(dnis)
    print(f"\nTotal DNIs a procesar: {total}")

    if total == 0:
        print("No hay DNIs nuevos para procesar.")
        return

    # Countdown antes de empezar
    print(f"\nIniciando en {start_delay} segundos...")
    print("(Mueve el mouse a la esquina superior izquierda para cancelar)")
    time.sleep(start_delay)

    # Procesar cada DNI
    exitosos = 0
    fallidos = 0

    for i, dni in enumerate(dnis, start=1):
        print(f"\n[{i}/{total}] ({exitosos} exitosos, {fallidos} fallidos)")

        if process_dni(dni, coords, results_file, failures_file):
            exitosos += 1
        else:
            fallidos += 1

        # Pequena pausa entre DNIs
        time.sleep(0.5)

    # Resumen final
    print(f"\n{'='*50}")
    print("RESUMEN FINAL")
    print(f"{'='*50}")
    print(f"Total procesados: {exitosos + fallidos}")
    print(f"Exitosos: {exitosos}")
    print(f"Fallidos: {fallidos}")
    print(f"Resultados en: {results_file}")
    print(f"Fallos en: {failures_file}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Extraccion masiva de direcciones por DNI')
    parser.add_argument('--csv', required=True, help='Archivo CSV con los DNIs')
    parser.add_argument('--coords', default='camino-lote-masivo.json', help='Archivo JSON con coordenadas')
    parser.add_argument('--output-dir', default='.', help='Directorio para archivos de salida')
    parser.add_argument('--start-delay', type=float, default=3.0, help='Segundos de espera antes de empezar')

    args = parser.parse_args()

    try:
        run(
            csv_path=Path(args.csv),
            coords_path=Path(args.coords),
            output_dir=Path(args.output_dir),
            start_delay=args.start_delay
        )
    except KeyboardInterrupt:
        print("\n\nInterrumpido por usuario")
        sys.exit(130)
