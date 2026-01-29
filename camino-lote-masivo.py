# -*- coding: utf-8 -*-
"""Camino Lote Masivo - Extraccion de direcciones por DNI.

Lee un CSV con DNIs y para cada uno:
1. Click en input de busqueda
2. Ctrl+A + Backspace para limpiar
3. Escribe el DNI
4. Enter para buscar
5. Click derecho en la primera cuenta
6. Click izquierdo en "Copiar" para copiar el nombre
7. Validar que el nombre copiado coincida con el del CSV (al menos un nombre/apellido)
   - Si no coincide o no se copia nada: marcar como "no creado" y continuar
8. Click izquierdo en primera cuenta para abrir detalle
9. Click derecho > Seleccionar todo
10. Click derecho > Copiar (direccion)
11. Si no se copia la direccion (cartel de error):
    - Click en reconnect_click + Enter para cerrar cartel
    - Reintentar: Click derecho > Seleccionar todo > Click derecho > Copiar
12. Guarda la direccion en archivo de resultados
13. Click en cerrar
14. Repite

Manejo de errores:
- Si 3+ "no creado" consecutivos:
  * Si VPN caida: Detiene scraping, espera reconexion, limpia popups, reintenta DNIs
  * Si VPN OK: Verifica popup bloqueante
    - Si bloqueado: 4x close button + recovery click para desbloquearlo

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

# Desactivar buffering para ver output en tiempo real
import functools
print = functools.partial(print, flush=True)

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


def normalize_name(name: str) -> set:
    """Normaliza un nombre y retorna un set de palabras."""
    import unicodedata
    # Quitar acentos
    normalized = unicodedata.normalize('NFD', name)
    normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    # Convertir a mayusculas y dividir en palabras
    words = normalized.upper().split()
    # Filtrar palabras muy cortas (articulos, etc.)
    return {w for w in words if len(w) > 2}


def names_match(csv_name: str, copied_name: str) -> bool:
    """Verifica si al menos un nombre/apellido coincide entre ambos nombres."""
    csv_words = normalize_name(csv_name)
    copied_words = normalize_name(copied_name)
    # Verificar si hay al menos una palabra en comun
    common = csv_words & copied_words
    return len(common) > 0


VPN_HOST = "10.167.205.151"
VPN_CHECK_INTERVAL = 10  # segundos entre cada ping cuando la VPN esta caida


def check_vpn() -> bool:
    """Verifica si la VPN esta activa haciendo ping al host."""
    import subprocess
    try:
        # En Windows usamos -n 1 para un solo ping, -w 1000 para timeout de 1 segundo
        result = subprocess.run(
            ['ping', '-n', '1', '-w', '1000', VPN_HOST],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def wait_for_vpn(vpn_log_file: Path) -> dict:
    """Espera hasta que la VPN vuelva a estar disponible.

    Retorna un dict con info del evento para logging.
    """
    from datetime import datetime

    start_time = datetime.now()
    print(f"\n{'!'*50}")
    print(f"[{start_time.strftime('%H:%M:%S')}] VPN CAIDA DETECTADA")
    print(f"{'!'*50}")
    print(f"ATENCION: SCRAPING DETENIDO - Esperando reconexion VPN ({VPN_HOST})...")
    print(f"No se procesaran mas DNIs hasta que la VPN vuelva")

    # Log en tiempo real
    with vpn_log_file.open('a', encoding='utf-8') as f:
        f.write(f"\n[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] VPN CAIDA - Scraping detenido\n")

    ping_attempts = 0
    while not check_vpn():
        ping_attempts += 1
        now = datetime.now()
        msg = f"  [{now.strftime('%H:%M:%S')}] Ping #{ping_attempts} fallido. Reintentando en {VPN_CHECK_INTERVAL}s..."
        print(msg)

        # Log cada ping
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Ping #{ping_attempts} - FALLO\n")

        time.sleep(VPN_CHECK_INTERVAL)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n[{end_time.strftime('%H:%M:%S')}] VPN RECONECTADA!")
    print(f"  Tiempo caida: {int(duration // 60)}m {int(duration % 60)}s")
    print(f"  Pings realizados: {ping_attempts}")

    # Log reconexion inicial
    with vpn_log_file.open('a', encoding='utf-8') as f:
        f.write(f"[{end_time.strftime('%Y-%m-%d %H:%M:%S')}] VPN RECONECTADA - Duracion: {int(duration // 60)}m {int(duration % 60)}s - Pings: {ping_attempts}\n")

    # Validar estabilidad de la conexion (3 pings adicionales)
    print(f"\nValidando estabilidad de la conexion...")
    stability_checks = 3
    stability_ok = True

    for i in range(stability_checks):
        time.sleep(2)  # Esperar 2 segundos entre pings de validacion
        if check_vpn():
            print(f"  Ping de estabilidad {i+1}/{stability_checks}: OK")
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ping estabilidad {i+1}/{stability_checks}: OK\n")
        else:
            print(f"  Ping de estabilidad {i+1}/{stability_checks}: FALLO - VPN aun inestable")
            stability_ok = False
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ping estabilidad {i+1}/{stability_checks}: FALLO\n")
            break

    if stability_ok:
        print(f"  Conexion VPN ESTABLE")
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conexion ESTABLE - Scraping reanudado\n")
        print(f"SCRAPING SE REANUDARA en 3 segundos...")
        time.sleep(3)
    else:
        print(f"  Conexion inestable, esperando 10s y revalidando...")
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conexion inestable - Esperando\n")
        time.sleep(10)
        # Reintentar validacion recursivamente
        return wait_for_vpn(vpn_log_file)

    return {
        'start': start_time,
        'end': end_time,
        'duration_seconds': duration,
        'ping_attempts': ping_attempts
    }


def reconnect_click_action(coords: dict, vpn_log_file: Path = None):
    """Realiza el click de reconexion y presiona Enter.

    Este click se hace despues de que la VPN vuelve para activar
    el sistema antes de limpiar popups.
    """
    reconnect = coords.get('reconnect_click', {})

    if not reconnect.get('x') or not reconnect.get('y'):
        print("  ADVERTENCIA: Coordenadas de reconnect_click no configuradas, saltando...")
        if vpn_log_file:
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ADVERTENCIA: Coordenadas reconnect_click no configuradas\n")
        return

    print(f"\n  Realizando click de reconexion en ({reconnect['x']}, {reconnect['y']})...")
    if vpn_log_file:
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Click reconexion ({reconnect['x']}, {reconnect['y']}) + Enter\n")

    pg.moveTo(reconnect['x'], reconnect['y'], duration=0.1)
    pg.click()
    time.sleep(0.5)

    print("  Presionando Enter...")
    pg.press('enter')
    time.sleep(1.0)

    print("  Click de reconexion completado")


def clear_vpn_popup(coords: dict, vpn_log_file: Path = None):
    """Limpia los popups que aparecen cuando la VPN se desconecta.

    Intenta copiar 'Búsqueda no guardada' del menu contextual.
    Si no lo logra, presiona Enter y reintenta.
    """
    print("\n  Limpiando posibles popups de VPN...")

    popup_right = coords.get('popup_right_click', {})
    popup_copy = coords.get('popup_copy_menu', {})

    if not popup_right.get('x') or not popup_copy.get('x'):
        print("  ADVERTENCIA: Coordenadas de popup no configuradas, saltando limpieza")
        if vpn_log_file:
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ADVERTENCIA: Coordenadas popup no configuradas\n")
        return

    max_attempts = 5
    expected_text = "Búsqueda no guardada"

    for attempt in range(max_attempts):
        print(f"  Intento {attempt + 1}/{max_attempts} de limpiar popup...")

        # Limpiar clipboard
        clear_clipboard()
        time.sleep(0.2)

        # Click derecho en el area del popup
        pg.moveTo(popup_right['x'], popup_right['y'], duration=0.1)
        pg.rightClick()
        time.sleep(0.5)

        # Click en copiar
        pg.moveTo(popup_copy['x'], popup_copy['y'], duration=0.1)
        pg.click()
        time.sleep(0.3)

        # Verificar si se copio el texto esperado
        copied = get_clipboard()

        if expected_text in copied:
            print(f"  Popup limpiado correctamente (copiado: '{copied[:30]}...')")
            if vpn_log_file:
                with vpn_log_file.open('a', encoding='utf-8') as f:
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Popup limpiado (intento {attempt + 1})\n")
            return

        # Si no se copio, presionar Enter para cerrar cualquier dialogo
        print(f"  No se detecto popup, presionando Enter...")
        pg.press('enter')
        time.sleep(0.5)
        pg.press('enter')
        time.sleep(0.5)

    print("  No se pudo confirmar limpieza de popup, continuando de todos modos...")
    if vpn_log_file:
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Popup no confirmado tras {max_attempts} intentos\n")


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


def process_dni(dni: str, nombre_csv: str, coords: dict, results_file: Path, failures_file: Path) -> str:
    """Procesa un DNI individual. Retorna 'ok', 'vpn_issue' o 'error'."""
    print(f"\n{'='*50}")
    print(f"Procesando DNI: {dni}")
    print(f"Nombre esperado: {nombre_csv}")
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

        # Paso 6: Click derecho en primera cuenta para copiar nombre
        clear_clipboard()
        first_result = coords.get('first_result', {})
        right_click(first_result['x'], first_result['y'], 'Primera cuenta (menu)', 0.5)

        # Paso 7: Click en "Copiar" para obtener el nombre
        copy_name_menu = coords.get('copy_name_menu', {})
        click(copy_name_menu['x'], copy_name_menu['y'], 'Copiar nombre', 0.5)

        # Obtener nombre copiado y validar
        time.sleep(0.3)
        nombre_copiado = get_clipboard()

        if not nombre_copiado.strip():
            print(f"  ERROR: No se copio ningun nombre - marcando como 'no creado'")
            save_failure(failures_file, dni, "no creado - sin nombre copiado")
            return "vpn_issue"  # Posible caida de VPN

        print(f"  Nombre copiado: {nombre_copiado}")

        # Validar que el nombre coincida
        if not names_match(nombre_csv, nombre_copiado):
            print(f"  ERROR: Nombre no coincide")
            print(f"    CSV: {nombre_csv}")
            print(f"    Copiado: {nombre_copiado}")

            # Intentar copiar "Búsqueda no guardada" para verificar estado del sistema
            print(f"  Verificando estado del sistema...")
            popup_right = coords.get('popup_right_click', {})
            popup_copy = coords.get('popup_copy_menu', {})

            if popup_right.get('x') and popup_copy.get('x'):
                clear_clipboard()
                time.sleep(0.2)

                # Click derecho en el area del popup
                pg.moveTo(popup_right['x'], popup_right['y'], duration=0.1)
                pg.rightClick()
                time.sleep(0.5)

                # Click izquierdo en copiar
                pg.moveTo(popup_copy['x'], popup_copy['y'], duration=0.1)
                pg.click()
                time.sleep(0.3)

                # Verificar si se copio
                copied = get_clipboard()
                expected_text = "Búsqueda no guardada"

                if expected_text in copied:
                    # Se copio correctamente - sistema OK, ya estamos donde debemos estar
                    print(f"  Sistema OK - 'Busqueda no guardada' copiada, continuando con siguiente DNI")
                    save_failure(failures_file, dni, f"no creado - nombre no coincide: {nombre_copiado}")
                    return "error"
                else:
                    # No se pudo copiar - sistema bloqueado, ejecutar recuperacion
                    print(f"  Sistema bloqueado detectado - ejecutando recuperacion...")

                    # Primero cerrar la ventana del DNI actual
                    close_btn = coords.get('close_btn', {})
                    if close_btn.get('x') and close_btn.get('y'):
                        print(f"    Cerrando ventana del DNI actual...")
                        click(close_btn['x'], close_btn['y'], 'Cerrar DNI', 0.5)

                    # Click en reconnect_click
                    reconnect = coords.get('reconnect_click', {})
                    if reconnect.get('x') and reconnect.get('y'):
                        print(f"    Click reconnect ({reconnect['x']}, {reconnect['y']})")
                        pg.moveTo(reconnect['x'], reconnect['y'], duration=0.1)
                        pg.click()
                        time.sleep(0.5)

                    # 4x close_btn
                    if close_btn.get('x') and close_btn.get('y'):
                        print(f"    4x close button...")
                        for i in range(4):
                            pg.moveTo(close_btn['x'], close_btn['y'], duration=0.1)
                            pg.click()
                            time.sleep(0.5)
                            print(f"      Click #{i+1}")

                    # btn_house
                    recovery = coords.get('btn_house', {})
                    if recovery.get('x') and recovery.get('y'):
                        print(f"    Click recovery ({recovery['x']}, {recovery['y']})")
                        pg.moveTo(recovery['x'], recovery['y'], duration=0.1)
                        pg.click()
                        time.sleep(1.0)

                    print(f"  Recuperacion completada - continuando con siguiente DNI")
                    save_failure(failures_file, dni, f"no creado - nombre no coincide + sistema bloqueado")
                    return "error"
            else:
                # No hay coordenadas configuradas, retornar error simple
                print(f"  ADVERTENCIA: Coordenadas popup no configuradas")
                save_failure(failures_file, dni, f"no creado - nombre no coincide: {nombre_copiado}")
                return "error"

        print(f"  Nombre validado OK")

        # Paso 8: Click en primera cuenta para abrir detalle
        click(first_result['x'], first_result['y'], 'Primera cuenta', 1.5)

        # Paso 9: Click derecho para menu contextual (seleccionar todo)
        right_click_addr = coords.get('right_click_address', {})
        right_click(right_click_addr['x'], right_click_addr['y'], 'Menu contextual', 0.5)

        # Paso 10: Click en "Seleccionar todo"
        select_all_menu = coords.get('select_all_menu', {})
        click(select_all_menu['x'], select_all_menu['y'], 'Seleccionar todo', 0.5)

        # Paso 11: Click derecho para menu de copiar
        right_click_copy = coords.get('right_click_copy', {})
        right_click(right_click_copy['x'], right_click_copy['y'], 'Menu copiar', 0.5)

        # Paso 12: Click en "Copiar"
        clear_clipboard()
        copy_menu = coords.get('copy_menu', {})
        click(copy_menu['x'], copy_menu['y'], 'Copiar', 0.5)

        # Obtener contenido copiado
        time.sleep(0.3)
        direccion = get_clipboard()

        if not direccion.strip():
            print(f"  ADVERTENCIA: No se copio ninguna direccion - posible cartel de error")
            print(f"  Intentando cerrar cartel con click reconnect + Enter...")

            # Click en reconnect_click para cerrar cartel
            reconnect = coords.get('reconnect_click', {})
            if reconnect.get('x') and reconnect.get('y'):
                pg.moveTo(reconnect['x'], reconnect['y'], duration=0.1)
                pg.click()
                time.sleep(0.5)
                pg.press('enter')
                time.sleep(1.0)
                print(f"  Cartel cerrado, reintentando copiar direccion...")

                # Reintentar copiar direccion
                # Click derecho para menu contextual (seleccionar todo)
                right_click_addr = coords.get('right_click_address', {})
                right_click(right_click_addr['x'], right_click_addr['y'], 'Menu contextual (reintento)', 0.5)

                # Click en "Seleccionar todo"
                select_all_menu = coords.get('select_all_menu', {})
                click(select_all_menu['x'], select_all_menu['y'], 'Seleccionar todo (reintento)', 0.5)

                # Click derecho para menu de copiar
                right_click_copy = coords.get('right_click_copy', {})
                right_click(right_click_copy['x'], right_click_copy['y'], 'Menu copiar (reintento)', 0.5)

                # Click en "Copiar"
                clear_clipboard()
                copy_menu = coords.get('copy_menu', {})
                click(copy_menu['x'], copy_menu['y'], 'Copiar (reintento)', 0.5)

                # Obtener contenido copiado (segundo intento)
                time.sleep(0.3)
                direccion = get_clipboard()

                if not direccion.strip():
                    print(f"  ERROR: No se pudo copiar direccion tras reintento")
                    save_failure(failures_file, dni, "Sin direccion copiada - fallo tras reintento")
                else:
                    print(f"  Direccion copiada exitosamente en reintento: {direccion[:100]}...")
                    save_result(results_file, dni, direccion)
            else:
                print(f"  ERROR: Coordenadas reconnect_click no configuradas")
                save_failure(failures_file, dni, "Sin direccion copiada - no se pudo cerrar cartel")
        else:
            print(f"  Direccion copiada: {direccion[:100]}...")
            save_result(results_file, dni, direccion)

        # Paso 13: Cerrar ventana
        close_btn = coords.get('close_btn', {})
        click(close_btn['x'], close_btn['y'], 'Cerrar', 0.5)

        return "ok"

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

        return "error"


def run(csv_path: Path, coords_path: Path, output_dir: Optional[Path] = None, start_delay: float = 3.0):
    """Ejecuta el proceso masivo."""
    pg.FAILSAFE = True  # Mover mouse a esquina superior izquierda para parar

    # Cargar coordenadas
    coords = load_coords(coords_path)

    # Configurar archivos de salida
    if output_dir is None:
        output_dir = Path('Result')
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = output_dir / f'resultados_{timestamp}.tsv'
    failures_file = output_dir / f'fallos_{timestamp}.tsv'
    vpn_log_file = output_dir / f'vpn_log_{timestamp}.txt'

    print(f"Archivo de resultados: {results_file}")
    print(f"Archivo de fallos: {failures_file}")
    print(f"Archivo de log VPN: {vpn_log_file}")

    # Inicializar archivo de log VPN
    with vpn_log_file.open('w', encoding='utf-8') as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Inicio sesion - VPN Host: {VPN_HOST}\n")

    # Cargar DNIs ya procesados (por si se retoma)
    processed = load_progress(results_file)
    print(f"DNIs ya procesados: {len(processed)}")

    # Leer CSV
    if not csv_path.exists():
        print(f"ERROR: No existe el archivo CSV: {csv_path}")
        sys.exit(1)

    registros = []  # Lista de tuplas (dni, nombre_cliente)
    with csv_path.open('r', encoding='utf-8', errors='ignore') as f:
        # Detectar delimitador
        sample = f.read(2048)
        f.seek(0)
        delimiter = ';' if sample.count(';') > sample.count(',') else ','

        reader = csv.DictReader(f, delimiter=delimiter)

        # Buscar columna de DNI (puede llamarse DNI, dni, Dni, documento, etc.)
        dni_col = None
        nombre_col = None
        if reader.fieldnames:
            for col in reader.fieldnames:
                col_lower = col.lower()
                if col_lower in ['dni', 'documento', 'doc', 'nro_documento']:
                    dni_col = col
                if 'nombre' in col_lower or 'cliente' in col_lower:
                    nombre_col = col

        if not dni_col:
            print(f"ERROR: No se encontro columna de DNI en el CSV")
            print(f"Columnas disponibles: {reader.fieldnames}")
            sys.exit(1)

        if not nombre_col:
            print(f"ERROR: No se encontro columna de nombre en el CSV")
            print(f"Columnas disponibles: {reader.fieldnames}")
            sys.exit(1)

        print(f"Usando columna DNI: {dni_col}")
        print(f"Usando columna Nombre: {nombre_col}")

        for row in reader:
            dni = row.get(dni_col, '').strip()
            nombre = row.get(nombre_col, '').strip()
            if dni and dni not in processed:
                registros.append((dni, nombre))

    total = len(registros)
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
    consecutive_failures = 0  # Contador de fallos consecutivos (vpn_issue o error)
    failed_dnis = []  # DNIs que fallaron consecutivamente
    MAX_CONSECUTIVE_FAILURES = 3

    # Tracking de eventos VPN
    vpn_events = []  # Lista de eventos de caida de VPN
    total_retries = 0
    total_retries_exitosos = 0

    i = 0
    while i < len(registros):
        dni, nombre_csv = registros[i]
        print(f"\n[{i+1}/{total}] ({exitosos} exitosos, {fallidos} fallidos)")

        result = process_dni(dni, nombre_csv, coords, results_file, failures_file)

        if result == "ok":
            exitosos += 1
            consecutive_failures = 0
            failed_dnis = []
        elif result == "vpn_issue" or result == "error":
            # Cualquier fallo (sin nombre o nombre no coincide) cuenta
            fallidos += 1
            consecutive_failures += 1
            failed_dnis.append((dni, nombre_csv))

            # Si hay 3+ fallos consecutivos, verificar si es problema de sistema
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n{'*'*60}")
                print(f"DETENCION POR FALLOS CONSECUTIVOS")
                print(f"{'*'*60}")
                print(f"Detectados {consecutive_failures} fallos consecutivos (sin nombre o nombre no coincide)")
                print(f"DNIs afectados: {[d[0] for d in failed_dnis]}")
                print(f"Verificando conectividad y estado del sistema...")

                # Log del evento
                with vpn_log_file.open('a', encoding='utf-8') as f:
                    f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {consecutive_failures} fallos consecutivos - DNIs: {', '.join([d[0] for d in failed_dnis])}\n")

                if not check_vpn():
                    # VPN caida - esperar reconexion (el scraping se detiene aqui)
                    vpn_event = wait_for_vpn(vpn_log_file)
                    vpn_event['dnis_afectados'] = [d[0] for d in failed_dnis]
                    vpn_events.append(vpn_event)

                    # Click de reconexion y Enter para activar el sistema
                    reconnect_click_action(coords, vpn_log_file)

                    # Limpiar popups que pueden haber aparecido
                    clear_vpn_popup(coords, vpn_log_file)

                    # Reintentar los DNIs que fallaron por VPN
                    print(f"\n{'='*60}")
                    print(f"REINTENTANDO {len(failed_dnis)} DNIs QUE FALLARON POR VPN")
                    print(f"{'='*60}")

                    # Log inicio de reintentos
                    with vpn_log_file.open('a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reintentos iniciados - Total: {len(failed_dnis)}\n")

                    retries_ok = 0
                    retries_fail = 0
                    for j, (retry_dni, retry_nombre) in enumerate(failed_dnis, 1):
                        print(f"\n  [REINTENTO {j}/{len(failed_dnis)}] DNI: {retry_dni}")
                        retry_result = process_dni(retry_dni, retry_nombre, coords, results_file, failures_file)
                        total_retries += 1
                        if retry_result == "ok":
                            retries_ok += 1
                            total_retries_exitosos += 1
                            exitosos += 1
                            fallidos -= 1  # Descontar el fallo anterior
                            print(f"    -> EXITO en reintento")
                            # Log reintento exitoso
                            with vpn_log_file.open('a', encoding='utf-8') as f:
                                f.write(f"  DNI {retry_dni}: EXITO\n")
                        else:
                            retries_fail += 1
                            print(f"    -> FALLO en reintento ({retry_result})")
                            # Log reintento fallido
                            with vpn_log_file.open('a', encoding='utf-8') as f:
                                f.write(f"  DNI {retry_dni}: FALLO ({retry_result})\n")

                    print(f"\n  Resultado reintentos: {retries_ok} exitosos, {retries_fail} fallidos")
                    print(f"{'='*60}")
                    print(f"REANUDANDO SCRAPING NORMAL...")
                    print(f"{'='*60}\n")

                    # Log resumen de reintentos
                    with vpn_log_file.open('a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reintentos finalizados - Exitosos: {retries_ok} - Fallidos: {retries_fail}\n")

                    vpn_event['retries_ok'] = retries_ok
                    vpn_event['retries_fail'] = retries_fail

                    # Reiniciar contadores
                    consecutive_failures = 0
                    failed_dnis = []
                else:
                    # VPN esta bien, puede ser otro problema (popup, error del sistema, etc)
                    print(f"\n  VPN activa (ping OK) - detectando otro problema...")
                    print(f"  Verificando si hay popup o cartel bloqueando...")

                    with vpn_log_file.open('a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] VPN activa (ping OK) - Otro problema detectado\n")

                    # Intentar copiar "Búsqueda no guardada" para verificar estado
                    popup_right = coords.get('popup_right_click', {})
                    popup_copy = coords.get('popup_copy_menu', {})

                    if popup_right.get('x') and popup_copy.get('x'):
                        clear_clipboard()
                        time.sleep(0.2)

                        # Click derecho en el area del popup
                        pg.moveTo(popup_right['x'], popup_right['y'], duration=0.1)
                        pg.rightClick()
                        time.sleep(0.5)

                        # Click izquierdo en copiar
                        pg.moveTo(popup_copy['x'], popup_copy['y'], duration=0.1)
                        pg.click()
                        time.sleep(0.3)

                        # Verificar si se copio
                        copied = get_clipboard()
                        expected_text = "Búsqueda no guardada"

                        if expected_text not in copied:
                            # No se pudo copiar - sistema bloqueado
                            print(f"  Sistema bloqueado detectado - ejecutando recuperacion...")
                            with vpn_log_file.open('a', encoding='utf-8') as f:
                                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sistema bloqueado - Ejecutando recuperacion\n")

                            # Click en reconnect_click primero
                            reconnect = coords.get('reconnect_click', {})
                            if reconnect.get('x') and reconnect.get('y'):
                                print(f"  Click reconnect ({reconnect['x']}, {reconnect['y']})")
                                pg.moveTo(reconnect['x'], reconnect['y'], duration=0.1)
                                pg.click()
                                time.sleep(0.5)

                            # Presionar 4 veces close_btn
                            close_btn = coords.get('close_btn', {})
                            if close_btn.get('x') and close_btn.get('y'):
                                print(f"  Presionando close button 4 veces...")
                                for attempt in range(4):
                                    pg.moveTo(close_btn['x'], close_btn['y'], duration=0.1)
                                    pg.click()
                                    time.sleep(0.5)
                                    print(f"    Click close #{attempt+1}")

                            # Click en btn_house
                            recovery = coords.get('btn_house', {})
                            if recovery.get('x') and recovery.get('y'):
                                print(f"  Click de recuperacion en ({recovery['x']}, {recovery['y']})...")
                                pg.moveTo(recovery['x'], recovery['y'], duration=0.1)
                                pg.click()
                                time.sleep(1.0)
                                print(f"  Recuperacion completada")

                                with vpn_log_file.open('a', encoding='utf-8') as f:
                                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Recuperacion ejecutada - reconnect + 4x close + btn_house\n")
                            else:
                                print(f"  ADVERTENCIA: Coordenadas btn_house no configuradas")
                        else:
                            print(f"  Popup verificado OK - sin bloqueos detectados")

                    consecutive_failures = 0
                    failed_dnis = []
        else:  # error
            fallidos += 1
            consecutive_failures = 0
            failed_dnis = []

        i += 1
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

    # Resumen de eventos VPN
    if vpn_events:
        print(f"\n{'-'*50}")
        print(f"EVENTOS DE VPN ({len(vpn_events)} desconexiones)")
        print(f"{'-'*50}")

        # Agregar resumen final al log de VPN
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"\n")

            for idx, event in enumerate(vpn_events, 1):
                print(f"\n  Evento #{idx}:")
                print(f"    Inicio: {event['start'].strftime('%H:%M:%S')}")
                print(f"    Fin: {event['end'].strftime('%H:%M:%S')}")
                print(f"    Duracion: {int(event['duration_seconds'] // 60)}m {int(event['duration_seconds'] % 60)}s")
                print(f"    Pings realizados: {event['ping_attempts']}")
                print(f"    DNIs afectados: {event.get('dnis_afectados', [])}")
                print(f"    Reintentos exitosos: {event.get('retries_ok', 0)}/{event.get('retries_ok', 0) + event.get('retries_fail', 0)}")

            total_duration = sum(e['duration_seconds'] for e in vpn_events)
            total_pings = sum(e['ping_attempts'] for e in vpn_events)

            print(f"\n  TOTALES VPN:")
            print(f"    Desconexiones: {len(vpn_events)}")
            print(f"    Tiempo total caido: {int(total_duration // 60)}m {int(total_duration % 60)}s")
            print(f"    Total pings: {total_pings}")
            print(f"    Reintentos: {total_retries} ({total_retries_exitosos} exitosos)")

            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sesion finalizada - Desconexiones: {len(vpn_events)} - Tiempo caido: {int(total_duration // 60)}m {int(total_duration % 60)}s - Pings: {total_pings} - Reintentos: {total_retries_exitosos}/{total_retries}\n")

        print(f"\n  Log detallado de VPN guardado en: {vpn_log_file}")
    else:
        # Si no hubo eventos VPN, cerrar el log
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sesion finalizada - Sin caidas de VPN\n")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Extraccion masiva de direcciones por DNI')
    parser.add_argument('--csv', required=True, help='Archivo CSV con los DNIs')
    parser.add_argument('--coords', default='camino-lote-masivo.json', help='Archivo JSON con coordenadas')
    parser.add_argument('--output-dir', default='Result', help='Directorio para archivos de salida')
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
