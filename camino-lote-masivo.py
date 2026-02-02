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
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List

# ============================================================================
# CONFIGURACIÓN Y CONSTANTES
# ============================================================================

# Tiempos de espera (segundos)
DELAY_CLICK = 0.3
DELAY_SHORT = 0.2
DELAY_MEDIUM = 0.5
DELAY_LONG = 1.0
DELAY_SEARCH_WAIT = 2.0
DELAY_DETAIL_OPEN = 1.5
DELAY_BETWEEN_DNIS = 0.5

# PyAutoGUI
MOUSE_MOVE_DURATION = 0.1
KEYBOARD_INTERVAL = 0.05

# Manejo de errores y reintentos
MAX_CONSECUTIVE_FAILURES = 3
MAX_POPUP_CLEAR_ATTEMPTS = 5
CLIPBOARD_RETRY_DELAY = 0.3
RECOVERY_CLOSE_CLICKS = 4

# VPN
VPN_HOST = "10.167.205.151"
VPN_CHECK_INTERVAL = 10
VPN_STABILITY_CHECKS = 3
VPN_STABILITY_DELAY = 2
VPN_PING_TIMEOUT = 1000  # ms para Windows

# Validación de nombres
MIN_WORD_LENGTH = 2  # Longitud mínima para comparar palabras en nombres

# Texto esperado en popups
EXPECTED_POPUP_TEXT = "Búsqueda no guardada"

# Archivos de salida
OUTPUT_DIR_DEFAULT = "Result"
RESULTS_FILE_PREFIX = "resultados"
FAILURES_FILE_PREFIX = "fallos"
VPN_LOG_FILE_PREFIX = "vpn_log"
SCRAPING_LOG_PREFIX = "scraping"
FILE_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# Columnas del CSV de entrada (en orden)
CSV_INPUT_COLUMNS = [
    'Nombre del Cliente', 'DNI', 'ANI1', 'Linea1', 'Linea2',
    'PlanActual', 'OperadorActual', 'Domicilio', 'CP', 'Localidad',
    'email', 'Provincia', 'BBDD', 'Generico'
]

# ============================================================================
# CONFIGURACIÓN INICIAL
# ============================================================================

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

# Logger global
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

def setup_logging(output_dir: Path, timestamp: str) -> Path:
    """Configura el sistema de logging con archivo y consola.

    Args:
        output_dir: Directorio donde guardar el archivo de log
        timestamp: Timestamp para el nombre del archivo

    Returns:
        Path al archivo de log creado
    """
    log_file = output_dir / f'{SCRAPING_LOG_PREFIX}_{timestamp}.log'

    # Configuración base para archivo (nivel DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=str(log_file),
        filemode='a',
        encoding='utf-8'
    )

    # Handler para consola (nivel INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)-8s | %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logger.info(f"Sistema de logging inicializado - Archivo: {log_file}")
    return log_file


def setup_pyautogui(pause_duration: float = DELAY_CLICK) -> None:
    """Configura PyAutoGUI con opciones seguras.

    Args:
        pause_duration: Pausa automática después de cada acción PyAutoGUI
    """
    pg.FAILSAFE = True
    pg.PAUSE = pause_duration
    logger.info(f"PyAutoGUI configurado: FAILSAFE=True, PAUSE={pause_duration}s")


# ============================================================================
# UTILIDADES DE CLIPBOARD
# ============================================================================


def get_clipboard() -> str:
    """Obtiene el contenido del portapapeles.

    Returns:
        Contenido del portapapeles o string vacío si falla
    """
    if pyperclip:
        try:
            return pyperclip.paste() or ''
        except (Exception,) as e:
            logger.debug(f"Pyperclip falló: {e}")

    # Fallback con tkinter
    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()
        try:
            content = root.clipboard_get()
        except tk.TclError as e:
            logger.debug(f"No se pudo obtener clipboard: {e}")
            content = ''
        finally:
            root.destroy()
        return content or ''
    except (ImportError, Exception) as e:
        logger.debug(f"Error al acceder clipboard: {e}")
        return ''


def clear_clipboard() -> None:
    """Limpia el portapapeles."""
    if pyperclip:
        try:
            pyperclip.copy('')
            return
        except Exception as e:
            logger.debug(f"Error limpiando clipboard con pyperclip: {e}")

    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()
        root.clipboard_clear()
        root.update()
        root.destroy()
    except Exception as e:
        logger.debug(f"Error limpiando clipboard con tkinter: {e}")


def get_clipboard_with_retry(
    max_attempts: int = 3,
    retry_delay: float = CLIPBOARD_RETRY_DELAY
) -> str:
    """Obtiene contenido del clipboard con reintentos.

    Args:
        max_attempts: Número máximo de intentos
        retry_delay: Segundos entre intentos

    Returns:
        Contenido del clipboard o string vacío si falla
    """
    for attempt in range(max_attempts):
        content = get_clipboard()
        if content:
            return content

        if attempt < max_attempts - 1:
            logger.debug(f"Clipboard vacío, reintentando ({attempt + 1}/{max_attempts})")
            time.sleep(retry_delay)

    logger.warning(f"No se pudo obtener clipboard después de {max_attempts} intentos")
    return ''


# ============================================================================
# UTILIDADES DE VALIDACIÓN
# ============================================================================


def normalize_name(name: str) -> set:
    """Normaliza un nombre y retorna un set de palabras.

    Args:
        name: Nombre a normalizar

    Returns:
        Set de palabras normalizadas (sin acentos, en mayúsculas, longitud > MIN_WORD_LENGTH)
    """
    import unicodedata
    # Quitar acentos
    normalized = unicodedata.normalize('NFD', name)
    normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    # Convertir a mayusculas y dividir en palabras
    words = normalized.upper().split()
    # Filtrar palabras muy cortas (articulos, etc.)
    return {w for w in words if len(w) > MIN_WORD_LENGTH}


def names_match(csv_name: str, copied_name: str) -> bool:
    """Verifica si al menos un nombre/apellido coincide entre ambos nombres.

    Args:
        csv_name: Nombre del CSV
        copied_name: Nombre copiado del sistema

    Returns:
        True si hay al menos una palabra en común
    """
    csv_words = normalize_name(csv_name)
    copied_words = normalize_name(copied_name)
    # Verificar si hay al menos una palabra en comun
    common = csv_words & copied_words
    return len(common) > 0


# ============================================================================
# GESTIÓN DE VPN
# ============================================================================


def check_vpn() -> bool:
    """Verifica si la VPN esta activa haciendo ping al host.

    Returns:
        True si la VPN está activa, False en caso contrario
    """
    import subprocess
    try:
        # En Windows usamos -n 1 para un solo ping, -w para timeout en ms
        result = subprocess.run(
            ['ping', '-n', '1', '-w', str(VPN_PING_TIMEOUT), VPN_HOST],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.debug("Timeout en ping a VPN")
        return False
    except FileNotFoundError:
        logger.error("Comando ping no encontrado")
        return False
    except Exception as e:
        logger.error(f"Error inesperado en check_vpn: {e}")
        return False


def wait_for_vpn(vpn_log_file: Path) -> dict:
    """Espera hasta que la VPN vuelva a estar disponible.

    Realiza pings periódicos al host VPN y valida estabilidad de la conexión
    antes de reanudar el scraping.

    Args:
        vpn_log_file: Path al archivo de log donde registrar eventos

    Returns:
        dict con información del evento:
            - start: datetime del inicio de caída
            - end: datetime de reconexión
            - duration_seconds: duración total en segundos
            - ping_attempts: número de pings realizados
    """
    start_time = datetime.now()
    logger.critical(f"VPN CAIDA DETECTADA - {VPN_HOST}")
    logger.critical(f"SCRAPING DETENIDO - Esperando reconexion VPN...")
    logger.critical("No se procesaran mas DNIs hasta que la VPN vuelva")

    # Log en tiempo real
    with vpn_log_file.open('a', encoding='utf-8') as f:
        f.write(f"\n[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] VPN CAIDA - Scraping detenido\n")

    ping_attempts = 0
    while not check_vpn():
        ping_attempts += 1
        logger.info(f"Ping #{ping_attempts} fallido. Reintentando en {VPN_CHECK_INTERVAL}s...")

        # Log cada ping
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ping #{ping_attempts} - FALLO\n")

        time.sleep(VPN_CHECK_INTERVAL)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.critical(f"VPN RECONECTADA!")
    logger.info(f"Tiempo caida: {int(duration // 60)}m {int(duration % 60)}s")
    logger.info(f"Pings realizados: {ping_attempts}")

    # Log reconexion inicial
    with vpn_log_file.open('a', encoding='utf-8') as f:
        f.write(f"[{end_time.strftime('%Y-%m-%d %H:%M:%S')}] VPN RECONECTADA - "
                f"Duracion: {int(duration // 60)}m {int(duration % 60)}s - Pings: {ping_attempts}\n")

    # Validar estabilidad de la conexion
    logger.info("Validando estabilidad de la conexion...")
    stability_ok = True

    for i in range(VPN_STABILITY_CHECKS):
        time.sleep(VPN_STABILITY_DELAY)
        if check_vpn():
            logger.debug(f"Ping de estabilidad {i+1}/{VPN_STABILITY_CHECKS}: OK")
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                       f"Ping estabilidad {i+1}/{VPN_STABILITY_CHECKS}: OK\n")
        else:
            logger.warning(f"Ping de estabilidad {i+1}/{VPN_STABILITY_CHECKS}: FALLO - VPN aun inestable")
            stability_ok = False
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                       f"Ping estabilidad {i+1}/{VPN_STABILITY_CHECKS}: FALLO\n")
            break

    if stability_ok:
        logger.info("Conexion VPN ESTABLE")
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Conexion ESTABLE - Scraping reanudado\n")
        logger.info("SCRAPING SE REANUDARA en 3 segundos...")
        time.sleep(3)
    else:
        logger.warning("Conexion inestable, esperando 10s y revalidando...")
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


def reconnect_click_action(coords: dict, vpn_log_file: Optional[Path] = None) -> None:
    """Realiza el click de reconexion y presiona Enter.

    Este click se hace despues de que la VPN vuelve para activar
    el sistema antes de limpiar popups.

    Args:
        coords: Diccionario con coordenadas
        vpn_log_file: Path opcional al archivo de log VPN
    """
    reconnect = coords.get('reconnect_click', {})

    if not reconnect.get('x') or not reconnect.get('y'):
        logger.warning("Coordenadas de reconnect_click no configuradas, saltando...")
        if vpn_log_file:
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                       f"ADVERTENCIA: Coordenadas reconnect_click no configuradas\n")
        return

    logger.info(f"Realizando click de reconexion en ({reconnect['x']}, {reconnect['y']})...")
    if vpn_log_file:
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                   f"Click reconexion ({reconnect['x']}, {reconnect['y']}) + Enter\n")

    pg.moveTo(reconnect['x'], reconnect['y'], duration=MOUSE_MOVE_DURATION)
    pg.click()
    time.sleep(DELAY_MEDIUM)

    logger.debug("Presionando Enter...")
    pg.press('enter')
    time.sleep(DELAY_LONG)

    logger.info("Click de reconexion completado")


def clear_vpn_popup(coords: dict, vpn_log_file: Optional[Path] = None) -> None:
    """Limpia los popups que aparecen cuando la VPN se desconecta.

    Intenta copiar texto esperado del menu contextual.
    Si no lo logra, presiona Enter y reintenta.

    Args:
        coords: Diccionario con coordenadas
        vpn_log_file: Path opcional al archivo de log VPN
    """
    logger.info("Limpiando posibles popups de VPN...")

    popup_right = coords.get('popup_right_click', {})
    popup_copy = coords.get('popup_copy_menu', {})

    if not popup_right.get('x') or not popup_copy.get('x'):
        logger.warning("Coordenadas de popup no configuradas, saltando limpieza")
        if vpn_log_file:
            with vpn_log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                       f"ADVERTENCIA: Coordenadas popup no configuradas\n")
        return

    for attempt in range(MAX_POPUP_CLEAR_ATTEMPTS):
        logger.debug(f"Intento {attempt + 1}/{MAX_POPUP_CLEAR_ATTEMPTS} de limpiar popup...")

        # Limpiar clipboard
        clear_clipboard()
        time.sleep(DELAY_SHORT)

        # Click derecho en el area del popup
        pg.moveTo(popup_right['x'], popup_right['y'], duration=MOUSE_MOVE_DURATION)
        pg.rightClick()
        time.sleep(DELAY_MEDIUM)

        # Click en copiar
        pg.moveTo(popup_copy['x'], popup_copy['y'], duration=MOUSE_MOVE_DURATION)
        pg.click()
        time.sleep(CLIPBOARD_RETRY_DELAY)

        # Verificar si se copio el texto esperado
        copied = get_clipboard()

        if EXPECTED_POPUP_TEXT in copied:
            logger.info(f"Popup limpiado correctamente (copiado: '{copied[:30]}...')")
            if vpn_log_file:
                with vpn_log_file.open('a', encoding='utf-8') as f:
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                           f"Popup limpiado (intento {attempt + 1})\n")
            return

        # Si no se copio, presionar Enter para cerrar cualquier dialogo
        logger.debug("No se detecto popup, presionando Enter...")
        pg.press('enter')
        time.sleep(DELAY_MEDIUM)
        pg.press('enter')
        time.sleep(DELAY_MEDIUM)

    logger.warning("No se pudo confirmar limpieza de popup, continuando de todos modos...")
    if vpn_log_file:
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                   f"Popup no confirmado tras {MAX_POPUP_CLEAR_ATTEMPTS} intentos\n")


# ============================================================================
# RECUPERACIÓN DE SISTEMA BLOQUEADO
# ============================================================================

def check_system_blocked(coords: dict) -> bool:
    """Verifica si el sistema está bloqueado intentando copiar texto esperado.

    Args:
        coords: Diccionario con coordenadas

    Returns:
        True si el sistema está bloqueado, False si está OK
    """
    popup_right = coords.get('popup_right_click', {})
    popup_copy = coords.get('popup_copy_menu', {})

    if not popup_right.get('x') or not popup_copy.get('x'):
        logger.warning("No se pueden verificar bloqueos (coordenadas popup faltantes)")
        return False

    clear_clipboard()
    time.sleep(DELAY_SHORT)

    # Click derecho en el area del popup
    pg.moveTo(popup_right['x'], popup_right['y'], duration=MOUSE_MOVE_DURATION)
    pg.rightClick()
    time.sleep(DELAY_MEDIUM)

    # Click izquierdo en copiar
    pg.moveTo(popup_copy['x'], popup_copy['y'], duration=MOUSE_MOVE_DURATION)
    pg.click()
    time.sleep(CLIPBOARD_RETRY_DELAY)

    # Verificar si se copio el texto esperado
    copied = get_clipboard()

    if EXPECTED_POPUP_TEXT in copied:
        logger.debug("Sistema OK - popup copiado correctamente")
        return False  # No está bloqueado
    else:
        logger.warning("Sistema bloqueado detectado - no se pudo copiar popup")
        return True  # Está bloqueado


def execute_system_recovery(coords: dict, log_file: Optional[Path] = None) -> bool:
    """Ejecuta la secuencia de recuperación del sistema bloqueado.

    Secuencia: reconnect_click -> 4x close_btn -> btn_house

    Args:
        coords: Diccionario con coordenadas
        log_file: Path opcional al archivo de log

    Returns:
        True si se ejecutó correctamente, False si faltan coordenadas
    """
    logger.warning("Ejecutando recuperación de sistema bloqueado")

    if log_file:
        with log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                   f"Sistema bloqueado - Ejecutando recuperacion\n")

    # Click en reconnect_click
    reconnect = coords.get('reconnect_click', {})
    if reconnect.get('x') and reconnect.get('y'):
        logger.debug(f"Click reconnect ({reconnect['x']}, {reconnect['y']})")
        pg.moveTo(reconnect['x'], reconnect['y'], duration=MOUSE_MOVE_DURATION)
        pg.click()
        time.sleep(DELAY_MEDIUM)
    else:
        logger.error("Falta coordenada: reconnect_click")
        return False

    # 4x close_btn
    close_btn = coords.get('close_btn', {})
    if close_btn.get('x') and close_btn.get('y'):
        logger.debug(f"Presionando close button {RECOVERY_CLOSE_CLICKS} veces")
        for i in range(RECOVERY_CLOSE_CLICKS):
            pg.moveTo(close_btn['x'], close_btn['y'], duration=MOUSE_MOVE_DURATION)
            pg.click()
            time.sleep(DELAY_MEDIUM)
            logger.debug(f"  Click close #{i+1}")
    else:
        logger.error("Falta coordenada: close_btn")
        return False

    # Click en btn_house
    recovery = coords.get('btn_house', {})
    if recovery.get('x') and recovery.get('y'):
        logger.debug(f"Click recovery ({recovery['x']}, {recovery['y']})")
        pg.moveTo(recovery['x'], recovery['y'], duration=MOUSE_MOVE_DURATION)
        pg.click()
        time.sleep(DELAY_LONG)
        logger.info("Recuperación completada")

        if log_file:
            with log_file.open('a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                       f"Recuperacion ejecutada - reconnect + {RECOVERY_CLOSE_CLICKS}x close + btn_house\n")
        return True
    else:
        logger.error("Falta coordenada: btn_house")
        return False


# ============================================================================
# CARGA Y VALIDACIÓN DE COORDENADAS
# ============================================================================


def validate_coordinates(coords: dict) -> Tuple[bool, List[str]]:
    """Valida que todas las coordenadas requeridas estén presentes.

    Args:
        coords: Diccionario con coordenadas

    Returns:
        Tupla (valid, missing_keys) con bool de validez y lista de claves faltantes
    """
    required_keys = [
        'dni_input',
        'first_result',
        'copy_name_menu',
        'right_click_address',
        'select_all_menu',
        'right_click_copy',
        'copy_menu',
        'close_btn',
        'reconnect_click',
        'popup_right_click',
        'popup_copy_menu',
        'btn_house'
    ]

    missing = []
    for key in required_keys:
        coord = coords.get(key, {})
        if not coord.get('x') or not coord.get('y'):
            missing.append(key)

    return len(missing) == 0, missing


def load_coords(path: Path) -> dict:
    """Carga y valida las coordenadas desde el JSON.

    Args:
        path: Path al archivo JSON con coordenadas

    Returns:
        Diccionario con coordenadas validadas

    Raises:
        SystemExit si el archivo no existe o las coordenadas son inválidas
    """
    if not path.exists():
        logger.error(f"No existe el archivo de coordenadas: {path}")
        sys.exit(1)

    with path.open('r', encoding='utf-8') as f:
        coords = json.load(f)

    # Validar coordenadas
    valid, missing = validate_coordinates(coords)
    if not valid:
        logger.error("Coordenadas incompletas en el archivo JSON:")
        for key in missing:
            logger.error(f"  - Falta o es inválida: {key}")
        sys.exit(1)

    logger.info("Coordenadas validadas correctamente")
    return coords


# ============================================================================
# ACCIONES BÁSICAS DE PYAUTOGUI
# ============================================================================


def click(x: int, y: int, label: str, delay: float = DELAY_CLICK) -> None:
    """Hace click en las coordenadas especificadas.

    Args:
        x: Coordenada X
        y: Coordenada Y
        label: Etiqueta descriptiva para logging
        delay: Tiempo de espera después del click (usa PAUSE global de PyAutoGUI también)
    """
    logger.debug(f"Click en {label} ({x}, {y})")
    pg.moveTo(x, y, duration=MOUSE_MOVE_DURATION)
    pg.click()
    time.sleep(delay)


def right_click(x: int, y: int, label: str, delay: float = DELAY_CLICK) -> None:
    """Hace click derecho en las coordenadas especificadas.

    Args:
        x: Coordenada X
        y: Coordenada Y
        label: Etiqueta descriptiva para logging
        delay: Tiempo de espera después del click
    """
    logger.debug(f"Click derecho en {label} ({x}, {y})")
    pg.moveTo(x, y, duration=MOUSE_MOVE_DURATION)
    pg.rightClick()
    time.sleep(delay)


def type_text(text: str, delay: float = DELAY_CLICK) -> None:
    """Escribe texto caracter por caracter.

    Args:
        text: Texto a escribir
        delay: Tiempo de espera después de escribir
    """
    logger.debug(f"Escribiendo: {text}")
    for char in text:
        pg.typewrite(char, interval=KEYBOARD_INTERVAL)
    time.sleep(delay)


# ============================================================================
# GESTIÓN DE PROGRESO Y ARCHIVOS
# ============================================================================


def load_progress(progress_file: Path) -> set:
    """Carga los DNIs ya procesados desde archivo CSV.

    Args:
        progress_file: Path al archivo de progreso (CSV)

    Returns:
        Set de DNIs ya procesados
    """
    if not progress_file.exists():
        return set()

    processed = set()
    with progress_file.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            dni = row.get('DNI', '').strip()
            if dni:
                processed.add(dni)
    return processed


def save_result(
    results_file: Path,
    row_data: dict,
    ubicacion: str,
    fieldnames: List[str],
    write_header: bool = False
) -> None:
    """Guarda un resultado exitoso en formato CSV con todas las columnas.

    Args:
        results_file: Path al archivo de resultados (CSV)
        row_data: Diccionario con todos los datos del registro original
        ubicacion: Ubicación/dirección obtenida del scraping
        fieldnames: Lista de nombres de columnas para el CSV
        write_header: Si True, escribe el header (solo para el primer registro)
    """
    # Limpiar la ubicación de saltos de línea
    ubicacion_clean = ubicacion.replace('\n', ' ').replace('\r', ' ').strip()

    # Crear copia del registro y agregar la ubicación
    output_row = {col: row_data.get(col, '') for col in fieldnames if col != 'Ubicacion'}
    output_row['Ubicacion'] = ubicacion_clean

    with results_file.open('a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
        if write_header:
            writer.writeheader()
        writer.writerow(output_row)


def save_failure(failures_file: Path, dni: str, reason: str) -> None:
    """Guarda un fallo.

    Args:
        failures_file: Path al archivo de fallos
        dni: DNI que falló
        reason: Razón del fallo
    """
    with failures_file.open('a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{dni}\t{reason}\t{timestamp}\n")


# ============================================================================
# PROCESAMIENTO DE DNI - FUNCIONES AUXILIARES
# ============================================================================

def search_dni(dni: str, coords: dict) -> None:
    """Realiza la búsqueda del DNI en el input.

    Args:
        dni: DNI a buscar
        coords: Diccionario con coordenadas
    """
    logger.debug(f"Buscando DNI: {dni}")
    input_coords = coords['dni_input']

    click(input_coords['x'], input_coords['y'], 'Input DNI', DELAY_CLICK)

    logger.debug("Ctrl+A (seleccionar todo)")
    pg.hotkey('ctrl', 'a')
    time.sleep(DELAY_SHORT)

    logger.debug("Backspace (borrar)")
    pg.press('backspace')
    time.sleep(DELAY_SHORT)

    type_text(dni, DELAY_CLICK)

    logger.debug("Enter (buscar)")
    pg.press('enter')
    time.sleep(DELAY_SEARCH_WAIT)


def copy_and_validate_name(
    nombre_csv: str,
    coords: dict,
    failures_file: Path
) -> Optional[str]:
    """Copia el nombre y valida que coincida con el CSV.

    Args:
        nombre_csv: Nombre esperado del CSV
        coords: Diccionario con coordenadas
        failures_file: Path al archivo de fallos (no usado aquí, por compatibilidad)

    Returns:
        Nombre copiado si es válido, None si no coincide o falla
    """
    clear_clipboard()
    first_result = coords['first_result']
    right_click(first_result['x'], first_result['y'], 'Primera cuenta', DELAY_MEDIUM)

    copy_name_menu = coords['copy_name_menu']
    click(copy_name_menu['x'], copy_name_menu['y'], 'Copiar nombre', DELAY_MEDIUM)

    time.sleep(CLIPBOARD_RETRY_DELAY)
    nombre_copiado = get_clipboard()

    if not nombre_copiado.strip():
        logger.error("No se copio ningun nombre")
        return None

    logger.debug(f"Nombre copiado: {nombre_copiado}")

    if not names_match(nombre_csv, nombre_copiado):
        logger.warning(f"Nombre no coincide - CSV: {nombre_csv}, Copiado: {nombre_copiado}")
        return None

    logger.debug("Nombre validado OK")
    return nombre_copiado


def copy_address_with_retry(coords: dict) -> Optional[str]:
    """Copia la dirección con manejo de reintento en caso de error.

    Args:
        coords: Diccionario con coordenadas

    Returns:
        Dirección copiada o None si falla
    """
    def attempt_copy_address() -> Optional[str]:
        """Intento de copiar dirección."""
        right_click_addr = coords['right_click_address']
        right_click(right_click_addr['x'], right_click_addr['y'], 'Menu contextual', DELAY_MEDIUM)

        select_all_menu = coords['select_all_menu']
        click(select_all_menu['x'], select_all_menu['y'], 'Seleccionar todo', DELAY_MEDIUM)

        right_click_copy = coords['right_click_copy']
        right_click(right_click_copy['x'], right_click_copy['y'], 'Menu copiar', DELAY_MEDIUM)

        clear_clipboard()
        copy_menu = coords['copy_menu']
        click(copy_menu['x'], copy_menu['y'], 'Copiar', DELAY_MEDIUM)

        time.sleep(CLIPBOARD_RETRY_DELAY)
        return get_clipboard()

    # Primer intento
    direccion = attempt_copy_address()

    if direccion.strip():
        return direccion

    # Reintento con cierre de cartel de error
    logger.warning("No se copio direccion - intentando cerrar cartel y reintentar")

    reconnect = coords.get('reconnect_click', {})
    if reconnect.get('x') and reconnect.get('y'):
        pg.moveTo(reconnect['x'], reconnect['y'], duration=MOUSE_MOVE_DURATION)
        pg.click()
        time.sleep(DELAY_MEDIUM)
        pg.press('enter')
        time.sleep(DELAY_LONG)
        logger.debug("Cartel cerrado, reintentando")

        direccion = attempt_copy_address()

        if direccion.strip():
            logger.info("Direccion copiada exitosamente en reintento")
            return direccion

    logger.error("No se pudo copiar direccion tras reintento")
    return None


# ============================================================================
# PROCESAMIENTO DE DNI - FUNCIÓN PRINCIPAL
# ============================================================================


def process_dni(
    row_data: dict,
    dni_col: str,
    nombre_col: str,
    coords: dict,
    results_file: Path,
    failures_file: Path,
    fieldnames: List[str],
    write_header: bool = False
) -> str:
    """Procesa un DNI individual.

    Args:
        row_data: Diccionario con todos los datos del registro
        dni_col: Nombre de la columna DNI
        nombre_col: Nombre de la columna de nombre
        coords: Diccionario con coordenadas
        results_file: Path al archivo de resultados
        failures_file: Path al archivo de fallos
        fieldnames: Lista de nombres de columnas para el CSV de salida
        write_header: Si True, escribe el header (solo para el primer registro)

    Returns:
        'ok': Procesado exitosamente
        'vpn_issue': Problema de VPN detectado
        'error': Error general
    """
    dni = row_data.get(dni_col, '').strip()
    nombre_csv = row_data.get(nombre_col, '').strip()

    logger.info(f"{'='*50}")
    logger.info(f"Procesando DNI: {dni} - Nombre esperado: {nombre_csv}")
    logger.info(f"{'='*50}")

    try:
        # Paso 1-5: Buscar DNI
        search_dni(dni, coords)

        # Paso 6-7: Copiar y validar nombre
        nombre_valido = copy_and_validate_name(nombre_csv, coords, failures_file)
        if nombre_valido is None:
            save_failure(failures_file, dni, "no creado - nombre no coincide o sin nombre")

            # Verificar si sistema está bloqueado
            if check_system_blocked(coords):
                logger.info("Sistema bloqueado, cerrando DNI actual y ejecutando recuperación")
                close_btn = coords['close_btn']
                click(close_btn['x'], close_btn['y'], 'Cerrar DNI', DELAY_MEDIUM)
                execute_system_recovery(coords)
            else:
                logger.debug("Sistema OK - continuando con siguiente DNI")

            return "vpn_issue"

        # Paso 8: Abrir detalle
        first_result = coords['first_result']
        click(first_result['x'], first_result['y'], 'Primera cuenta', DELAY_DETAIL_OPEN)

        # Paso 9-12: Copiar dirección con reintento automático
        direccion = copy_address_with_retry(coords)

        if direccion:
            logger.info(f"Direccion copiada: {direccion[:100]}...")
            save_result(results_file, row_data, direccion, fieldnames, write_header)
        else:
            save_failure(failures_file, dni, "Sin direccion copiada - fallo tras reintento")

        # Paso 13: Cerrar ventana
        close_btn = coords['close_btn']
        click(close_btn['x'], close_btn['y'], 'Cerrar', DELAY_MEDIUM)

        return "ok" if direccion else "error"

    except KeyboardInterrupt:
        raise
    except Exception as e:
        logger.error(f"Error inesperado procesando DNI {dni}: {e}", exc_info=True)
        save_failure(failures_file, dni, f"Exception: {str(e)}")

        # Intentar cerrar ventana en caso de error
        try:
            close_btn = coords.get('close_btn', {})
            if close_btn.get('x') and close_btn.get('y'):
                click(close_btn['x'], close_btn['y'], 'Cerrar (recovery)', DELAY_MEDIUM)
        except Exception:
            pass

        return "error"


# ============================================================================
# FUNCIÓN PRINCIPAL - RUN
# ============================================================================


def run(
    csv_path: Path,
    coords_path: Path,
    output_dir: Optional[Path] = None,
    start_delay: float = 3.0
) -> None:
    """Ejecuta el proceso masivo de scraping.

    Args:
        csv_path: Path al archivo CSV con DNIs
        coords_path: Path al archivo JSON con coordenadas
        output_dir: Directorio para archivos de salida (default: Result)
        start_delay: Segundos de espera antes de empezar (default: 3.0)
    """
    # Configurar archivos de salida
    if output_dir is None:
        output_dir = Path(OUTPUT_DIR_DEFAULT)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime(FILE_TIMESTAMP_FORMAT)

    # Configurar logging
    log_file = setup_logging(output_dir, timestamp)

    # Configurar PyAutoGUI
    setup_pyautogui(DELAY_CLICK)

    # Archivos de salida
    results_file = output_dir / f'{RESULTS_FILE_PREFIX}_{timestamp}.csv'
    failures_file = output_dir / f'{FAILURES_FILE_PREFIX}_{timestamp}.tsv'
    vpn_log_file = output_dir / f'{VPN_LOG_FILE_PREFIX}_{timestamp}.txt'

    logger.info(f"Archivo de resultados: {results_file}")
    logger.info(f"Archivo de fallos: {failures_file}")
    logger.info(f"Archivo de log VPN: {vpn_log_file}")
    logger.info(f"Archivo de log general: {log_file}")

    # Cargar y validar coordenadas
    coords = load_coords(coords_path)

    # Inicializar archivo de log VPN
    with vpn_log_file.open('w', encoding='utf-8') as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Inicio sesion - VPN Host: {VPN_HOST}\n")

    # Cargar DNIs ya procesados (por si se retoma)
    processed = load_progress(results_file)
    logger.info(f"DNIs ya procesados: {len(processed)}")

    # Leer CSV
    if not csv_path.exists():
        logger.error(f"No existe el archivo CSV: {csv_path}")
        sys.exit(1)

    registros = []  # Lista de diccionarios con todos los datos del registro
    with csv_path.open('r', encoding='utf-8', errors='ignore') as f:
        # Detectar delimitador
        sample = f.read(2048)
        f.seek(0)
        delimiter = ';' if sample.count(';') > sample.count(',') else ','

        reader = csv.DictReader(f, delimiter=delimiter)
        input_fieldnames = reader.fieldnames or []

        # Buscar columna de DNI (puede llamarse DNI, dni, Dni, documento, etc.)
        dni_col = None
        nombre_col = None
        if input_fieldnames:
            for col in input_fieldnames:
                col_lower = col.lower()
                if col_lower in ['dni', 'documento', 'doc', 'nro_documento']:
                    dni_col = col
                if 'nombre' in col_lower or 'cliente' in col_lower:
                    nombre_col = col

        if not dni_col:
            logger.error("No se encontro columna de DNI en el CSV")
            logger.error(f"Columnas disponibles: {input_fieldnames}")
            sys.exit(1)

        if not nombre_col:
            logger.error("No se encontro columna de nombre en el CSV")
            logger.error(f"Columnas disponibles: {input_fieldnames}")
            sys.exit(1)

        logger.info(f"Usando columna DNI: {dni_col}")
        logger.info(f"Usando columna Nombre: {nombre_col}")

        for row in reader:
            dni = row.get(dni_col, '').strip()
            if dni and dni not in processed:
                registros.append(row)

    # Fieldnames para el archivo de salida (columnas originales + Ubicacion)
    output_fieldnames = list(input_fieldnames) + ['Ubicacion']

    total = len(registros)
    logger.info(f"Total DNIs a procesar: {total}")

    if total == 0:
        logger.info("No hay DNIs nuevos para procesar.")
        return

    # Countdown antes de empezar
    logger.info(f"Iniciando en {start_delay} segundos...")
    logger.info("(Mueve el mouse a la esquina superior izquierda para cancelar)")
    time.sleep(start_delay)

    # Procesar cada DNI
    exitosos = 0
    fallidos = 0
    consecutive_failures = 0  # Contador de fallos consecutivos (vpn_issue o error)
    failed_rows = []  # Registros que fallaron consecutivamente

    # Tracking de eventos VPN
    vpn_events = []  # Lista de eventos de caida de VPN
    total_retries = 0
    total_retries_exitosos = 0

    # Control de header CSV (solo escribir una vez)
    header_written = results_file.exists() and results_file.stat().st_size > 0

    i = 0
    while i < len(registros):
        row_data = registros[i]
        dni = row_data.get(dni_col, '').strip()
        logger.info(f"[{i+1}/{total}] ({exitosos} exitosos, {fallidos} fallidos)")

        # Determinar si escribir header (solo si archivo vacío/nuevo)
        write_header = not header_written

        result = process_dni(
            row_data, dni_col, nombre_col, coords,
            results_file, failures_file, output_fieldnames, write_header
        )

        # Si fue exitoso, marcar que el header ya fue escrito
        if result == "ok" and not header_written:
            header_written = True

        if result == "ok":
            exitosos += 1
            consecutive_failures = 0
            failed_rows = []
        elif result == "vpn_issue" or result == "error":
            # Cualquier fallo (sin nombre o nombre no coincide) cuenta
            fallidos += 1
            consecutive_failures += 1
            failed_rows.append(row_data)

            # Si hay 3+ fallos consecutivos, verificar si es problema de sistema
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                failed_dnis_list = [r.get(dni_col, '') for r in failed_rows]
                logger.warning("*" * 60)
                logger.warning("DETENCION POR FALLOS CONSECUTIVOS")
                logger.warning("*" * 60)
                logger.warning(f"Detectados {consecutive_failures} fallos consecutivos")
                logger.warning(f"DNIs afectados: {failed_dnis_list}")
                logger.info("Verificando conectividad y estado del sistema...")

                # Log del evento
                with vpn_log_file.open('a', encoding='utf-8') as f:
                    f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {consecutive_failures} fallos consecutivos - DNIs: {', '.join(failed_dnis_list)}\n")

                if not check_vpn():
                    # VPN caida - esperar reconexion (el scraping se detiene aqui)
                    vpn_event = wait_for_vpn(vpn_log_file)
                    vpn_event['dnis_afectados'] = failed_dnis_list
                    vpn_events.append(vpn_event)

                    # Click de reconexion y Enter para activar el sistema
                    reconnect_click_action(coords, vpn_log_file)

                    # Limpiar popups que pueden haber aparecido
                    clear_vpn_popup(coords, vpn_log_file)

                    # Reintentar los DNIs que fallaron por VPN
                    logger.info("=" * 60)
                    logger.info(f"REINTENTANDO {len(failed_rows)} DNIs QUE FALLARON POR VPN")
                    logger.info("=" * 60)

                    # Log inicio de reintentos
                    with vpn_log_file.open('a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                               f"Reintentos iniciados - Total: {len(failed_rows)}\n")

                    retries_ok = 0
                    retries_fail = 0
                    for j, retry_row in enumerate(failed_rows, 1):
                        retry_dni = retry_row.get(dni_col, '').strip()
                        logger.info(f"[REINTENTO {j}/{len(failed_rows)}] DNI: {retry_dni}")
                        retry_result = process_dni(
                            retry_row, dni_col, nombre_col, coords,
                            results_file, failures_file, output_fieldnames, not header_written
                        )
                        total_retries += 1
                        if retry_result == "ok":
                            retries_ok += 1
                            total_retries_exitosos += 1
                            exitosos += 1
                            fallidos -= 1  # Descontar el fallo anterior
                            if not header_written:
                                header_written = True
                            logger.info(f"  -> EXITO en reintento")
                            # Log reintento exitoso
                            with vpn_log_file.open('a', encoding='utf-8') as f:
                                f.write(f"  DNI {retry_dni}: EXITO\n")
                        else:
                            retries_fail += 1
                            logger.warning(f"  -> FALLO en reintento ({retry_result})")
                            # Log reintento fallido
                            with vpn_log_file.open('a', encoding='utf-8') as f:
                                f.write(f"  DNI {retry_dni}: FALLO ({retry_result})\n")

                    logger.info(f"Resultado reintentos: {retries_ok} exitosos, {retries_fail} fallidos")
                    logger.info("=" * 60)
                    logger.info("REANUDANDO SCRAPING NORMAL...")
                    logger.info("=" * 60)

                    # Log resumen de reintentos
                    with vpn_log_file.open('a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reintentos finalizados - Exitosos: {retries_ok} - Fallidos: {retries_fail}\n")

                    vpn_event['retries_ok'] = retries_ok
                    vpn_event['retries_fail'] = retries_fail

                    # Reiniciar contadores
                    consecutive_failures = 0
                    failed_rows = []
                else:
                    # VPN esta bien, puede ser otro problema (popup, error del sistema, etc)
                    logger.info("VPN activa (ping OK) - detectando otro problema...")
                    logger.info("Verificando si hay popup o cartel bloqueando...")

                    with vpn_log_file.open('a', encoding='utf-8') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                               f"VPN activa (ping OK) - Otro problema detectado\n")

                    # Verificar si sistema está bloqueado y ejecutar recuperación
                    if check_system_blocked(coords):
                        execute_system_recovery(coords, vpn_log_file)
                    else:
                        logger.debug("Popup verificado OK - sin bloqueos detectados")

                    consecutive_failures = 0
                    failed_rows = []
        else:  # error
            fallidos += 1
            consecutive_failures = 0
            failed_rows = []

        i += 1
        # Pequena pausa entre DNIs
        time.sleep(DELAY_BETWEEN_DNIS)

    # Resumen final
    logger.info("=" * 50)
    logger.info("RESUMEN FINAL")
    logger.info("=" * 50)
    logger.info(f"Total procesados: {exitosos + fallidos}")
    logger.info(f"Exitosos: {exitosos}")
    logger.info(f"Fallidos: {fallidos}")
    logger.info(f"Resultados en: {results_file}")
    logger.info(f"Fallos en: {failures_file}")

    # Resumen de eventos VPN
    if vpn_events:
        logger.info("-" * 50)
        logger.info(f"EVENTOS DE VPN ({len(vpn_events)} desconexiones)")
        logger.info("-" * 50)

        for idx, event in enumerate(vpn_events, 1):
            logger.info(f"  Evento #{idx}:")
            logger.info(f"    Inicio: {event['start'].strftime('%H:%M:%S')}")
            logger.info(f"    Fin: {event['end'].strftime('%H:%M:%S')}")
            logger.info(f"    Duracion: {int(event['duration_seconds'] // 60)}m {int(event['duration_seconds'] % 60)}s")
            logger.info(f"    Pings realizados: {event['ping_attempts']}")
            logger.info(f"    DNIs afectados: {event.get('dnis_afectados', [])}")
            total_event_retries = event.get('retries_ok', 0) + event.get('retries_fail', 0)
            logger.info(f"    Reintentos exitosos: {event.get('retries_ok', 0)}/{total_event_retries}")

        total_duration = sum(e['duration_seconds'] for e in vpn_events)
        total_pings = sum(e['ping_attempts'] for e in vpn_events)

        logger.info("  TOTALES VPN:")
        logger.info(f"    Desconexiones: {len(vpn_events)}")
        logger.info(f"    Tiempo total caido: {int(total_duration // 60)}m {int(total_duration % 60)}s")
        logger.info(f"    Total pings: {total_pings}")
        logger.info(f"    Reintentos: {total_retries} ({total_retries_exitosos} exitosos)")

        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sesion finalizada - "
                   f"Desconexiones: {len(vpn_events)} - Tiempo caido: {int(total_duration // 60)}m "
                   f"{int(total_duration % 60)}s - Pings: {total_pings} - "
                   f"Reintentos: {total_retries_exitosos}/{total_retries}\n")

        logger.info(f"Log detallado de VPN guardado en: {vpn_log_file}")
    else:
        # Si no hubo eventos VPN, cerrar el log
        with vpn_log_file.open('a', encoding='utf-8') as f:
            f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                   f"Sesion finalizada - Sin caidas de VPN\n")


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Extraccion masiva de direcciones por DNI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  %(prog)s --csv dnis.csv
  %(prog)s --csv dnis.csv --coords mis_coords.json
  %(prog)s --csv dnis.csv --output-dir resultados --start-delay 5

Archivo CSV debe contener columnas 'DNI' y 'nombre' (o variantes).
Archivo JSON debe contener coordenadas de clicks para todas las acciones.
        """
    )
    parser.add_argument(
        '--csv',
        required=True,
        help='Archivo CSV con los DNIs y nombres'
    )
    parser.add_argument(
        '--coords',
        default='camino-lote-masivo.json',
        help=f'Archivo JSON con coordenadas (default: camino-lote-masivo.json)'
    )
    parser.add_argument(
        '--output-dir',
        default=OUTPUT_DIR_DEFAULT,
        help=f'Directorio para archivos de salida (default: {OUTPUT_DIR_DEFAULT})'
    )
    parser.add_argument(
        '--start-delay',
        type=float,
        default=3.0,
        help='Segundos de espera antes de empezar (default: 3.0)'
    )

    args = parser.parse_args()

    try:
        run(
            csv_path=Path(args.csv),
            coords_path=Path(args.coords),
            output_dir=Path(args.output_dir),
            start_delay=args.start_delay
        )
    except KeyboardInterrupt:
        # Usar logger si está configurado, sino print
        try:
            logger.critical("\n\nInterrumpido por usuario")
        except:
            print("\n\nInterrumpido por usuario")
        sys.exit(130)
