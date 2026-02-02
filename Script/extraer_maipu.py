#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para extraer DNI y direcciones por códigos postales específicos del archivo TSV de resultados
"""

import csv
import re
from datetime import datetime

# Archivo de entrada
ARCHIVO_ENTRADA = 'resultados_20260130_095244.tsv'

# Generar nombre de archivo de salida con timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
ARCHIVO_SALIDA = f'maipu_resultados_{timestamp}.tsv'

# Códigos postales a buscar (sin el prefijo M)
CODIGOS_POSTALES = [
    '5511', '5513', '5515', '5516', '5517', '5522', '5529', '5531',
    '5578', '5581', '5586', '5587', '5588', '5589', '5591', '5593',
    '5606', '5650', '5651', '5655', '5656', '5658', '5672'
]

def extraer_maipu():
    """
    Extrae todos los registros que contengan códigos postales específicos en la dirección
    """
    registros_maipu = []
    total_registros = 0

    print(f"Leyendo archivo: {ARCHIVO_ENTRADA}")
    print(f"Buscando códigos postales: {', '.join(CODIGOS_POSTALES)}\n")

    try:
        with open(ARCHIVO_ENTRADA, 'r', encoding='utf-8') as archivo:
            # Leer el archivo TSV
            lector = csv.reader(archivo, delimiter='\t')

            for fila in lector:
                total_registros += 1

                if len(fila) >= 2:
                    dni = fila[0]
                    direccion = fila[1]

                    # Buscar códigos postales en la dirección
                    # Busca tanto formato "M5515" como "5515"
                    for codigo in CODIGOS_POSTALES:
                        # Patrón: busca el código con o sin M, con límites de palabra
                        patron = r'\b(M?' + codigo + r')\b'
                        if re.search(patron, direccion, re.IGNORECASE):
                            registros_maipu.append((dni, direccion))
                            print(f"✓ Encontrado (CP: {codigo}): {dni} - {direccion}")
                            break  # Evitar duplicados si hay múltiples códigos en la misma dirección

        # Guardar resultados
        if registros_maipu:
            with open(ARCHIVO_SALIDA, 'w', encoding='utf-8', newline='') as archivo_salida:
                escritor = csv.writer(archivo_salida, delimiter='\t')

                # Escribir encabezado
                escritor.writerow(['DNI', 'DIRECCION'])

                # Escribir registros
                for dni, direccion in registros_maipu:
                    escritor.writerow([dni, direccion])

            print(f"\n{'='*70}")
            print(f"Resumen:")
            print(f"  Total de registros procesados: {total_registros}")
            print(f"  Registros encontrados: {len(registros_maipu)}")
            print(f"  Archivo de salida: {ARCHIVO_SALIDA}")
            print(f"{'='*70}")
        else:
            print("\n⚠️  No se encontraron registros con los códigos postales especificados")

    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo {ARCHIVO_ENTRADA}")
    except Exception as e:
        print(f"❌ Error al procesar el archivo: {e}")

if __name__ == '__main__':
    extraer_maipu()
