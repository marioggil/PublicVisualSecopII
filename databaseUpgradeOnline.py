from pydal import DAL, Field
from datetime import datetime
import os
import json
import pandas as pd
from sodapy import Socrata
import time
from models.db import db
from datetime import datetime, timedelta

def estimar_tiempo_restante(total, procesados, tiempo_transcurrido):
    """
    Calcula el tiempo estimado restante para completar un proceso.
    
    Args:
        total (int): Cantidad total de elementos a procesar
        procesados (int): Cantidad de elementos ya procesados
        tiempo_transcurrido (float): Tiempo en segundos que ha tomado procesar los elementos actuales
    
    Returns:
        dict: Diccionario con:
            - segundos_restantes (float): Segundos estimados para completar
            - fecha_hora_fin (datetime): Fecha y hora estimada de finalización
            - velocidad (float): Elementos procesados por segundo
            - porcentaje_completado (float): Porcentaje del proceso completado
    
    Raises:
        ValueError: Si los valores de entrada no son válidos
    """
    # Validaciones
    if total <= 0:
        raise ValueError("El total debe ser mayor a 0")
    
    if procesados < 0:
        raise ValueError("Los elementos procesados no pueden ser negativos")
    
    if procesados > total:
        raise ValueError("Los elementos procesados no pueden ser mayores al total")
    
    if tiempo_transcurrido < 0:
        raise ValueError("El tiempo transcurrido no puede ser negativo")
    
    # Si ya se completó todo
    if procesados >= total:
        return {
            'segundos_restantes': 0,
            'fecha_hora_fin': datetime.now(),
            'velocidad': procesados / tiempo_transcurrido if tiempo_transcurrido > 0 else 0,
            'porcentaje_completado': 100.0
        }
    
    # Si no se ha procesado nada aún o no ha transcurrido tiempo
    if procesados == 0 or tiempo_transcurrido == 0:
        return {
            'segundos_restantes': None,
            'fecha_hora_fin': None,
            'velocidad': 0,
            'porcentaje_completado': 0.0
        }
    
    # Calcular velocidad de procesamiento (elementos por segundo)
    velocidad = procesados / tiempo_transcurrido
    
    # Calcular elementos restantes
    elementos_restantes = total - procesados
    
    # Calcular tiempo restante en segundos
    segundos_restantes = elementos_restantes / velocidad
    
    # Calcular fecha y hora de finalización
    fecha_hora_fin = datetime.now() + timedelta(seconds=segundos_restantes)
    
    # Calcular porcentaje completado
    porcentaje_completado = (procesados / total) * 100
    
    return {
        'segundos_restantes': segundos_restantes,
        'fecha_hora_fin': fecha_hora_fin,
        'velocidad': velocidad,
        'porcentaje_completado': porcentaje_completado
    }


def formatear_tiempo(segundos):
    """
    Formatea segundos en una cadena legible (días, horas, minutos, segundos).
    
    Args:
        segundos (float): Cantidad de segundos
    
    Returns:
        str: Tiempo formateado en formato legible
    """
    if segundos is None:
        return "Tiempo no disponible"
    
    if segundos < 0:
        return "Tiempo inválido"
    
    dias = int(segundos // 86400)
    horas = int((segundos % 86400) // 3600)
    minutos = int((segundos % 3600) // 60)
    segs = int(segundos % 60)
    
    partes = []
    if dias > 0:
        partes.append(f"{dias} día{'s' if dias != 1 else ''}")
    if horas > 0:
        partes.append(f"{horas} hora{'s' if horas != 1 else ''}")
    if minutos > 0:
        partes.append(f"{minutos} minuto{'s' if minutos != 1 else ''}")
    if segs > 0 or not partes:
        partes.append(f"{segs} segundo{'s' if segs != 1 else ''}")
    
    return ", ".join(partes)

# Configuración de la base de datos

# Mapeo de nombres de columnas (original -> nombre en BD)
COLUMN_MAPPING = {
    'nombre_entidad': None,  # Removido de la BD
    'nit_entidad': 'nit_entidad',
    'departamento': None,  # Removido de la BD
    'ciudad': None,  # Removido de la BD
    'localizaci_n': None,  # Removido de la BD
    'orden': None,  # Removido de la BD
    'sector': None,  # Removido de la BD
    'rama': None,  # Removido de la BD
    'entidad_centralizada': None,  # Removido de la BD
    'proceso_de_compra': 'proceso_compra',
    'id_contrato': 'id_contrato',
    'referencia_del_contrato': 'referencia_contrato',
    'estado_contrato': 'estado_contrato',
    'codigo_de_categoria_principal': 'codigo_categoria_principal',
    'descripcion_del_proceso': 'descripcion_proceso',
    'tipo_de_contrato': 'tipo_contrato',
    'modalidad_de_contratacion': 'modalidad_contratacion',
    'justificacion_modalidad_de': 'justificacion_modalidad',
    'fecha_de_firma': 'fecha_firma',
    'fecha_de_inicio_del_contrato': 'fecha_inicio',
    'fecha_de_fin_del_contrato': 'fecha_fin',
    'condiciones_de_entrega': 'condiciones_entrega',
    'tipodocproveedor': None,  # Removido de la BD
    'documento_proveedor': 'documento_proveedor',
    'proveedor_adjudicado': None,  # Removido de la BD
    'es_grupo': None,  # Removido de la BD
    'es_pyme': None,  # Removido de la BD
    'habilita_pago_adelantado': 'habilita_pago_adelantado',
    'liquidaci_n': 'liquidacion',
    'obligaci_n_ambiental': 'obligacion_ambiental',
    'obligaciones_postconsumo': 'obligaciones_postconsumo',
    'reversion': 'reversion',
    'origen_de_los_recursos': 'origen_recursos',
    'destino_gasto': 'destino_gasto',
    'valor_del_contrato': 'valor_contrato',
    'valor_de_pago_adelantado': 'valor_pago_adelantado',
    'valor_facturado': 'valor_facturado',
    'valor_pendiente_de_pago': 'valor_pendiente_pago',
    'valor_pagado': 'valor_pagado',
    'valor_amortizado': 'valor_amortizado',
    'valor_pendiente_de': 'valor_pendiente_amortizacion',
    'valor_pendiente_de_ejecucion': 'valor_pendiente_ejecucion',
    'estado_bpin': 'estado_bpin',
    'c_digo_bpin': 'codigo_bpin',
    'anno_bpin': 'anno_bpin',
    'saldo_cdp': 'saldo_cdp',
    'saldo_vigencia': 'saldo_vigencia',
    'espostconflicto': 'es_postconflicto',
    'dias_adicionados': 'dias_adicionados',
    'puntos_del_acuerdo': 'puntos_acuerdo',
    'pilares_del_acuerdo': 'pilares_acuerdo',
    'urlproceso': 'url_proceso',
    'nombre_representante_legal': None,  # Removido de la BD
    'nacionalidad_representante_legal': None,  # Removido de la BD
    'domicilio_representante_legal': None,  # Removido de la BD
    'tipo_de_identificaci_n_representante_legal': None,  # Removido de la BD
    'identificaci_n_representante_legal': 'identificacion_representante_legal',
    'g_nero_representante_legal': None,  # Removido de la BD
    'presupuesto_general_de_la_nacion_pgn': 'presupuesto_pgn',
    'sistema_general_de_participaciones': 'sistema_participaciones',
    'sistema_general_de_regal_as': 'sistema_regalias',
    'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_': 'recursos_propios_alcaldias',
    'recursos_de_credito': 'recursos_credito',
    'recursos_propios': 'recursos_propios',
    'ultima_actualizacion': 'ultima_actualizacion',
    'codigo_entidad': 'codigo_entidad',
    'codigo_proveedor': 'codigo_proveedor',
    'fecha_inicio_liquidacion': 'fecha_inicio_liquidacion',
    'fecha_fin_liquidacion': 'fecha_fin_liquidacion',
    'objeto_del_contrato': 'objeto_contrato',
    'duraci_n_del_contrato': 'duracion_contrato',
    'nombre_del_banco': 'nombre_banco',
    'tipo_de_cuenta': 'tipo_cuenta',
    'n_mero_de_cuenta': 'numero_cuenta',
    'el_contrato_puede_ser_prorrogado': 'puede_prorrogarse',
    'nombre_ordenador_del_gasto': None,  # Removido de la BD
    'tipo_de_documento_ordenador_del_gasto': None,  # Removido de la BD
    'n_mero_de_documento_ordenador_del_gasto': 'num_doc_ordenador_gasto',
    'nombre_supervisor': None,  # Removido de la BD
    'tipo_de_documento_supervisor': None,  # Removido de la BD
    'n_mero_de_documento_supervisor': 'num_doc_supervisor',
    'nombre_ordenador_de_pago': None,  # Removido de la BD
    'tipo_de_documento_ordenador_de_pago': None,  # Removido de la BD
    'n_mero_de_documento_ordenador_de_pago': 'num_doc_ordenador_pago'
}

def limpiar_valor(valor, campo=None):
    """
    Convierte valores 'No Definido', 'No definido', etc. a None.
    También maneja diccionarios extrayendo valores específicos.
    Limpia caracteres especiales de campos numéricos específicos.
    
    Args:
        valor: El valor a limpiar
        campo: Nombre del campo (para aplicar limpieza específica)
    """
    if isinstance(valor, dict):
        # Si es un diccionario, intenta extraer el valor 'url' o devuelve None
        return valor.get('url') if 'url' in valor else None
    
    if isinstance(valor, str):
        valores_nulos = ['no definido', 'no válido', 'sin descripcion', 'sin descripción']
        if valor.strip().lower() in valores_nulos:
            return None
        
        # Campos que deben tener limpieza de caracteres especiales
        campos_numericos = [
            'nit_entidad',
            'num_doc_ordenador_gasto',
            'num_doc_supervisor',
            'num_doc_ordenador_pago'
        ]
        
        if campo in campos_numericos:
            # Eliminar puntos, comas y comillas simples
            valor = valor.replace('.', '').replace(',', '').replace("'", '').replace("-","").strip()
    
    return valor


def transformar_nombres_columnas(registro, mapeo=None):
    """
    Transforma los nombres de las columnas según el mapeo proporcionado.
    Elimina campos que no existen en la nueva estructura de BD (valor None en mapeo).
    
    Args:
        registro (dict): Diccionario con los datos originales
        mapeo (dict): Diccionario con el mapeo de nombres (original -> nuevo)
                    Si es None, usa COLUMN_MAPPING por defecto
    
    Returns:
        dict: Diccionario con los nombres transformados y solo campos válidos
    """
    if mapeo is None:
        mapeo = COLUMN_MAPPING
    
    registro_transformado = {}
    
    for key_original, valor in registro.items():
        # Obtiene el nombre mapeado
        key_nuevo = mapeo.get(key_original, key_original)
        
        # Si el mapeo es None, significa que ese campo fue removido de la BD
        if key_nuevo is None:
            continue
        
        # Agrega el campo transformado con limpieza específica por campo
        registro_transformado[key_nuevo] = limpiar_valor(valor, campo=key_nuevo)
    
    return registro_transformado


def guardar_contratos(datos, tabla='contratos', mapeo=None):
    """
    Guarda una lista de contratos en la base de datos.
    
    Args:
        datos (list): Lista de diccionarios con los datos de contratos
        tabla (str): Nombre de la tabla donde guardar
        mapeo (dict): Mapeo de nombres de columnas (opcional)
    
    Returns:
        dict: Estadísticas de la operación (insertados, errores)
    """
    stats = {
        'insertados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    try:
        for i, registro in enumerate(datos):
            try:
                # Transformar nombres de columnas y limpiar valores
                registro_limpio = transformar_nombres_columnas(registro, mapeo)
                
                # Insertar en la base de datos
                db[tabla].insert(**registro_limpio)
                stats['insertados'] += 1
                
            except Exception as e:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'error': str(e),
                    'registro': registro.get('id_contrato', 'N/A')
                })
        
        # Confirmar transacción
        db.commit()
        
    except Exception as e:
        db.rollback()
        stats['error_general'] = str(e)
    
    return stats

# Mapeo de nombres de columnas para adiciones (original -> nombre en BD)
ADICIONES_COLUMN_MAPPING = {
    'identificador': 'id_adicion',
    'id_contrato': 'id_contrato',
    'tipo': 'tipo_modificacion',
    'descripcion': 'descripcion',
    'fecharegistro': 'fecha_registro'
}



def limpiar_valor_adicion(valor):
    """
    Convierte valores 'No Definido', 'No definido', etc. a None.
    También maneja diccionarios y otros casos especiales.
    
    Args:
        valor: Valor a limpiar
    
    Returns:
        Valor limpio o None
    """
    if isinstance(valor, dict):
        # Si es un diccionario, intenta extraer el valor 'url' o devuelve None
        return valor.get('url') if 'url' in valor else None
    
    if isinstance(valor, str):
        valores_nulos = [
            'no definido',
            'no válido',
            'sin descripcion',
            'sin descripción',
            'n/a',
            'na'
        ]
        if valor.strip().lower() in valores_nulos:
            return None
    
    return valor


def transformar_nombres_columnas_adicion(registro, mapeo=None):
    """
    Transforma los nombres de las columnas según el mapeo proporcionado.
    
    Args:
        registro (dict): Diccionario con los datos originales
        mapeo (dict): Diccionario con el mapeo de nombres (original -> nuevo)
                    Si es None, usa ADICIONES_COLUMN_MAPPING por defecto
    
    Returns:
        dict: Diccionario con los nombres transformados y valores limpios
    """
    if mapeo is None:
        mapeo = ADICIONES_COLUMN_MAPPING
    
    registro_transformado = {}
    
    for key_original, valor in registro.items():
        # Usa el nombre mapeado o el original si no existe en el mapeo
        key_nuevo = mapeo.get(key_original, key_original)
        registro_transformado[key_nuevo] = limpiar_valor_adicion(valor)
    
    return registro_transformado


def guardar_adiciones(datos, tabla='adiciones', mapeo=None, actualizar_existentes=False):
    """
    Guarda una lista de adiciones/modificaciones en la base de datos.
    
    Args:
        datos (list): Lista de diccionarios con los datos de adiciones
        tabla (str): Nombre de la tabla donde guardar (default: 'adiciones')
        mapeo (dict): Mapeo de nombres de columnas (opcional)
        actualizar_existentes (bool): Si True, actualiza registros existentes en lugar de fallar
    
    Returns:
        dict: Estadísticas de la operación (insertados, actualizados, errores)
    """
    stats = {
        'insertados': 0,
        'actualizados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    try:
        for i, registro in enumerate(datos):
            try:
                # Transformar nombres de columnas y limpiar valores
                registro_limpio = transformar_nombres_columnas_adicion(registro, mapeo)
                
                # Verificar si el registro ya existe (por id_adicion)
                id_adicion = registro_limpio.get('id_adicion')
                
                if actualizar_existentes and id_adicion:
                    # Buscar registro existente
                    existe = db(db[tabla].id_adicion == id_adicion).select().first()
                    
                    if existe:
                        # Actualizar registro existente
                        db(db[tabla].id_adicion == id_adicion).update(**registro_limpio)
                        stats['actualizados'] += 1
                    else:
                        # Insertar nuevo registro
                        db[tabla].insert(**registro_limpio)
                        stats['insertados'] += 1
                else:
                    # Insertar en la base de datos
                    db[tabla].insert(**registro_limpio)
                    stats['insertados'] += 1
                
            except Exception as e:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'error': str(e),
                    'registro': registro.get('identificador', 'N/A')
                })
        
        
        
        
    except Exception as e:
        db.rollback()
        stats['error_general'] = str(e)
    
    return stats


# Mapeo de nombres de columnas para ejecuciones (original -> nombre en BD)
EJECUCIONES_COLUMN_MAPPING = {
    'identificadorcontrato': 'id_contrato',
    'tipoejecucion': 'tipo_ejecucion',
    'nombreplan': 'nombre_plan',
    'fechadeentregaesperada': 'fecha_entrega_esperada',
    'porcentajedeavanceesperado': 'porcentaje_avance_esperado',
    'fechadeentregareal': 'fecha_entrega_real',
    'porcentaje_de_avance_real': 'porcentaje_avance_real',
    'estado_del_contrato': 'estado_contrato',
    'referencia_de_articulos': 'referencia_articulos',
    'descripci_n': 'descripcion',
    'unidad': 'unidad',
    'cantidad_adjudicada': 'cantidad_adjudicada',
    'cantidad_planeada': 'cantidad_planeada',
    'cantidadrecibida': 'cantidad_recibida',
    'cantidadporrecibir': 'cantidad_por_recibir',
    'fechacreacion': 'fecha_creacion'
}




def limpiar_valor_ejecucion(valor):
    """
    Convierte valores 'No Definido', 'No definido', etc. a None.
    También maneja valores numéricos con formatos especiales.
    
    Args:
        valor: Valor a limpiar
    
    Returns:
        Valor limpio o None
    """
    if isinstance(valor, dict):
        # Si es un diccionario, intenta extraer el valor 'url' o devuelve None
        return valor.get('url') if 'url' in valor else None
    
    if isinstance(valor, str):
        # Limpiar espacios
        valor_limpio = valor.strip()
        
        # Valores considerados nulos
        valores_nulos = [
            'no definido',
            'no válido',
            'sin descripcion',
            'sin descripción',
            'n/a',
            'na'
        ]
        
        if valor_limpio.lower() in valores_nulos:
            return None
        
        # Manejar valores numéricos mal formateados como '.000000'
        if valor_limpio.startswith('.') and all(c in '0.' for c in valor_limpio):
            return '0'
    
    return valor


def transformar_nombres_columnas_ejecucion(registro, mapeo=None):
    """
    Transforma los nombres de las columnas según el mapeo proporcionado.
    
    Args:
        registro (dict): Diccionario con los datos originales
        mapeo (dict): Diccionario con el mapeo de nombres (original -> nuevo)
                    Si es None, usa EJECUCIONES_COLUMN_MAPPING por defecto
    
    Returns:
        dict: Diccionario con los nombres transformados y valores limpios
    """
    if mapeo is None:
        mapeo = EJECUCIONES_COLUMN_MAPPING
    
    registro_transformado = {}
    
    for key_original, valor in registro.items():
        # Usa el nombre mapeado o el original si no existe en el mapeo
        key_nuevo = mapeo.get(key_original, key_original)
        registro_transformado[key_nuevo] = limpiar_valor_ejecucion(valor)
    
    return registro_transformado


def guardar_ejecuciones(datos, tabla='ejecuciones', mapeo=None, batch_size=100):
    """
    Guarda una lista de ejecuciones de contratos en la base de datos.
    
    Args:
        datos (list): Lista de diccionarios con los datos de ejecuciones
        tabla (str): Nombre de la tabla donde guardar (default: 'ejecuciones')
        mapeo (dict): Mapeo de nombres de columnas (opcional)
        batch_size (int): Tamaño del lote para inserciones masivas
    
    Returns:
        dict: Estadísticas de la operación (insertados, errores)
    """
    stats = {
        'insertados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    try:
        for i, registro in enumerate(datos):
            try:
                # Transformar nombres de columnas y limpiar valores
                registro_limpio = transformar_nombres_columnas_ejecucion(registro, mapeo)
                
                # Insertar en la base de datos
                db[tabla].insert(**registro_limpio)
                stats['insertados'] += 1
                
                # Commit cada batch_size registros para mejorar rendimiento
                if (i + 1) % batch_size == 0:
                    db.commit()
                
            except Exception as e:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'error': str(e),
                    'registro': registro.get('identificadorcontrato', 'N/A')
                })
        
        # Confirmar transacción final
        db.commit()
        
    except Exception as e:
        db.rollback()
        stats['error_general'] = str(e)
    
    return stats

def normalizar_documento(documento):
    """
    Normaliza un documento eliminando espacios y caracteres innecesarios.
    """
    if not documento:
        return None
    
    doc_limpio = str(documento).strip().replace(' ', '')
    return doc_limpio if doc_limpio else None


def parsear_fecha(fecha_str):
    """
    Parsea una fecha en diferentes formatos.
    Soporta: DD/MM/YYYY, YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS.fff
    """
    if not fecha_str:
        return None
    
    fecha_limpia = limpiar_valor(fecha_str)
    if not fecha_limpia:
        return None
    
    # Formatos comunes
    formatos = [
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S'
    ]
    
    for formato in formatos:
        try:
            return datetime.strptime(fecha_limpia, formato).date()
        except ValueError:
            continue
    
    return None


# ============================================================================
# FUNCIONES PARA SANCIONADOS SIRI
# ============================================================================

def extraer_datos_siri(datos_siri):
    """
    Extrae los campos relevantes de un registro SIRI.
    
    Args:
        datos_siri (dict): Diccionario con datos del SIRI
    
    Returns:
        dict: Datos normalizados
    """
    documento = normalizar_documento(datos_siri.get('numero_identificacion'))
    
    if not documento:
        return None
    
    # Construir nombre completo para comparación
    primer_nombre = limpiar_valor(datos_siri.get('primer_nombre'))
    segundo_nombre = limpiar_valor(datos_siri.get('segundo_nombre'))
    primer_apellido = limpiar_valor(datos_siri.get('primer_apellido'))
    segundo_apellido = limpiar_valor(datos_siri.get('segundo_apellido'))
    
    return {
        'documento': documento,
        'tipo_inhabilitacion': limpiar_valor(datos_siri.get('tipo_inhabilidad')),
        'primer_nombre': primer_nombre,
        'segundo_nombre': segundo_nombre,
        'primer_apellido': primer_apellido,
        'segundo_apellido': segundo_apellido,
        'nombre_completo': " ".join([primer_nombre,segundo_nombre,primer_apellido,segundo_apellido]),
        'sancion': limpiar_valor(datos_siri.get('sanciones')),
        'fecha_efectos_juridicos': parsear_fecha(datos_siri.get('fecha_efectos_juridicos')),
        'numero_resolucion': None,
        'origen': 'SIRI'
    }


# ============================================================================
# FUNCIONES PARA AMONESTADOS SECOP
# ============================================================================

def extraer_datos_secop(datos_secop):
    """
    Extrae los campos relevantes de un registro SECOP.
    
    Args:
        datos_secop (dict): Diccionario con datos del SECOP
    
    Returns:
        dict: Datos normalizados
    """
    documento = normalizar_documento(datos_secop.get('documento_contratista'))
    
    if not documento:
        return None
    
    return {
        'documento': documento,
        'tipo_inhabilitacion': 'AMONESTACION',  # Tipo fijo para SECOP
        'primer_nombre': None,
        'segundo_nombre': None,
        'primer_apellido': None,
        'segundo_apellido': None,
        'nombre_completo': limpiar_valor(datos_secop.get('nombre_contratista')),
        'sancion': None,
        'fecha_efectos_juridicos': parsear_fecha(datos_secop.get('fecha_de_firmeza')),
        'numero_resolucion': limpiar_valor(datos_secop.get('numero_de_resolucion')),
        'origen': 'SECOP'
    }


# ============================================================================
# FUNCIÓN PRINCIPAL DE GUARDADO
# ============================================================================

def verificar_registro_diferente(documento, nuevos_datos):
    """
    Verifica si el nuevo registro tiene al menos un campo diferente
    a los registros existentes para el mismo documento.
    
    Args:
        documento (str): Número de documento
        nuevos_datos (dict): Nuevos datos a comparar
    
    Returns:
        bool: True si hay diferencias, False si es duplicado
    """
    registros_existentes = db(db.sancionados.documento == documento).select()
    
    if not registros_existentes:
        return True  # No hay registros, es nuevo
    
    # Campos a comparar (excluyendo id, documento y origen)
    campos_comparar = [
        'tipo_inhabilitacion',
        'primer_nombre',
        'segundo_nombre',
        'primer_apellido',
        'segundo_apellido',
        'nombre_completo',
        'sancion',
        'fecha_efectos_juridicos',
        'numero_resolucion'
    ]
    
    for registro in registros_existentes:
        es_igual = True
        
        for campo in campos_comparar:
            valor_existente = getattr(registro, campo)
            valor_nuevo = nuevos_datos.get(campo)
            
            # Normalizar None y strings vacíos
            if valor_existente == '' or valor_existente is None:
                valor_existente = None
            if valor_nuevo == '' or valor_nuevo is None:
                valor_nuevo = None
            
            # Si encuentra alguna diferencia, el registro es diferente
            if valor_existente != valor_nuevo:
                es_igual = False
                break
        
        # Si encontró un registro idéntico, no insertar
        if es_igual:
            return False
    
    # No se encontró ningún registro idéntico
    return True


def guardar_sancionado(datos_normalizados):
    """
    Guarda un registro de sancionado/amonestado si es diferente.
    
    Args:
        datos_normalizados (dict): Datos ya normalizados
    
    Returns:
        dict: Resultado de la operación
    """
    resultado = {
        'exito': False,
        'accion': None,
        'mensaje': ''
    }
    
    try:
        documento = datos_normalizados.get('documento')
        
        if not documento:
            resultado['mensaje'] = 'Documento no válido'
            return resultado
        
        # Verificar si el registro es diferente
        if verificar_registro_diferente(documento, datos_normalizados):
            # Insertar nuevo registro
            db.sancionados.insert(**datos_normalizados)
            db.commit()
            
            resultado['exito'] = True
            resultado['accion'] = 'insertado'
            resultado['mensaje'] = f'Sanción registrada para documento {documento}'
        else:
            resultado['exito'] = True
            resultado['accion'] = 'duplicado'
            resultado['mensaje'] = f'Registro duplicado para documento {documento}, no se insertó'
        
    except Exception as e:
        db.rollback()
        resultado['mensaje'] = f'Error: {str(e)}'
    
    return resultado


def guardar_sancionado_siri(datos_siri):
    """
    Procesa y guarda un registro del SIRI.
    
    Args:
        datos_siri (dict): Diccionario con datos originales del SIRI
    
    Returns:
        dict: Resultado de la operación
    """
    datos_normalizados = extraer_datos_siri(datos_siri)
    
    if not datos_normalizados:
        return {
            'exito': False,
            'accion': None,
            'mensaje': 'No se pudieron extraer datos válidos del SIRI'
        }
    
    return guardar_sancionado(datos_normalizados)


def guardar_amonestado_secop(datos_secop):
    """
    Procesa y guarda un registro de SECOP.
    
    Args:
        datos_secop (dict): Diccionario con datos originales del SECOP
    
    Returns:
        dict: Resultado de la operación
    """
    datos_normalizados = extraer_datos_secop(datos_secop)
    
    if not datos_normalizados:
        return {
            'exito': False,
            'accion': None,
            'mensaje': 'No se pudieron extraer datos válidos del SECOP'
        }
    
    return guardar_sancionado(datos_normalizados)


# ============================================================================
# FUNCIONES DE PROCESAMIENTO MASIVO
# ============================================================================

def procesar_multiples_siri(lista_siri):
    """
    Procesa múltiples registros del SIRI.
    
    Args:
        lista_siri (list): Lista de diccionarios con datos del SIRI
    
    Returns:
        dict: Estadísticas del procesamiento
    """
    stats = {
        'total_procesados': 0,
        'insertados': 0,
        'duplicados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    for i, registro in enumerate(lista_siri):
        try:
            resultado = guardar_sancionado_siri(registro)
            stats['total_procesados'] += 1
            
            if resultado['exito']:
                if resultado['accion'] == 'insertado':
                    stats['insertados'] += 1
                elif resultado['accion'] == 'duplicado':
                    stats['duplicados'] += 1
            else:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'documento': registro.get('numero_identificacion', 'N/A'),
                    'error': resultado['mensaje']
                })
        
        except Exception as e:
            stats['errores'] += 1
            stats['detalles_errores'].append({
                'indice': i,
                'error': str(e)
            })
    
    return stats


def procesar_multiples_secop(lista_secop):
    """
    Procesa múltiples registros de SECOP.
    
    Args:
        lista_secop (list): Lista de diccionarios con datos del SECOP
    
    Returns:
        dict: Estadísticas del procesamiento
    """
    stats = {
        'total_procesados': 0,
        'insertados': 0,
        'duplicados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    for i, registro in enumerate(lista_secop):
        try:
            resultado = guardar_amonestado_secop(registro)
            stats['total_procesados'] += 1
            
            if resultado['exito']:
                if resultado['accion'] == 'insertado':
                    stats['insertados'] += 1
                elif resultado['accion'] == 'duplicado':
                    stats['duplicados'] += 1
            else:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'documento': registro.get('documento_contratista', 'N/A'),
                    'error': resultado['mensaje']
                })
        
        except Exception as e:
            stats['errores'] += 1
            stats['detalles_errores'].append({
                'indice': i,
                'error': str(e)
            })
    
    return stats

def limpiar_documento(documento):
    """
    Limpia el documento removiendo puntos, comas y comillas.
    """
    if isinstance(documento, str):
        return documento.replace('.', '').replace(',', '').replace("'", '').strip()
    return documento


def normalizar_valor_boolean(valor):
    """
    Convierte valores como 'Sí', 'No', 'Si' a 'Sí' o 'No' estandarizado.
    """
    if isinstance(valor, str):
        valor_lower = valor.lower().strip()
        if valor_lower in ['sí', 'si', 's', 'yes', 'y']:
            return 'Sí'
        elif valor_lower in ['no', 'n']:
            return 'No'
    return valor


def guardar_entidad_persona(datos):
    """
    Guarda o actualiza una entidad en la tabla entidades_personas.
    
    Args:
        datos (dict): Diccionario con los datos del JSON
        
    Returns:
        dict: Información sobre la operación realizada
    """
    resultado = {
        'operacion': None,
        'documento': None,
        'error': None
    }
    
    try:
        # Extraer y procesar datos de la entidad
        nit = limpiar_documento(datos.get('nit_entidad', ''))
        
        if not nit:
            resultado['error'] = 'NIT de entidad no proporcionado'
            return resultado
        
        # Preparar datos de la entidad
        datos_entidad = {
            'documento': nit,
            'tipo_documento': 'NIT',
            'nombre': datos.get('nombre_entidad'),
            'departamento': datos.get('departamento'),
            'ciudad': datos.get('ciudad'),
            'orden': datos.get('orden'),
            'sector': datos.get('sector'),
            'rama': datos.get('rama'),
            'entidad_centralizada': datos.get('entidad_centralizada'),
            'es_grupo': None,
            'es_pyme': None,
            'es_entidad': 'Sí',
            'es_proveedor': None,
            'es_representante_legal': None,
            'es_ordenador_del_gasto': None,
            'es_supervisor': None
        }
        
        # Buscar si ya existe la entidad
        registro_existente = db(db.entidades_personas.documento == nit).select().first()
        
        if registro_existente:
            # Actualizar solo los campos booleanos si están vacíos
            campos_a_actualizar = {}
            
            if not registro_existente.es_entidad or registro_existente.es_entidad == 'No':
                campos_a_actualizar['es_entidad'] = 'Sí'
            
            if campos_a_actualizar:
                registro_existente.update_record(**campos_a_actualizar)
                resultado['operacion'] = 'actualizado'
            else:
                resultado['operacion'] = 'sin_cambios'
        else:
            # Insertar nuevo registro
            db.entidades_personas.insert(**datos_entidad)
            resultado['operacion'] = 'insertado'
        
        resultado['documento'] = nit
        
    except Exception as e:
        resultado['error'] = str(e)
    
    return resultado


def guardar_proveedor(datos):
    """
    Guarda o actualiza un proveedor en la tabla entidades_personas.
    
    Args:
        datos (dict): Diccionario con los datos del JSON
        
    Returns:
        dict: Información sobre la operación realizada
    """
    resultado = {
        'operacion': None,
        'documento': None,
        'error': None
    }
    
    try:
        # Extraer y procesar datos del proveedor
        doc_proveedor = limpiar_documento(datos.get('documento_proveedor', ''))
        
        if not doc_proveedor:
            resultado['error'] = 'Documento de proveedor no proporcionado'
            return resultado
        
        tipo_doc = datos.get('tipodocproveedor', 'Cédula de Ciudadanía')
        nombre = datos.get('proveedor_adjudicado')
        es_grupo = normalizar_valor_boolean(datos.get('es_grupo', 'No'))
        es_pyme = normalizar_valor_boolean(datos.get('es_pyme', 'No'))
        
        # Preparar datos del proveedor
        datos_proveedor = {
            'documento': doc_proveedor,
            'tipo_documento': tipo_doc,
            'nombre': nombre,
            'es_grupo': es_grupo,
            'es_pyme': es_pyme,
            'es_entidad': None,
            'es_proveedor': 'Sí',
            'es_representante_legal': None,
            'es_ordenador_del_gasto': None,
            'es_supervisor': None
        }
        
        # Buscar si ya existe el proveedor
        registro_existente = db(db.entidades_personas.documento == doc_proveedor).select().first()
        
        if registro_existente:
            # Actualizar solo los campos booleanos si están vacíos
            campos_a_actualizar = {}
            
            if not registro_existente.es_proveedor or registro_existente.es_proveedor == 'No':
                campos_a_actualizar['es_proveedor'] = 'Sí'
            
            if not registro_existente.es_grupo:
                campos_a_actualizar['es_grupo'] = es_grupo
            
            if not registro_existente.es_pyme:
                campos_a_actualizar['es_pyme'] = es_pyme
            
            if campos_a_actualizar:
                registro_existente.update_record(**campos_a_actualizar)
                resultado['operacion'] = 'actualizado'
            else:
                resultado['operacion'] = 'sin_cambios'
        else:
            # Insertar nuevo registro
            db.entidades_personas.insert(**datos_proveedor)
            resultado['operacion'] = 'insertado'
        
        resultado['documento'] = doc_proveedor
        
    except Exception as e:
        resultado['error'] = str(e)
    
    return resultado


def guardar_representante_legal(datos):
    """
    Guarda o actualiza un representante legal en la tabla entidades_personas.
    
    Args:
        datos (dict): Diccionario con los datos del JSON
        
    Returns:
        dict: Información sobre la operación realizada
    """
    resultado = {
        'operacion': None,
        'documento': None,
        'error': None
    }
    
    try:
        # Extraer y procesar datos del representante legal
        doc_rep = limpiar_documento(datos.get('identificaci_n_representante_legal', ''))
        
        if not doc_rep or doc_rep.lower() in ['sin descripcion', 'no definido']:
            resultado['error'] = 'Documento de representante legal no válido'
            return resultado
        
        tipo_doc = datos.get('tipo_de_identificaci_n_representante_legal', 'Cédula de Ciudadanía')
        nombre = datos.get('nombre_representante_legal')
        nacionalidad = datos.get('nacionalidad_representante_legal')
        genero = datos.get('g_nero_representante_legal')
        domicilio = datos.get('domicilio_representante_legal')
        
        # Preparar datos del representante legal
        datos_representante = {
            'documento': doc_rep,
            'tipo_documento': tipo_doc,
            'nombre': nombre,
            'nacionalidad': nacionalidad,
            'genero': genero,
            'domicilio': domicilio,
            'es_grupo': None,
            'es_pyme': None,
            'es_entidad': None,
            'es_proveedor': None,
            'es_representante_legal': 'Sí',
            'es_ordenador_del_gasto': None,
            'es_supervisor': None
        }
        
        # Buscar si ya existe el representante
        registro_existente = db(db.entidades_personas.documento == doc_rep).select().first()
        
        if registro_existente:
            # Actualizar solo el campo booleano si está vacío
            campos_a_actualizar = {}
            
            if not registro_existente.es_representante_legal or registro_existente.es_representante_legal == 'No':
                campos_a_actualizar['es_representante_legal'] = 'Sí'
            
            if campos_a_actualizar:
                registro_existente.update_record(**campos_a_actualizar)
                resultado['operacion'] = 'actualizado'
            else:
                resultado['operacion'] = 'sin_cambios'
        else:
            # Insertar nuevo registro
            db.entidades_personas.insert(**datos_representante)
            resultado['operacion'] = 'insertado'
        
        resultado['documento'] = doc_rep
        
    except Exception as e:
        resultado['error'] = str(e)
    
    return resultado


def guardar_ordenador_gasto(datos):
    """
    Guarda o actualiza un ordenador del gasto en la tabla entidades_personas.
    
    Args:
        datos (dict): Diccionario con los datos del JSON
        
    Returns:
        dict: Información sobre la operación realizada
    """
    resultado = {
        'operacion': None,
        'documento': None,
        'error': None
    }
    
    try:
        # Extraer y procesar datos del ordenador del gasto
        doc_ordenador = limpiar_documento(datos.get('n_mero_de_documento_ordenador_del_gasto', ''))
        
        if not doc_ordenador:
            resultado['error'] = 'Documento de ordenador del gasto no proporcionado'
            return resultado
        
        tipo_doc = datos.get('tipo_de_documento_ordenador_del_gasto', 'Cédula de Ciudadanía')
        nombre = datos.get('nombre_ordenador_del_gasto')
        
        # Preparar datos del ordenador del gasto
        datos_ordenador = {
            'documento': doc_ordenador,
            'tipo_documento': tipo_doc,
            'nombre': nombre,
            'es_grupo': None,
            'es_pyme': None,
            'es_entidad': None,
            'es_proveedor': None,
            'es_representante_legal': None,
            'es_ordenador_del_gasto': 'Sí',
            'es_supervisor': None
        }
        
        # Buscar si ya existe el ordenador
        registro_existente = db(db.entidades_personas.documento == doc_ordenador).select().first()
        
        if registro_existente:
            # Actualizar solo el campo booleano si está vacío
            campos_a_actualizar = {}
            
            if not registro_existente.es_ordenador_del_gasto or registro_existente.es_ordenador_del_gasto == 'No':
                campos_a_actualizar['es_ordenador_del_gasto'] = 'Sí'
            
            if campos_a_actualizar:
                registro_existente.update_record(**campos_a_actualizar)
                resultado['operacion'] = 'actualizado'
            else:
                resultado['operacion'] = 'sin_cambios'
        else:
            # Insertar nuevo registro
            db.entidades_personas.insert(**datos_ordenador)
            resultado['operacion'] = 'insertado'
        
        resultado['documento'] = doc_ordenador
        
    except Exception as e:
        resultado['error'] = str(e)
    
    return resultado


def guardar_supervisor(datos):
    """
    Guarda o actualiza un supervisor en la tabla entidades_personas.
    
    Args:
        datos (dict): Diccionario con los datos del JSON
        
    Returns:
        dict: Información sobre la operación realizada
    """
    resultado = {
        'operacion': None,
        'documento': None,
        'error': None
    }
    
    try:
        # Extraer y procesar datos del supervisor
        doc_supervisor = limpiar_documento(datos.get('n_mero_de_documento_supervisor', ''))
        
        if not doc_supervisor:
            resultado['error'] = 'Documento de supervisor no proporcionado'
            return resultado
        
        tipo_doc = datos.get('tipo_de_documento_supervisor', 'Cédula de Ciudadanía')
        nombre = datos.get('nombre_supervisor')
        
        # Preparar datos del supervisor
        datos_supervisor = {
            'documento': doc_supervisor,
            'tipo_documento': tipo_doc,
            'nombre': nombre,
            'es_grupo': None,
            'es_pyme': None,
            'es_entidad': None,
            'es_proveedor': None,
            'es_representante_legal': None,
            'es_ordenador_del_gasto': None,
            'es_supervisor': 'Sí'
        }
        
        # Buscar si ya existe el supervisor
        registro_existente = db(db.entidades_personas.documento == doc_supervisor).select().first()
        
        if registro_existente:
            # Actualizar solo el campo booleano si está vacío
            campos_a_actualizar = {}
            
            if not registro_existente.es_supervisor or registro_existente.es_supervisor == 'No':
                campos_a_actualizar['es_supervisor'] = 'Sí'
            
            if campos_a_actualizar:
                registro_existente.update_record(**campos_a_actualizar)
                resultado['operacion'] = 'actualizado'
            else:
                resultado['operacion'] = 'sin_cambios'
        else:
            # Insertar nuevo registro
            db.entidades_personas.insert(**datos_supervisor)
            resultado['operacion'] = 'insertado'
        
        resultado['documento'] = doc_supervisor
        
    except Exception as e:
        resultado['error'] = str(e)
    
    return resultado


def guardar_ordenador_pago(datos):
    """
    Guarda o actualiza un ordenador de pago en la tabla entidades_personas.
    Nota: Se almacena como supervisor ya que no hay campo específico.
    
    Args:
        datos (dict): Diccionario con los datos del JSON
        
    Returns:
        dict: Información sobre la operación realizada
    """
    resultado = {
        'operacion': None,
        'documento': None,
        'error': None
    }
    
    try:
        # Extraer y procesar datos del ordenador de pago
        doc_ordenador_pago = limpiar_documento(datos.get('n_mero_de_documento_ordenador_de_pago', ''))
        
        if not doc_ordenador_pago:
            resultado['error'] = 'Documento de ordenador de pago no proporcionado'
            return resultado
        
        tipo_doc = datos.get('tipo_de_documento_ordenador_de_pago', 'Cédula de Ciudadanía')
        nombre = datos.get('nombre_ordenador_de_pago')
        
        # Preparar datos del ordenador de pago (como supervisor)
        datos_ordenador_pago = {
            'documento': doc_ordenador_pago,
            'tipo_documento': tipo_doc,
            'nombre': nombre,
            'es_grupo': None,
            'es_pyme': None,
            'es_entidad': None,
            'es_proveedor': None,
            'es_representante_legal': None,
            'es_ordenador_del_gasto': None,
            'es_supervisor': 'Sí'
        }
        
        # Buscar si ya existe
        registro_existente = db(db.entidades_personas.documento == doc_ordenador_pago).select().first()
        
        if registro_existente:
            # Actualizar solo el campo booleano si está vacío
            campos_a_actualizar = {}
            
            if not registro_existente.es_supervisor or registro_existente.es_supervisor == 'No':
                campos_a_actualizar['es_supervisor'] = 'Sí'
            
            if campos_a_actualizar:
                registro_existente.update_record(**campos_a_actualizar)
                resultado['operacion'] = 'actualizado'
            else:
                resultado['operacion'] = 'sin_cambios'
        else:
            # Insertar nuevo registro
            db.entidades_personas.insert(**datos_ordenador_pago)
            resultado['operacion'] = 'insertado'
        
        resultado['documento'] = doc_ordenador_pago
        
    except Exception as e:
        resultado['error'] = str(e)
    
    return resultado


def procesar_entidades_personas_desde_contrato(datos):
    """
    Procesa todos los roles (entidad, proveedor, representante, etc.) 
    desde un registro de contrato y los guarda en la base de datos.
    
    Args:
        datos (dict): Diccionario con los datos del JSON del contrato
        
    Returns:
        dict: Estadísticas de la operación
    """
    stats = {
        'entidad': None,
        'proveedor': None,
        'representante_legal': None,
        'ordenador_gasto': None,
        'supervisor': None,
        'ordenador_pago': None,
        'errores': []
    }
    
    try:
        # Guardar entidad
        stats['entidad'] = guardar_entidad_persona(datos)
        
        # Guardar proveedor
        stats['proveedor'] = guardar_proveedor(datos)
        
        # Guardar representante legal
        stats['representante_legal'] = guardar_representante_legal(datos)
        
        # Guardar ordenador del gasto
        stats['ordenador_gasto'] = guardar_ordenador_gasto(datos)
        
        # Guardar supervisor
        stats['supervisor'] = guardar_supervisor(datos)
        
        # Guardar ordenador de pago
        stats['ordenador_pago'] = guardar_ordenador_pago(datos)
        
        # Confirmar transacción
        db.commit()
        
    except Exception as e:
        db.rollback()
        stats['errores'].append(str(e))
    
    return stats





pwd=os.path.dirname("private/experiment_config.json")
def extractConfig(nameModel="SystemData",relPath=os.path.join(pwd,"experiment_config.json"),dataOut="keyantrophics"):
    configPath=os.path.join(os.getcwd(),relPath)
    with open(configPath, 'r', encoding='utf-8') as file:
        config = json.load(file)[nameModel]
    Output= config[dataOut]
    return Output
claveApiSocrata=extractConfig(nameModel="SocratesApi",dataOut="claveAppApi")
url=extractConfig(nameModel="SystemData",dataOut="urlApi",relPath=os.path.join(pwd,"experiment_config.json"))



client = Socrata("www.datos.gov.co", claveApiSocrata)

SancionesSecopI="4n4q-k399"
AntededentesSiri="iaeu-rcn6"
ContratosSecopII="jbjy-vk9h"#https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-II-Contratos-Electr-nicos/jbjy-vk9h/about_data

AdicionesSecopII="cb9c-h8sn"#https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-II-Adiciones/cb9c-h8sn/about_data
EjecucionesSecopII="mfmm-jqmq"#https://www.datos.gov.co/Estad-sticas-Nacionales/SECOP-II-Ejecuci-n-Contratos/mfmm-jqmq/data_preview

equivalenciadb={
    "CONTRATOS":ContratosSecopII,
    "ADICIONES":AdicionesSecopII,
    "EJECUCIONES":EjecucionesSecopII,
    "AMONESTADOS SECOPI":SancionesSecopI,
    "SANCIONADOS SIRI":AntededentesSiri
}

def load_data_socrata(source="CONTRATOS",max_sample=None,n2save=5000):
    id_dataset=equivalenciadb[source]
    t=0
    total=int(client.get_metadata(id_dataset, content_type="json")['columns'][0]['cachedContents']['non_null'])
    print(f"Son {total} {source}")
    reloj=time.time()
    for item in client.get_all(id_dataset):
        t+=1
        #if db(db["contratos"].id_contrato == item["id_contrato"]).select():
            #continue
        #print(item)
        if source =="CONTRATOS":
            guardar_contratos([item])
            procesar_entidades_personas_desde_contrato(item)
        elif source == "ADICIONES":
            guardar_adiciones([item], actualizar_existentes=True)
        elif source == "EJECUCIONES":
            guardar_ejecuciones([item])
        elif source=="AMONESTADOS SECOPI":
            guardar_amonestado_secop(item)
        elif source=="SANCIONADOS SIRI":
            guardar_sancionado_siri(item)
        else:
            raise ValueError("No valided option")
        
        
        if t % n2save==0:
            db.commit()
            procesados=t
            tiempo_transcurrido=time.time()-reloj
            resultado = estimar_tiempo_restante(total, t, tiempo_transcurrido)
            print(f"\nTotal a procesar: {total}")
            print(f"Procesados: {procesados}")
            print(f"Tiempo transcurrido: {tiempo_transcurrido} segundos")
            print(f"--- ESTIMACIÓN ---")
            print(f"Velocidad: {resultado['velocidad']:.2f} elementos/segundo")
            print(f"Porcentaje completado: {resultado['porcentaje_completado']:.2f}%")
            print(f"Tiempo restante: {formatear_tiempo(resultado['segundos_restantes'])}")
            print(f"Segundos restantes: {resultado['segundos_restantes']:.2f}")
            print(f"Hora estimada de finalización: {resultado['fecha_hora_fin'].strftime('%Y-%m-%d %H:%M:%S')}")
        if max_sample:
            if t % max_sample == 0:
                break
    db.commit()











load_data_socrata(source="CONTRATOS")
load_data_socrata(source="AMONESTADOS SECOPI")
load_data_socrata(source="SANCIONADOS SIRI")
load_data_socrata(source="ADICIONES")
load_data_socrata(source="EJECUCIONES")


