from pydal import DAL, Field

import os
import json
import pandas as pd
import time

from fastapi import FastAPI, Query, File, UploadFile,Form,HTTPException
from pydantic import BaseModel, field_validator, computed_field, Field as PydanticField
from typing import Dict, Annotated, Literal, Union,Optional, List
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi import Request
import time
import os
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
import json
from datetime import date, datetime
from fastapi import FastAPI, HTTPException
from sodapy import Socrata
from models.db import db
pwd = os.getcwd()
app = FastAPI()
def extractConfig(nameModel="SystemData",relPath=os.path.join(pwd,"private/experiment_config.json"),dataOut="keyantrophics"):
    configPath=os.path.join(os.getcwd(),relPath)
    with open(configPath, 'r', encoding='utf-8') as file:
        config = json.load(file)[nameModel]
    Output= config[dataOut]
    return Output
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Configurar templates (para archivos HTML externos)
templates = Jinja2Templates(directory="templates")

# Configurar archivos estáticos (CSS, JS, imágenes)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Estadisitcas Basicas 

from models.db import db
from datetime import datetime

def contar_contratos(
    nit_entidad=None,
    estado_contrato=None,
    tipo_contrato=None,
    modalidad_contratacion=None,
    codigo_categoria_principal=None,
    fecha_firma_desde=None,
    fecha_firma_hasta=None,
    fecha_inicio_desde=None,
    fecha_inicio_hasta=None,
    fecha_fin_desde=None,
    fecha_fin_hasta=None,
    justificacion_modalidad=None,
    reversion=None,
    valor_contrato_min=None,
    valor_contrato_max=None,
    dias_adicionados_min=None,
    dias_adicionados_max=None,
    puede_prorrogarse=None,
    duracion_contrato=None
):
    """
    Cuenta la cantidad de contratos aplicando filtros opcionales.
    
    Args:
        nit_entidad (str): NIT de la entidad
        estado_contrato (str): Estado del contrato
        tipo_contrato (str): Tipo de contrato
        modalidad_contratacion (str): Modalidad de contratación
        codigo_categoria_principal (str): Código de categoría principal
        fecha_firma_desde (datetime/str): Fecha de firma desde
        fecha_firma_hasta (datetime/str): Fecha de firma hasta
        fecha_inicio_desde (datetime/str): Fecha de inicio desde
        fecha_inicio_hasta (datetime/str): Fecha de inicio hasta
        fecha_fin_desde (datetime/str): Fecha de fin desde
        fecha_fin_hasta (datetime/str): Fecha de fin hasta
        justificacion_modalidad (str): Justificación de modalidad
        reversion (str): Reversión
        valor_contrato_min (float): Valor mínimo del contrato
        valor_contrato_max (float): Valor máximo del contrato
        dias_adicionados_min (int): Días adicionados mínimos
        dias_adicionados_max (int): Días adicionados máximos
        puede_prorrogarse (str): Si puede prorrogarse
        duracion_contrato (str): Duración del contrato
    
    Returns:
        int: Cantidad de contratos que cumplen con los filtros
    """
    # Construir la consulta base
    query = db.contratos.id > 0  # Consulta inicial que siempre es verdadera
    
    # Aplicar filtros si se proporcionan
    if nit_entidad is not None:
        query &= (db.contratos.nit_entidad == nit_entidad)
    
    if estado_contrato is not None:
        query &= (db.contratos.estado_contrato == estado_contrato)
    
    if tipo_contrato is not None:
        query &= (db.contratos.tipo_contrato == tipo_contrato)
    
    if modalidad_contratacion is not None:
        query &= (db.contratos.modalidad_contratacion == modalidad_contratacion)
    
    if codigo_categoria_principal is not None:
        query &= (db.contratos.codigo_categoria_principal == codigo_categoria_principal)
    
    if justificacion_modalidad is not None:
        query &= (db.contratos.justificacion_modalidad == justificacion_modalidad)
    
    if reversion is not None:
        query &= (db.contratos.reversion == reversion)
    
    if puede_prorrogarse is not None:
        query &= (db.contratos.puede_prorrogarse == puede_prorrogarse)
    
    if duracion_contrato is not None:
        query &= (db.contratos.duracion_contrato == duracion_contrato)
    
    # Filtros de rango para fechas de firma
    if fecha_firma_desde is not None:
        if isinstance(fecha_firma_desde, str):
            fecha_firma_desde = datetime.strptime(fecha_firma_desde, '%Y-%m-%d')
        query &= (db.contratos.fecha_firma >= fecha_firma_desde)
    
    if fecha_firma_hasta is not None:
        if isinstance(fecha_firma_hasta, str):
            fecha_firma_hasta = datetime.strptime(fecha_firma_hasta, '%Y-%m-%d')
        query &= (db.contratos.fecha_firma <= fecha_firma_hasta)
    
    # Filtros de rango para fechas de inicio
    if fecha_inicio_desde is not None:
        if isinstance(fecha_inicio_desde, str):
            fecha_inicio_desde = datetime.strptime(fecha_inicio_desde, '%Y-%m-%d')
        query &= (db.contratos.fecha_inicio >= fecha_inicio_desde)
    
    if fecha_inicio_hasta is not None:
        if isinstance(fecha_inicio_hasta, str):
            fecha_inicio_hasta = datetime.strptime(fecha_inicio_hasta, '%Y-%m-%d')
        query &= (db.contratos.fecha_inicio <= fecha_inicio_hasta)
    
    # Filtros de rango para fechas de fin
    if fecha_fin_desde is not None:
        if isinstance(fecha_fin_desde, str):
            fecha_fin_desde = datetime.strptime(fecha_fin_desde, '%Y-%m-%d')
        query &= (db.contratos.fecha_fin >= fecha_fin_desde)
    
    if fecha_fin_hasta is not None:
        if isinstance(fecha_fin_hasta, str):
            fecha_fin_hasta = datetime.strptime(fecha_fin_hasta, '%Y-%m-%d')
        query &= (db.contratos.fecha_fin <= fecha_fin_hasta)
    
    # Filtros de rango para valor de contrato
    if valor_contrato_min is not None:
        query &= (db.contratos.valor_contrato >= valor_contrato_min)
    
    if valor_contrato_max is not None:
        query &= (db.contratos.valor_contrato <= valor_contrato_max)
    
    # Filtros de rango para días adicionados
    if dias_adicionados_min is not None:
        query &= (db.contratos.dias_adicionados >= dias_adicionados_min)
    
    if dias_adicionados_max is not None:
        query &= (db.contratos.dias_adicionados <= dias_adicionados_max)
    
    # Contar registros
    count = db(query).count()
    
    return count


def contar_contratos_avanzado(**filtros):
    """
    Versión alternativa que acepta filtros como diccionario usando **kwargs.
    Más flexible para construir filtros dinámicamente.
    
    Args:
        **filtros: Filtros como argumentos con nombre
    
    Returns:
        dict: Diccionario con el conteo y los filtros aplicados
    """
    count = contar_contratos(**filtros)
    
    return {
        'total': count,
        'filtros_aplicados': {k: v for k, v in filtros.items() if v is not None}
    }


def obtener_estadisticas_contratos(**filtros):
    """
    Obtiene estadísticas adicionales de los contratos filtrados.
    
    Args:
        **filtros: Filtros como argumentos con nombre
    
    Returns:
        dict: Diccionario con estadísticas
    """
    # Construir query con los mismos filtros
    query = db.contratos.id > 0
    
    if 'nit_entidad' in filtros and filtros['nit_entidad'] is not None:
        query &= (db.contratos.nit_entidad == filtros['nit_entidad'])
    
    if 'estado_contrato' in filtros and filtros['estado_contrato'] is not None:
        query &= (db.contratos.estado_contrato == filtros['estado_contrato'])
    
    if 'tipo_contrato' in filtros and filtros['tipo_contrato'] is not None:
        query &= (db.contratos.tipo_contrato == filtros['tipo_contrato'])
    
    if 'modalidad_contratacion' in filtros and filtros['modalidad_contratacion'] is not None:
        query &= (db.contratos.modalidad_contratacion == filtros['modalidad_contratacion'])
    
    # Agregar más filtros según sea necesario...
    
    # Obtener estadísticas
    contratos = db(query).select(
        db.contratos.valor_contrato,
        db.contratos.dias_adicionados
    )
    
    if not contratos:
        return {
            'total': 0,
            'valor_total': 0,
            'valor_promedio': 0,
            'valor_minimo': 0,
            'valor_maximo': 0,
            'dias_adicionados_promedio': 0
        }
    
    valores = [c.valor_contrato for c in contratos if c.valor_contrato is not None]
    dias = [c.dias_adicionados for c in contratos if c.dias_adicionados is not None]
    
    return {
        'total': len(contratos),
        'valor_total': sum(valores) if valores else 0,
        'valor_promedio': sum(valores) / len(valores) if valores else 0,
        'valor_minimo': min(valores) if valores else 0,
        'valor_maximo': max(valores) if valores else 0,
        'dias_adicionados_promedio': sum(dias) / len(dias) if dias else 0
    }


def contar_contratos_por_entidad(
    departamento=None,
    ciudad=None,
    orden=None,
    sector=None,
    rama=None,
    entidad_centralizada=None
):
    """
    Cuenta la cantidad de contratos filtrando por características de la entidad.
    Realiza un JOIN con la tabla entidades_personas.
    
    Args:
        departamento (str): Departamento de la entidad
        ciudad (str): Ciudad de la entidad
        orden (str): Orden de la entidad (Nacional, Territorial, etc.)
        sector (str): Sector de la entidad
        rama (str): Rama de la entidad (Ejecutivo, Legislativo, etc.)
        entidad_centralizada (str): Si es centralizada o descentralizada
    
    Returns:
        int: Cantidad de contratos que cumplen con los filtros
    """
    # Construir la consulta base con JOIN
    # JOIN: contratos.nit_entidad = entidades_personas.documento
    query = (db.contratos.nit_entidad == db.entidades_personas.documento)
    
    # Asegurarse de que sea una entidad (no proveedor ni otros roles)
    query &= (db.entidades_personas.es_entidad == 'Sí')
    
    # Aplicar filtros si se proporcionan
    if departamento is not None:
        query &= (db.entidades_personas.departamento == departamento)
    
    if ciudad is not None:
        query &= (db.entidades_personas.ciudad == ciudad)
    
    if orden is not None:
        query &= (db.entidades_personas.orden == orden)
    
    if sector is not None:
        query &= (db.entidades_personas.sector == sector)
    
    if rama is not None:
        query &= (db.entidades_personas.rama == rama)
    
    if entidad_centralizada is not None:
        query &= (db.entidades_personas.entidad_centralizada == entidad_centralizada)
    
    # Contar registros con JOIN
    count = db(query).count()
    
    return count


def contar_contratos_por_entidad_avanzado(**filtros):
    """
    Versión alternativa que acepta filtros como diccionario usando **kwargs.
    
    Args:
        **filtros: Filtros como argumentos con nombre
    
    Returns:
        dict: Diccionario con el conteo y los filtros aplicados
    """
    count = contar_contratos_por_entidad(**filtros)
    
    return {
        'total': count,
        'filtros_aplicados': {k: v for k, v in filtros.items() if v is not None}
    }


def obtener_contratos_por_entidad(
    departamento=None,
    ciudad=None,
    orden=None,
    sector=None,
    rama=None,
    entidad_centralizada=None,
    limit=None,
    orderby=None
):
    """
    Obtiene los contratos completos filtrando por características de la entidad.
    
    Args:
        departamento (str): Departamento de la entidad
        ciudad (str): Ciudad de la entidad
        orden (str): Orden de la entidad
        sector (str): Sector de la entidad
        rama (str): Rama de la entidad
        entidad_centralizada (str): Si es centralizada o descentralizada
        limit (int): Límite de registros a retornar
        orderby (field): Campo por el cual ordenar
    
    Returns:
        list: Lista de contratos que cumplen con los filtros
    """
    # Construir la consulta base con JOIN
    query = (db.contratos.nit_entidad == db.entidades_personas.documento)
    query &= (db.entidades_personas.es_entidad == 'Sí')
    
    # Aplicar filtros
    if departamento is not None:
        query &= (db.entidades_personas.departamento == departamento)
    
    if ciudad is not None:
        query &= (db.entidades_personas.ciudad == ciudad)
    
    if orden is not None:
        query &= (db.entidades_personas.orden == orden)
    
    if sector is not None:
        query &= (db.entidades_personas.sector == sector)
    
    if rama is not None:
        query &= (db.entidades_personas.rama == rama)
    
    if entidad_centralizada is not None:
        query &= (db.entidades_personas.entidad_centralizada == entidad_centralizada)
    
    # Realizar consulta
    contratos = db(query).select(
        db.contratos.ALL,
        db.entidades_personas.nombre,
        db.entidades_personas.departamento,
        db.entidades_personas.ciudad,
        db.entidades_personas.orden,
        db.entidades_personas.sector,
        db.entidades_personas.rama,
        db.entidades_personas.entidad_centralizada,
        limitby=(0, limit) if limit else None,
        orderby=orderby
    )
    
    return contratos


def obtener_estadisticas_por_entidad(
    departamento=None,
    ciudad=None,
    orden=None,
    sector=None,
    rama=None,
    entidad_centralizada=None
):
    """
    Obtiene estadísticas de contratos filtrando por características de la entidad.
    
    Args:
        departamento (str): Departamento de la entidad
        ciudad (str): Ciudad de la entidad
        orden (str): Orden de la entidad
        sector (str): Sector de la entidad
        rama (str): Rama de la entidad
        entidad_centralizada (str): Si es centralizada o descentralizada
    
    Returns:
        dict: Diccionario con estadísticas
    """
    # Construir query
    query = (db.contratos.nit_entidad == db.entidades_personas.documento)
    query &= (db.entidades_personas.es_entidad == 'Sí')
    
    if departamento is not None:
        query &= (db.entidades_personas.departamento == departamento)
    
    if ciudad is not None:
        query &= (db.entidades_personas.ciudad == ciudad)
    
    if orden is not None:
        query &= (db.entidades_personas.orden == orden)
    
    if sector is not None:
        query &= (db.entidades_personas.sector == sector)
    
    if rama is not None:
        query &= (db.entidades_personas.rama == rama)
    
    if entidad_centralizada is not None:
        query &= (db.entidades_personas.entidad_centralizada == entidad_centralizada)
    
    # Obtener contratos
    contratos = db(query).select(
        db.contratos.valor_contrato,
        db.contratos.dias_adicionados,
        db.contratos.estado_contrato
    )
    
    if not contratos:
        return {
            'total_contratos': 0,
            'entidades_unicas': 0,
            'valor_total': 0,
            'valor_promedio': 0,
            'valor_minimo': 0,
            'valor_maximo': 0,
            'dias_adicionados_promedio': 0,
            'contratos_por_estado': {}
        }
    
    # Calcular estadísticas
    valores = [c.valor_contrato for c in contratos if c.valor_contrato is not None]
    dias = [c.dias_adicionados for c in contratos if c.dias_adicionados is not None]
    
    # Contar contratos por estado
    estados = {}
    for c in contratos:
        estado = c.estado_contrato or 'Sin estado'
        estados[estado] = estados.get(estado, 0) + 1
    
    # Contar entidades únicas
    entidades_query = query
    entidades_unicas = db(entidades_query).select(
        db.contratos.nit_entidad,
        distinct=True
    )
    
    return {
        'total_contratos': len(contratos),
        'entidades_unicas': len(entidades_unicas),
        'valor_total': sum(valores) if valores else 0,
        'valor_promedio': sum(valores) / len(valores) if valores else 0,
        'valor_minimo': min(valores) if valores else 0,
        'valor_maximo': max(valores) if valores else 0,
        'dias_adicionados_promedio': sum(dias) / len(dias) if dias else 0,
        'contratos_por_estado': estados
    }


def contar_contratos_combinado(
    # Filtros de entidad
    departamento=None,
    ciudad=None,
    orden=None,
    sector=None,
    rama=None,
    entidad_centralizada=None,
    # Filtros de contrato
    estado_contrato=None,
    tipo_contrato=None,
    modalidad_contratacion=None,
    valor_contrato_min=None,
    valor_contrato_max=None
):
    """
    Cuenta contratos combinando filtros de entidad y de contrato.
    
    Args:
        Filtros de entidad: departamento, ciudad, orden, sector, rama, entidad_centralizada
        Filtros de contrato: estado_contrato, tipo_contrato, modalidad_contratacion, 
                            valor_contrato_min, valor_contrato_max
    
    Returns:
        int: Cantidad de contratos que cumplen todos los filtros
    """
    # Construir query base
    query = (db.contratos.nit_entidad == db.entidades_personas.documento)
    query &= (db.entidades_personas.es_entidad == 'Sí')
    
    # Filtros de entidad
    if departamento is not None:
        query &= (db.entidades_personas.departamento == departamento)
    
    if ciudad is not None:
        query &= (db.entidades_personas.ciudad == ciudad)
    
    if orden is not None:
        query &= (db.entidades_personas.orden == orden)
    
    if sector is not None:
        query &= (db.entidades_personas.sector == sector)
    
    if rama is not None:
        query &= (db.entidades_personas.rama == rama)
    
    if entidad_centralizada is not None:
        query &= (db.entidades_personas.entidad_centralizada == entidad_centralizada)
    
    # Filtros de contrato
    if estado_contrato is not None:
        query &= (db.contratos.estado_contrato == estado_contrato)
    
    if tipo_contrato is not None:
        query &= (db.contratos.tipo_contrato == tipo_contrato)
    
    if modalidad_contratacion is not None:
        query &= (db.contratos.modalidad_contratacion == modalidad_contratacion)
    
    if valor_contrato_min is not None:
        query &= (db.contratos.valor_contrato >= valor_contrato_min)
    
    if valor_contrato_max is not None:
        query &= (db.contratos.valor_contrato <= valor_contrato_max)
    
    # Contar
    count = db(query).count()
    
    return count


def contar_contratos_por_proveedor(
    es_grupo=None,
    es_pyme=None,
    es_entidad=None,
    es_proveedor=None,
    es_representante_legal=None,
    es_ordenador_del_gasto=None,
    es_supervisor=None
):
    """
    Cuenta la cantidad de contratos filtrando por características del proveedor.
    Realiza un JOIN con la tabla entidades_personas usando documento_proveedor.
    
    Args:
        es_grupo (str): Si es grupo ('Sí' o 'No')
        es_pyme (str): Si es PYME ('Sí' o 'No')
        es_entidad (str): Si es entidad ('Sí' o 'No')
        es_proveedor (str): Si es proveedor ('Sí' o 'No')
        es_representante_legal (str): Si es representante legal ('Sí' o 'No')
        es_ordenador_del_gasto (str): Si es ordenador del gasto ('Sí' o 'No')
        es_supervisor (str): Si es supervisor ('Sí' o 'No')
    
    Returns:
        int: Cantidad de contratos que cumplen con los filtros
    """
    # Construir la consulta base con JOIN
    # JOIN: contratos.documento_proveedor = entidades_personas.documento
    query = (db.contratos.documento_proveedor == db.entidades_personas.documento)
    
    # Aplicar filtros si se proporcionan
    if es_grupo is not None:
        query &= (db.entidades_personas.es_grupo == es_grupo)
    
    if es_pyme is not None:
        query &= (db.entidades_personas.es_pyme == es_pyme)
    
    if es_entidad is not None:
        query &= (db.entidades_personas.es_entidad == es_entidad)
    
    if es_proveedor is not None:
        query &= (db.entidades_personas.es_proveedor == es_proveedor)
    
    if es_representante_legal is not None:
        query &= (db.entidades_personas.es_representante_legal == es_representante_legal)
    
    if es_ordenador_del_gasto is not None:
        query &= (db.entidades_personas.es_ordenador_del_gasto == es_ordenador_del_gasto)
    
    if es_supervisor is not None:
        query &= (db.entidades_personas.es_supervisor == es_supervisor)
    
    # Contar registros con JOIN
    count = db(query).count()
    
    return count


def contar_contratos_por_proveedor_avanzado(**filtros):
    """
    Versión alternativa que acepta filtros como diccionario usando **kwargs.
    
    Args:
        **filtros: Filtros como argumentos con nombre
    
    Returns:
        dict: Diccionario con el conteo y los filtros aplicados
    """
    count = contar_contratos_por_proveedor(**filtros)
    
    return {
        'total': count,
        'filtros_aplicados': {k: v for k, v in filtros.items() if v is not None}
    }


def obtener_contratos_por_proveedor(
    es_grupo=None,
    es_pyme=None,
    es_entidad=None,
    es_proveedor=None,
    es_representante_legal=None,
    es_ordenador_del_gasto=None,
    es_supervisor=None,
    limit=None,
    orderby=None
):
    """
    Obtiene los contratos completos filtrando por características del proveedor.
    
    Args:
        es_grupo (str): Si es grupo
        es_pyme (str): Si es PYME
        es_entidad (str): Si es entidad
        es_proveedor (str): Si es proveedor
        es_representante_legal (str): Si es representante legal
        es_ordenador_del_gasto (str): Si es ordenador del gasto
        es_supervisor (str): Si es supervisor
        limit (int): Límite de registros a retornar
        orderby (field): Campo por el cual ordenar
    
    Returns:
        list: Lista de contratos que cumplen con los filtros
    """
    # Construir la consulta base con JOIN
    query = (db.contratos.documento_proveedor == db.entidades_personas.documento)
    
    # Aplicar filtros
    if es_grupo is not None:
        query &= (db.entidades_personas.es_grupo == es_grupo)
    
    if es_pyme is not None:
        query &= (db.entidades_personas.es_pyme == es_pyme)
    
    if es_entidad is not None:
        query &= (db.entidades_personas.es_entidad == es_entidad)
    
    if es_proveedor is not None:
        query &= (db.entidades_personas.es_proveedor == es_proveedor)
    
    if es_representante_legal is not None:
        query &= (db.entidades_personas.es_representante_legal == es_representante_legal)
    
    if es_ordenador_del_gasto is not None:
        query &= (db.entidades_personas.es_ordenador_del_gasto == es_ordenador_del_gasto)
    
    if es_supervisor is not None:
        query &= (db.entidades_personas.es_supervisor == es_supervisor)
    
    # Realizar consulta
    contratos = db(query).select(
        db.contratos.ALL,
        db.entidades_personas.nombre,
        db.entidades_personas.documento,
        db.entidades_personas.tipo_documento,
        db.entidades_personas.es_grupo,
        db.entidades_personas.es_pyme,
        db.entidades_personas.es_entidad,
        db.entidades_personas.es_proveedor,
        db.entidades_personas.es_representante_legal,
        db.entidades_personas.es_ordenador_del_gasto,
        db.entidades_personas.es_supervisor,
        limitby=(0, limit) if limit else None,
        orderby=orderby
    )
    
    return contratos


def obtener_estadisticas_por_proveedor(
    es_grupo=None,
    es_pyme=None,
    es_entidad=None,
    es_proveedor=None,
    es_representante_legal=None,
    es_ordenador_del_gasto=None,
    es_supervisor=None
):
    """
    Obtiene estadísticas de contratos filtrando por características del proveedor.
    
    Args:
        es_grupo (str): Si es grupo
        es_pyme (str): Si es PYME
        es_entidad (str): Si es entidad
        es_proveedor (str): Si es proveedor
        es_representante_legal (str): Si es representante legal
        es_ordenador_del_gasto (str): Si es ordenador del gasto
        es_supervisor (str): Si es supervisor
    
    Returns:
        dict: Diccionario con estadísticas
    """
    # Construir query
    query = (db.contratos.documento_proveedor == db.entidades_personas.documento)
    
    if es_grupo is not None:
        query &= (db.entidades_personas.es_grupo == es_grupo)
    
    if es_pyme is not None:
        query &= (db.entidades_personas.es_pyme == es_pyme)
    
    if es_entidad is not None:
        query &= (db.entidades_personas.es_entidad == es_entidad)
    
    if es_proveedor is not None:
        query &= (db.entidades_personas.es_proveedor == es_proveedor)
    
    if es_representante_legal is not None:
        query &= (db.entidades_personas.es_representante_legal == es_representante_legal)
    
    if es_ordenador_del_gasto is not None:
        query &= (db.entidades_personas.es_ordenador_del_gasto == es_ordenador_del_gasto)
    
    if es_supervisor is not None:
        query &= (db.entidades_personas.es_supervisor == es_supervisor)
    
    # Obtener contratos
    contratos = db(query).select(
        db.contratos.valor_contrato,
        db.contratos.dias_adicionados,
        db.contratos.estado_contrato,
        db.contratos.documento_proveedor
    )
    
    if not contratos:
        return {
            'total_contratos': 0,
            'proveedores_unicos': 0,
            'valor_total': 0,
            'valor_promedio': 0,
            'valor_minimo': 0,
            'valor_maximo': 0,
            'dias_adicionados_promedio': 0,
            'contratos_por_estado': {}
        }
    
    # Calcular estadísticas
    valores = [c.valor_contrato for c in contratos if c.valor_contrato is not None]
    dias = [c.dias_adicionados for c in contratos if c.dias_adicionados is not None]
    
    # Contar contratos por estado
    estados = {}
    for c in contratos:
        estado = c.estado_contrato or 'Sin estado'
        estados[estado] = estados.get(estado, 0) + 1
    
    # Contar proveedores únicos
    proveedores_unicos = set()
    for c in contratos:
        if c.documento_proveedor:
            proveedores_unicos.add(c.documento_proveedor)
    
    return {
        'total_contratos': len(contratos),
        'proveedores_unicos': len(proveedores_unicos),
        'valor_total': sum(valores) if valores else 0,
        'valor_promedio': sum(valores) / len(valores) if valores else 0,
        'valor_minimo': min(valores) if valores else 0,
        'valor_maximo': max(valores) if valores else 0,
        'dias_adicionados_promedio': sum(dias) / len(dias) if dias else 0,
        'contratos_por_estado': estados
    }


def contar_contratos_por_roles_multiples(
    es_grupo=None,
    es_pyme=None,
    es_entidad=None,
    es_proveedor=None,
    es_representante_legal=None,
    es_ordenador_del_gasto=None,
    es_supervisor=None,
    # Filtros adicionales de contrato
    estado_contrato=None,
    tipo_contrato=None,
    modalidad_contratacion=None,
    valor_contrato_min=None,
    valor_contrato_max=None
):
    """
    Cuenta contratos combinando filtros de proveedor y de contrato.
    
    Args:
        Filtros de proveedor: es_grupo, es_pyme, es_entidad, es_proveedor, 
                             es_representante_legal, es_ordenador_del_gasto, es_supervisor
        Filtros de contrato: estado_contrato, tipo_contrato, modalidad_contratacion,
                            valor_contrato_min, valor_contrato_max
    
    Returns:
        int: Cantidad de contratos que cumplen todos los filtros
    """
    # Construir query base
    query = (db.contratos.documento_proveedor == db.entidades_personas.documento)
    
    # Filtros de proveedor
    if es_grupo is not None:
        query &= (db.entidades_personas.es_grupo == es_grupo)
    
    if es_pyme is not None:
        query &= (db.entidades_personas.es_pyme == es_pyme)
    
    if es_entidad is not None:
        query &= (db.entidades_personas.es_entidad == es_entidad)
    
    if es_proveedor is not None:
        query &= (db.entidades_personas.es_proveedor == es_proveedor)
    
    if es_representante_legal is not None:
        query &= (db.entidades_personas.es_representante_legal == es_representante_legal)
    
    if es_ordenador_del_gasto is not None:
        query &= (db.entidades_personas.es_ordenador_del_gasto == es_ordenador_del_gasto)
    
    if es_supervisor is not None:
        query &= (db.entidades_personas.es_supervisor == es_supervisor)
    
    # Filtros de contrato
    if estado_contrato is not None:
        query &= (db.contratos.estado_contrato == estado_contrato)
    
    if tipo_contrato is not None:
        query &= (db.contratos.tipo_contrato == tipo_contrato)
    
    if modalidad_contratacion is not None:
        query &= (db.contratos.modalidad_contratacion == modalidad_contratacion)
    
    if valor_contrato_min is not None:
        query &= (db.contratos.valor_contrato >= valor_contrato_min)
    
    if valor_contrato_max is not None:
        query &= (db.contratos.valor_contrato <= valor_contrato_max)
    
    # Contar
    count = db(query).count()
    
    return count


def obtener_distribucion_por_roles():
    """
    Obtiene un resumen de la distribución de contratos por roles.
    
    Returns:
        dict: Diccionario con conteos por cada rol
    """
    return {
        'total_proveedores': contar_contratos_por_proveedor(es_proveedor='Sí'),
        'total_pymes': contar_contratos_por_proveedor(es_pyme='Sí'),
        'total_grupos': contar_contratos_por_proveedor(es_grupo='Sí'),
        'total_proveedores_individuales': contar_contratos_por_proveedor(
            es_proveedor='Sí',
            es_grupo='No',
            es_pyme='No'
        ),
        'proveedores_que_son_entidades': contar_contratos_por_proveedor(
            es_proveedor='Sí',
            es_entidad='Sí'
        ),
        'proveedores_que_son_representantes': contar_contratos_por_proveedor(
            es_proveedor='Sí',
            es_representante_legal='Sí'
        )
    }




#A usar
obtener_estadisticas_por_entidad()
total = contar_contratos()

def make_url_tail(nit_entidad=None,
    documento_proveedor=None,
    valor_minimo=None,
    valor_maximo=None,
    html_full:bool=False):
    if html_full:
        url_tail =["html_full=True"]
    else:
        url_tail =[]
    if nit_entidad:
        url_tail.append("nit_entidad="+ nit_entidad)
    if documento_proveedor:
        url_tail.append("documento_proveedor=" + documento_proveedor)
    
    if valor_minimo:
        url_tail.append("valor_minimo="+ str(valor_minimo))
    
    if valor_maximo:
        url_tail.append("valor_maximo=" + str(valor_maximo))
    str_url_tail=""
    if len(url_tail)>0:
        str_url_tail="?"+url_tail[0]
        if len(url_tail)>1:
            str_url_tail=str_url_tail+"&"+"&".join(url_tail[1:])
        
    return str_url_tail


@app.get('/', response_class=HTMLResponse)
async def root(request: Request,
    nit_entidad=None, 
    documento_proveedor=None, 
    valor_minimo=None, 
    valor_maximo=None,
    html_full:bool=False):
    
    str_url_tail=make_url_tail(nit_entidad=nit_entidad,
    documento_proveedor=documento_proveedor,
    valor_minimo=valor_minimo,
    valor_maximo=valor_maximo,
    html_full=html_full)
    
    context = {
        "request": request,"str_url_tail":str_url_tail}
    return templates.TemplateResponse("index.html", context)

@app.get('/html/index_info', response_class=HTMLResponse)
async def index_tot(request: Request,
    nit_entidad=None, 
    documento_proveedor=None, 
    valor_minimo=None, 
    valor_maximo=None,
    html_full:bool=False):

    str_url_tail=make_url_tail(nit_entidad=nit_entidad,
    documento_proveedor=documento_proveedor,
    valor_minimo=valor_minimo,
    valor_maximo=valor_maximo,
    html_full=html_full)
    context = {
        "request": request,"str_url_tail":str_url_tail}
    
    return templates.TemplateResponse("index_tot.html", context)

@app.get('/html/header', response_class=HTMLResponse)
async def header(request: Request):
    context = {
        "request": request}
    
    return templates.TemplateResponse("header.html", context)

@app.get('/html/section_cards', response_class=HTMLResponse)
async def section_cards(request: Request,
    nit_entidad=None, 
    documento_proveedor=None, 
    valor_minimo=None, 
    valor_maximo=None,
    html_full:bool=False):
    print(valor_minimo)
    str_url_tail=make_url_tail(nit_entidad=nit_entidad,
    documento_proveedor=documento_proveedor,
    valor_minimo=valor_minimo,
    valor_maximo=valor_maximo,
    html_full=html_full)

    context = {
        "request": request,"str_url_tail":str_url_tail}
    
    return templates.TemplateResponse("section_cards.html", context)
@app.get('/html/footer', response_class=HTMLResponse)
async def footer(request: Request):
    context = {
        "request": request}
    
    return templates.TemplateResponse("footer.html", context)

# def generar_nodos_y_enlaces(nit_entidad=None, documento_proveedor=None, departamento=None, 
#                             fecha_inicio=None, fecha_fin=None, valor_minimo=None, valor_maximo=None,
#                             tamano_min=5, tamano_max=50):
#     """
#     Genera dos listas:
#     1. nodos: Lista de diccionarios con entidades y proveedores
#     2. enlaces: Lista de diccionarios con las relaciones entre entidades y proveedores
    
#     Parámetros:
#     - nit_entidad: Filtrar por un NIT de entidad específico
#     - documento_proveedor: Filtrar por documento de proveedor específico
#     - departamento: Filtrar por departamento
#     - fecha_inicio: Filtrar contratos desde esta fecha
#     - fecha_fin: Filtrar contratos hasta esta fecha
#     - valor_minimo: Valor mínimo del contrato
#     - valor_maximo: Valor máximo del contrato
#     - tamano_min: Tamaño mínimo de los nodos (default: 5)
#     - tamano_max: Tamaño máximo de los nodos (default: 50)
#     """
    
#     nodos = []
#     enlaces = []
#     enlaces_dict = {}
    
#     # ============= CONSTRUIR QUERY BASE =============
#     query_base = (db.contratos.id > 0)
    
#     if nit_entidad:
#         query_base &= (db.contratos.nit_entidad == nit_entidad)
    
#     if documento_proveedor:
#         query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    
#     if departamento:
#         query_base &= (db.contratos.departamento == departamento)
    
#     if fecha_inicio:
#         query_base &= (db.contratos.fecha_inicio >= fecha_inicio)
    
#     if fecha_fin:
#         query_base &= (db.contratos.fecha_fin <= fecha_fin)
    
#     if valor_minimo:
#         query_base &= (db.contratos.valor_contrato >= valor_minimo)
    
#     if valor_maximo:
#         query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
#     # ============= PROCESAR ENTIDADES =============
#     #resultado = db.contratos.nit_entidad.count()
#     suma_valor = db.contratos.valor_contrato.sum()
#     resultado = db(query_base).count()
#     #suma_valor = db(query_base).select(suma_valor_total=db.contratos.valor_contrato.sum()).first().suma_valor_total
#     query_entidades = query_base & (db.contratos.nit_entidad != None)
    
#     conteos = db(query_entidades).select(
#         db.contratos.nit_entidad,
#         resultado,
#         suma_valor,
#         groupby=db.contratos.nit_entidad,
#         having=resultado > 0,
#         orderby=~resultado
#     )
    
#     # Lista temporal para almacenar nodos de entidades
#     nodos_entidades = []
    
#     for row in conteos:
#         nitActual = row.contratos.nit_entidad
#         valor_total = float(row[suma_valor] or 0)
        
#         # Buscar datos de la entidad
#         dataEntidad = db(db.entidades_personas.documento == nitActual).select().last()
        
#         try:
#             data_dict = dataEntidad.as_dict()
#             nodo_entidad = {
#                 "id": nitActual,
#                 "name": data_dict.get('nombre', data_dict.get('nombre_entidad', f"Entidad {nitActual}")),
#                 "departamento": data_dict.get("departamento",""),
#                 "tipo": "entidad",
#                 "url": "",
#                 "color": '#6da2c4',
#                 "size": 0,  # Se calculará después
#                 "cantidad_contratos": row[resultado],
#                 "valor_contrato": valor_total
#             }
#             nodos_entidades.append(nodo_entidad)
#         except:
#             print(nitActual)
#             nodo_entidad = {
#                 "id": nitActual,
#                 "name": "Sin informacion",
#                 "departamento": "Sin informacion",
#                 "tipo": "entidad",
#                 "url": "",
#                 "color": '#6da2c4',
#                 "size": 0,  # Se calculará después
#                 "cantidad_contratos": row[resultado],
#                 "valor_contrato": valor_total
#             }
#             nodos_entidades.append(nodo_entidad)
#             #nodos_entidades.append(nodo_entidad)
    
#     # ============= PROCESAR PROVEEDORES =============
#     resultadoProv = db.contratos.documento_proveedor.count()
#     suma_valor_prov = db.contratos.valor_contrato.sum()
    
#     query_proveedores = query_base & (db.contratos.documento_proveedor != None)
    
#     conteosProv = db(query_proveedores).select(
#         db.contratos.documento_proveedor,
#         resultadoProv,
#         suma_valor_prov,
#         groupby=db.contratos.documento_proveedor,
#         having=resultadoProv > 0,
#         orderby=~resultadoProv
#     )
    
#     # Lista temporal para almacenar nodos de proveedores
#     nodos_proveedores = []
    
#     for row in conteosProv:
#         docActual = row.contratos.documento_proveedor
#         valor_total = float(row[suma_valor_prov] or 0)
        
#         # Buscar datos del proveedor
#         dataProv = db(db.entidades_personas.documento == docActual).select().last()
        
#         try:
#             data_dict = dataProv.as_dict()
#             nodo_proveedor = {
#                 "id": docActual,
#                 "name": data_dict.get('nombre', data_dict.get('nombre_entidad', f"Entidad {nitActual}")),

#                 "es_pyme": data_dict.get('es_pyme', "No"),
#                 "es_grupo": data_dict.get('es_grupo', "No"),
#                 "tipo": "proveedor",
#                 "url": "",
#                 "color": "#1dc96a",
#                 "size": 0,  # Se calculará después
#                 "cantidad_contratos": row[resultadoProv],
#                 "valor_contrato": valor_total
#             }
#             nodos_proveedores.append(nodo_proveedor)
#         except:
#             print(docActual)
#             nodo_proveedor = {
#                 "id": docActual,
#                 "name": "Sin informacion",

#                 "es_pyme": "Sin informacion",
#                 "es_grupo": "Sin informacion",
#                 "tipo": "proveedor",
#                 "url": "",
#                 "color": "#1dc96a",
#                 "size": 0,  # Se calculará después
#                 "cantidad_contratos": row[resultadoProv],
#                 "valor_contrato": valor_total
#             }
#             nodos_proveedores.append(nodo_proveedor)
    
#     # ============= NORMALIZAR TAMAÑOS =============
#     # Combinar todos los nodos para calcular valores min y max globales
#     todos_nodos = nodos_entidades + nodos_proveedores
    
#     if len(todos_nodos) > 0:
#         # Obtener valores mínimo y máximo de contratos
#         valores = [nodo['valor_contrato'] for nodo in todos_nodos if nodo['valor_contrato'] > 0]
        
#         if len(valores) > 0:
#             valor_min_global = min(valores)
#             valor_max_global = max(valores)
            
#             # Evitar división por cero si todos los valores son iguales
#             rango_valores = valor_max_global - valor_min_global
            
#             if rango_valores > 0:
#                 # Normalización lineal: size = tamano_min + (valor - min) / (max - min) * (tamano_max - tamano_min)
#                 for nodo in todos_nodos:
#                     if nodo['valor_contrato'] > 0:
#                         valor_normalizado = (nodo['valor_contrato'] - valor_min_global) / rango_valores
#                         nodo['size'] = tamano_min + valor_normalizado * (tamano_max - tamano_min)
#                     else:
#                         nodo['size'] = tamano_min
#             else:
#                 # Si todos los valores son iguales, usar tamaño promedio
#                 tamano_promedio = (tamano_min + tamano_max) / 2
#                 for nodo in todos_nodos:
#                     nodo['size'] = tamano_promedio
#         else:
#             # Si no hay valores, usar tamaño mínimo
#             for nodo in todos_nodos:
#                 nodo['size'] = tamano_min
    
#     # Agregar nodos normalizados a la lista final
#     nodos = todos_nodos
    
#     # ============= GENERAR ENLACES (RELACIONES) =============
#     query_enlaces = query_base & (db.contratos.nit_entidad != None) & (db.contratos.documento_proveedor != None)
    
#     contratos = db(query_enlaces).select(
#         db.contratos.nit_entidad,
#         db.contratos.documento_proveedor,
#         db.contratos.valor_contrato
#     )
    
#     # Agrupar enlaces y sumar valores
#     for contrato in contratos:
#         source = contrato.nit_entidad
#         target = contrato.documento_proveedor
#         identificador = f"{source}_{target}"
        
#         if identificador not in enlaces_dict:
#             enlaces_dict[identificador] = {
#                 "source": source,
#                 "target": target,
#                 "identificador": identificador,
#                 "color": "#ff0000",
#                 "cantidad_contratos": 0,
#                 "valor_contrato": 0
#             }
        
#         enlaces_dict[identificador]["cantidad_contratos"] += 1
#         enlaces_dict[identificador]["valor_contrato"] += float(contrato.valor_contrato or 0)
    
#     # Convertir diccionario de enlaces a lista
#     enlaces = list(enlaces_dict.values())
#     #print(nodos,enlaces)
#     return nodos, enlaces

def generar_nodos_y_enlaces(nit_entidad=None, documento_proveedor=None, departamento=None, 
                            fecha_inicio=None, fecha_fin=None, valor_minimo=None, valor_maximo=None,
                            tamano_min=5, tamano_max=50):
    """
    Genera dos listas:
    1. nodos: Lista de diccionarios con entidades y proveedores
    2. enlaces: Lista de diccionarios con las relaciones entre entidades y proveedores
    """
    
    nodos = []
    enlaces = []
    enlaces_dict = {}
    # ============= CONSTRUIR QUERY BASE =============
    query_base = (db.contratos.id > 0)
    
    if nit_entidad:
        query_base &= (db.contratos.nit_entidad == nit_entidad)
    
    if documento_proveedor:
        query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    
    if departamento:
        query_base &= (db.contratos.departamento == departamento)
    
    if fecha_inicio:
        query_base &= (db.contratos.fecha_inicio >= fecha_inicio)
    
    if fecha_fin:
        query_base &= (db.contratos.fecha_fin <= fecha_fin)
    
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    # ============= PROCESAR ENTIDADES =============
    query_entidades = query_base & (db.contratos.nit_entidad != None)
    
    conteos = db(query_entidades).select(
        db.contratos.nit_entidad,
        db.contratos.nit_entidad.count().with_alias('cantidad'),
        db.contratos.valor_contrato.sum().with_alias('suma_valor'),
        groupby=db.contratos.nit_entidad,
        orderby=~db.contratos.nit_entidad.count()
    )
    
    # Lista temporal para almacenar nodos de entidades
    nodos_entidades = []
    
    for row in conteos:
        nitActual = row.contratos.nit_entidad
        valor_total = float(row.suma_valor or 0)
        cantidad_contratos = row.cantidad
        
        # Buscar datos de la entidad
        dataEntidad = db(db.entidades_personas.documento == nitActual).select().last()
        
        try:
            data_dict = dataEntidad.as_dict()
            nodo_entidad = {
                "id": nitActual,
                "name": data_dict.get('nombre', data_dict.get('nombre_entidad', f"Entidad {nitActual}")),
                "departamento": data_dict.get("departamento",""),
                "tipo": "entidad",
                "url": "",
                "color": '#6da2c4',
                "size": 0,
                "cantidad_contratos": cantidad_contratos,
                "valor_contrato": valor_total
            }
            nodos_entidades.append(nodo_entidad)
        except:
            print(nitActual)
            nodo_entidad = {
                "id": nitActual,
                "name": "Sin informacion",
                "departamento": "Sin informacion",
                "tipo": "entidad",
                "url": "",
                "color": '#6da2c4',
                "size": 0,
                "cantidad_contratos": cantidad_contratos,
                "valor_contrato": valor_total
            }
            nodos_entidades.append(nodo_entidad)
    
    # ============= PROCESAR PROVEEDORES =============
    query_proveedores = query_base & (db.contratos.documento_proveedor != None)
    
    conteosProv = db(query_proveedores).select(
        db.contratos.documento_proveedor,
        db.contratos.documento_proveedor.count().with_alias('cantidad'),
        db.contratos.valor_contrato.sum().with_alias('suma_valor'),
        groupby=db.contratos.documento_proveedor,
        orderby=~db.contratos.documento_proveedor.count()
    )
    
    # Lista temporal para almacenar nodos de proveedores
    nodos_proveedores = []
    
    for row in conteosProv:
        docActual = row.contratos.documento_proveedor
        valor_total = float(row.suma_valor or 0)
        cantidad_contratos = row.cantidad
        
        # Buscar datos del proveedor
        dataProv = db(db.entidades_personas.documento == docActual).select().last()
        
        try:
            data_dict = dataProv.as_dict()
            nodo_proveedor = {
                "id": docActual,
                "name": data_dict.get('nombre', data_dict.get('nombre_entidad', f"Proveedor {docActual}")),
                "es_pyme": data_dict.get('es_pyme', "No"),
                "es_grupo": data_dict.get('es_grupo', "No"),
                "tipo": "proveedor",
                "url": "",
                "color": "#1dc96a",
                "size": 0,
                "cantidad_contratos": cantidad_contratos,
                "valor_contrato": valor_total
            }
            nodos_proveedores.append(nodo_proveedor)
        except:
            print(docActual)
            nodo_proveedor = {
                "id": docActual,
                "name": "Sin informacion",
                "es_pyme": "Sin informacion",
                "es_grupo": "Sin informacion",
                "tipo": "proveedor",
                "url": "",
                "color": "#1dc96a",
                "size": 0,
                "cantidad_contratos": cantidad_contratos,
                "valor_contrato": valor_total
            }
            nodos_proveedores.append(nodo_proveedor)
    
    # ============= NORMALIZAR TAMAÑOS =============
    todos_nodos = nodos_entidades + nodos_proveedores
    
    if len(todos_nodos) > 0:
        valores = [nodo['valor_contrato'] for nodo in todos_nodos if nodo['valor_contrato'] > 0]
        
        if len(valores) > 0:
            valor_min_global = min(valores)
            valor_max_global = max(valores)
            rango_valores = valor_max_global - valor_min_global
            
            if rango_valores > 0:
                for nodo in todos_nodos:
                    if nodo['valor_contrato'] > 0:
                        valor_normalizado = (nodo['valor_contrato'] - valor_min_global) / rango_valores
                        nodo['size'] = tamano_min + valor_normalizado * (tamano_max - tamano_min)
                    else:
                        nodo['size'] = tamano_min
            else:
                tamano_promedio = (tamano_min + tamano_max) / 2
                for nodo in todos_nodos:
                    nodo['size'] = tamano_promedio
        else:
            for nodo in todos_nodos:
                nodo['size'] = tamano_min
    
    nodos = todos_nodos
    
    # ============= GENERAR ENLACES (RELACIONES) =============
    query_enlaces = query_base & (db.contratos.nit_entidad != None) & (db.contratos.documento_proveedor != None)
    
    contratos = db(query_enlaces).select(
        db.contratos.nit_entidad,
        db.contratos.documento_proveedor,
        db.contratos.valor_contrato
    )
    
    for contrato in contratos:
        source = contrato.nit_entidad
        target = contrato.documento_proveedor
        identificador = f"{source}_{target}"
        
        if identificador not in enlaces_dict:
            enlaces_dict[identificador] = {
                "source": source,
                "target": target,
                "identificador": identificador,
                "color": "#ff0000",
                "cantidad_contratos": 0,
                "valor_contrato": 0
            }
        
        enlaces_dict[identificador]["cantidad_contratos"] += 1
        enlaces_dict[identificador]["valor_contrato"] += float(contrato.valor_contrato or 0)
    
    enlaces = list(enlaces_dict.values())
    
    return nodos, enlaces

def generar_html_grafo(nodos, enlaces, titulo="Grafo de Contratos", html_full=True,str_url_tail=""):
    """
    Genera un archivo HTML con visualización de grafo D3.js a partir de nodos y enlaces
    
    Parámetros:
    - nodos: Lista de diccionarios con nodos (entidades y proveedores)
    - enlaces: Lista de diccionarios con enlaces entre nodos
    - titulo: Título del grafo
    - html_full: html completo
    - url_tail : modificador para los urls
    """
    # Convertir nodos a formato JavaScript
    nodos_js = "[\n"
    for i, nodo in enumerate(nodos):
        nname=nodo['name'].replace("'","")
        coma = "," if i < len(nodos) - 1 else ""
        nodos_js += f"    {{ id: '{nodo['id']}', name: '{nname}', "
        
        # Agregar campos según el tipo de nodo
        if nodo['tipo'] == 'entidad':
            nodos_js += f"dep: '{nodo.get('departamento', '')}', "
        else:  # proveedor
            nodos_js += f"es_pyme: '{nodo.get('es_pyme', 'No')}', es_grupo: '{nodo.get('es_grupo', 'No')}', "
        
        nodos_js += f"url: '{nodo.get('url', '')}', color: '{nodo['color']}', "
        nodos_js += f"cantidad_contratos: {nodo['cantidad_contratos']}, size: {nodo['size']}, valor: {nodo['valor_contrato']}, tipo: '{nodo['tipo']}'}}{coma}\n"
    nodos_js += "]"
    
    # Convertir enlaces a formato JavaScript
    enlaces_js = "[\n"
    for i, enlace in enumerate(enlaces):
        coma = "," if i < len(enlaces) - 1 else ""
        enlaces_js += f"    {{ source: '{enlace['source']}', target: '{enlace['target']}', "
        enlaces_js += f"identificador: '{enlace['identificador']}', color: '{enlace['color']}', "
        enlaces_js += f"cantidad_contratos: {enlace['cantidad_contratos']}, "
        enlaces_js += f"valor_contrato: {enlace['valor_contrato']}}}{coma}\n"
    enlaces_js += "]"
    
    # Generar items de leyenda para entidades
    leyenda_entidades = ""
    entidades_unicas = {}
    for nodo in nodos:
        if nodo['tipo'] == 'entidad':
            dep = nodo.get('departamento', 'Sin departamento')
            if dep not in entidades_unicas:
                entidades_unicas[dep] = nodo['color']
    
    for dep, color in list(entidades_unicas.items())[:10]:  # Limitar a 10 para no saturar
        leyenda_entidades += f"""
            <div class="legend-item">
                <div class="legend-color" style="background: {color};"></div>
                <span>{dep}</span>
            </div>"""
    
    # Template HTML
    if html_full:
        html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grafo de Contratos</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: Arial, sans-serif;
            background: #f5f5f5;
            align-items: center;
            justify-content: center;
        }}
        #graph {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            width: 97%;
            height: 97%;
        }}
        .node {{
            cursor: pointer;
            transition: r 0.2s;
        }}
        .node:hover {{
            stroke: #333;
            stroke-width: 3px;
        }}
        .node-label {{
            font-size: 12px;
            pointer-events: none;
            user-select: none;
            fill: #333;
            font-weight: 500;
        }}
        .link {{
            stroke-opacity: 0.6;
        }}
        .link:hover {{
            stroke: #2cf105;
            stroke-width: 3px;
        }}
        .tooltip {{
            position: absolute;
            padding: 8px 12px;
            background: rgba(0, 0, 0, 0.8);
            color: white;
            border-radius: 4px;
            font-size: 12px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s;
            z-index: 1000;
        }}
        .infodiv {{
            width: 100%;
            align-items: center;
            justify-content: center;
            display: flex;
            flex-wrap: wrap;
        }}
        #title {{
            width: 100%;
            height: 30px;
            background: gray;
            color: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            align-items: center;
            justify-content: center;
            display: flex;
        }}
        .legend {{
            position: fixed;
            top: 80px;
            right: 20px;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            max-width: 300px;
            max-height: 80vh;
            overflow-y: auto;
            z-index: 999;
            transition: transform 0.3s ease;
        }}
        .legend.hidden {{
            transform: translateX(350px);
        }}
        .legend h3 {{
            margin-top: 0;
            margin-bottom: 15px;
            font-size: 16px;
            color: #333;
            border-bottom: 2px solid #eee;
            padding-bottom: 8px;
        }}
        .legend-section {{
            margin-bottom: 20px;
        }}
        .legend-section h4 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            color: #666;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin-bottom: 8px;
            font-size: 12px;
        }}
        .legend-color {{
            width: 20px;
            height: 20px;
            border-radius: 50%;
            margin-right: 10px;
            border: 2px solid #fff;
            box-shadow: 0 1px 3px rgba(0,0,0,0.2);
        }}
        .legend-line {{
            width: 30px;
            height: 3px;
            margin-right: 10px;
        }}
        .toggle-legend-btn {{
            position: fixed;
            top: 30px;
            right: 20px;
            background: #4CAF50;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
            z-index: 1000;
            transition: background 0.3s;
        }}
        .toggle-legend-btn:hover {{
            background: #45a049;
        }}
    </style>
</head>
<body>
    <button class="toggle-legend-btn" onclick="toggleLegend()">Mostrar/Ocultar Leyenda</button>

    <div class="infodiv">
        <div id="title" class="title">
            <p style="margin: 0px">{titulo}</p>
        </div>
        <div id="tooltip" class="tooltip"></div>
        <svg id="graph"></svg>
        
        <div id="legend" class="legend">
            <h3>Leyenda del Grafo</h3>

            <div class="legend-section">
                <h4>Nodos - Entidades</h4>
                <div class="legend-item">
                    <div class="legend-color" style="background: #6da2c4;"></div>
                    <span>Entidades (azul)</span>
                </div>
            </div>

            <div class="legend-section">
                <h4>Nodos - Proveedores</h4>
                <div class="legend-item">
                    <div class="legend-color" style="background: #1dc96a;"></div>
                    <span>Proveedores (verde)</span>
                </div>
            </div>

            <div class="legend-section">
                <h4>Enlaces (Contratos)</h4>
                <div class="legend-item">
                    <div class="legend-line" style="background: #ff0000;"></div>
                    <span>Relación contractual</span>
                </div>
            </div>

            <div class="legend-section">
                <h4>Tamaño de nodos</h4>
                <div class="legend-item">
                    <span>Representa el número de contratos</span>
                </div>
            </div>

            <div class="legend-section">
                <h4>Estadísticas</h4>
                <div class="legend-item">
                    <span>Total nodos: {len(nodos)}</span>
                </div>
                <div class="legend-item">
                    <span>Total enlaces: {len(enlaces)}</span>
                </div>
                <div class="legend-item">
                    <span>Entidades: {len([n for n in nodos if n['tipo'] == 'entidad'])}</span>
                </div>
                <div class="legend-item">
                    <span>Proveedores: {len([n for n in nodos if n['tipo'] == 'proveedor'])}</span>
                </div>
            </div>
        </div>
    </div>

    <script>
        const nodes = {nodos_js};
        
        const links = {enlaces_js};

        function toggleLegend() {{
            const legend = document.getElementById('legend');
            legend.classList.toggle('hidden');
        }}

        function formatNumber(num) {{
            return new Intl.NumberFormat('es-CO', {{ 
                style: 'currency', 
                currency: 'COP',
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }}).format(num);
        }}

        // Configuración del SVG
        const width = window.innerWidth - 40;
        const height = window.innerHeight - 40;

        const svg = d3.select('#graph')
            .attr('width', width)
            .attr('height', height);

        const tooltip = d3.select('#tooltip');

        // Contenedor para zoom
        const g = svg.append('g');

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.1, 10])
            .on('zoom', (event) => {{
                g.attr('transform', event.transform);
            }});

        svg.call(zoom);
        svg.call(zoom.transform, d3.zoomIdentity
            .translate(width / 2, height / 2)
            .scale(0.5)
            .translate(-width / 2, -height / 2)
        );

        // Simulación de fuerzas
        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(50))
            .force('charge', d3.forceManyBody().strength(-2))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(d => d.size +5 ));

        // Crear enlaces
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke', d => d.color)
            .attr('stroke-width', d => Math.max(2, Math.min(10, d.cantidad_contratos / 2)))
            .on('mouseover', (event, d) => {{
                tooltip
                    .style('opacity', 1)
                    .html(`
                        <strong>Relación Contractual</strong><br>
                        Entidad: ${{nodes.find(n => n.id === d.source.id).name}}<br>
                        Proveedor: ${{nodes.find(n => n.id === d.target.id).name}}<br>
                        Contratos: ${{d.cantidad_contratos}}<br>
                        Valor total: ${{formatNumber(d.valor_contrato)}}
                    `)
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px');
                
                node
                    .filter(n => n.id === d.source.id || n.id === d.target.id)
                    .attr('stroke', '#2cf105')
                    .attr('stroke-width', 3);
            }})
            .on('mouseout', () => {{
                tooltip.style('opacity', 0);
                node
                    .attr('stroke', '#fff')
                    .attr('stroke-width', 2);
            }});

        // Crear nodos
        const node = g.append('g')
            .selectAll('circle')
            .data(nodes)
            .join('circle')
            .attr('class', 'node')
            .attr('r', d => Math.max(5, Math.min(50, d.size)))
            .attr('fill', d => d.color)
            .attr('stroke', '#fff')
            .attr('stroke-width', 2)
            .call(drag(simulation))
            .on('mouseover', (event, d) => {{
                let tooltipContent = `<strong>${{d.name}}</strong><br>`;
                tooltipContent += `Tipo: ${{d.tipo === 'entidad' ? 'Entidad' : 'Proveedor'}}<br>`;
                tooltipContent += `ID: ${{d.id}}<br>`;
                
                if (d.tipo === 'entidad') {{
                    tooltipContent += `Departamento: ${{d.dep || 'N/A'}}<br>`;
                }} else {{
                    tooltipContent += `PyME: ${{d.es_pyme}}<br>`;
                    tooltipContent += `Grupo: ${{d.es_grupo}}<br>`;
                }}
                
                tooltipContent += `Contratos: ${{d.cantidad_contratos}}<br>`;
                tooltipContent += `Valor total: ${{formatNumber(d.valor)}}`;
                
                tooltip
                    .style('opacity', 1)
                    .html(tooltipContent)
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px');
            }})
            .on('mouseout', () => {{
                tooltip.style('opacity', 0);
            }});

        // Etiquetas de nodos
        const label = g.append('g')
            .selectAll('text')
            .data(nodes)
            .join('text')
            .attr('class', 'node-label')
            .attr('text-anchor', 'middle')
            .attr('dy', d => -(Math.max(5, Math.min(50, d.size)) + 5));
            //.text(d => d.name.length > 30 ? d.name.substring(0, 30) + '...' : d.name);

        // Actualizar posiciones en cada tick
        simulation.on('tick', () => {{
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);

            node
                .attr('cx', d => d.x)
                .attr('cy', d => d.y);

            label
                .attr('x', d => d.x)
                .attr('y', d => d.y);
        }});

        // Función de arrastre
        function drag(simulation) {{
            function dragstarted(event) {{
                if (!event.active) simulation.alphaTarget(0.3).restart();
                event.subject.fx = event.subject.x;
                event.subject.fy = event.subject.y;
            }}

            function dragged(event) {{
                event.subject.fx = event.x;
                event.subject.fy = event.y;
            }}

            function dragended(event) {{
                if (!event.active) simulation.alphaTarget(0);
                event.subject.fx = null;
                event.subject.fy = null;
            }}

            return d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended);
        }}

        // Ajustar tamaño al cambiar ventana
        window.addEventListener('resize', () => {{
            const newWidth = window.innerWidth - 40;
            const newHeight = window.innerHeight - 40;
            svg.attr('width', newWidth).attr('height', newHeight);
            simulation.force('center', d3.forceCenter(newWidth / 2, newHeight / 2));
            simulation.alpha(0.3).restart();
        }});
    </script>
</body>
</html>"""
    else:
        html_content = f"""

    
    <div class="infodiv">
        <div id="title" class="title">
            <button onclick="location.href='/html/graph_rel/{str_url_tail}'" class="btn primary">Mas detalles</a>
        </div>
        <div id="tooltip" class="tooltip"></div>
        <svg id="graph"></svg>
        


    <script>
        const nodes = {nodos_js};
        
        const links = {enlaces_js};

        function toggleLegend() {{
            const legend = document.getElementById('legend');
            legend.classList.toggle('hidden');
        }}

        function formatNumber(num) {{
            return new Intl.NumberFormat('es-CO', {{ 
                style: 'currency', 
                currency: 'COP',
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }}).format(num);
        }}

        // Configuración del SVG
        const width = window.innerWidth - 40;
        const height = window.innerHeight - 40;

        const svg = d3.select('#graph')
            .attr('width', width)
            .attr('height', height);

        const tooltip = d3.select('#tooltip');

        // Contenedor para zoom
        const g = svg.append('g');

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.1, 10])
            .on('zoom', (event) => {{
                g.attr('transform', event.transform);
            }});

        svg.call(zoom);
        svg.call(zoom.transform, d3.zoomIdentity
            .translate(width / 2, height / 2)
            .scale(0.5)
            .translate(-width / 2, -height / 2)
        );

        // Simulación de fuerzas
        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(50))
            .force('charge', d3.forceManyBody().strength(-2))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(d => d.size +5 ));

        // Crear enlaces
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke', d => d.color)
            .attr('stroke-width', d => Math.max(2, Math.min(10, d.cantidad_contratos / 2)))
            .on('mouseover', (event, d) => {{
                tooltip
                    .style('opacity', 0)
                    .html(`
                        <strong>Relación Contractual</strong><br>
                        Entidad: ${{nodes.find(n => n.id === d.source.id).name}}<br>
                        Proveedor: ${{nodes.find(n => n.id === d.target.id).name}}<br>
                        Contratos: ${{d.cantidad_contratos}}<br>
                        Valor total: ${{formatNumber(d.valor_contrato)}}
                    `)
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px');
                
                node
                    .filter(n => n.id === d.source.id || n.id === d.target.id)
                    .attr('stroke', '#2cf105')
                    .attr('stroke-width', 3);
            }})
            .on('mouseout', () => {{
                tooltip.style('opacity', 0);
                node
                    .attr('stroke', '#fff')
                    .attr('stroke-width', 2);
            }});

        // Crear nodos
        const node = g.append('g')
            .selectAll('circle')
            .data(nodes)
            .join('circle')
            .attr('class', 'node')
            .attr('r', d => Math.max(5, Math.min(50, d.size)))
            .attr('fill', d => d.color)
            .attr('stroke', '#fff')
            .attr('stroke-width', 2)
            .call(drag(simulation))
            .on('mouseover', (event, d) => {{
                let tooltipContent = `<strong>${{d.name}}</strong><br>`;
                tooltipContent += `Tipo: ${{d.tipo === 'entidad' ? 'Entidad' : 'Proveedor'}}<br>`;
                tooltipContent += `ID: ${{d.id}}<br>`;
                
                if (d.tipo === 'entidad') {{
                    tooltipContent += `Departamento: ${{d.dep || 'N/A'}}<br>`;
                }} else {{
                    tooltipContent += `PyME: ${{d.es_pyme}}<br>`;
                    tooltipContent += `Grupo: ${{d.es_grupo}}<br>`;
                }}
                
                tooltipContent += `Contratos: ${{d.cantidad_contratos}}<br>`;
                tooltipContent += `Valor total: ${{formatNumber(d.valor)}}`;
                
                tooltip
                    .style('opacity', 0)
                    .html(tooltipContent)
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px');
            }})
            .on('mouseout', () => {{
                tooltip.style('opacity', 0);
            }});

        // Etiquetas de nodos
        const label = g.append('g')
            .selectAll('text')
            .data(nodes)
            .join('text')
            .attr('class', 'node-label')
            .attr('text-anchor', 'middle')
            .attr('dy', d => -(Math.max(5, Math.min(50, d.size)) + 5));
            //.text(d => d.name.length > 30 ? d.name.substring(0, 30) + '...' : d.name);

        // Actualizar posiciones en cada tick
        simulation.on('tick', () => {{
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);

            node
                .attr('cx', d => d.x)
                .attr('cy', d => d.y);

            label
                .attr('x', d => d.x)
                .attr('y', d => d.y);
        }});

        // Función de arrastre
        function drag(simulation) {{
            function dragstarted(event) {{
                if (!event.active) simulation.alphaTarget(0.3).restart();
                event.subject.fx = event.subject.x;
                event.subject.fy = event.subject.y;
            }}

            function dragged(event) {{
                event.subject.fx = event.x;
                event.subject.fy = event.y;
            }}

            function dragended(event) {{
                if (!event.active) simulation.alphaTarget(0);
                event.subject.fx = null;
                event.subject.fy = null;
            }}

            return d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended);
        }}

        // Ajustar tamaño al cambiar ventana
        window.addEventListener('resize', () => {{
            const newWidth = window.innerWidth - 40;
            const newHeight = window.innerHeight - 40;
            svg.attr('width', newWidth).attr('height', newHeight);
            simulation.force('center', d3.forceCenter(newWidth / 2, newHeight / 2));
            simulation.alpha(0.01).restart();
        }});
    </script>
"""
    


    return html_content











@app.get('/html/graph_rel/', response_class=HTMLResponse)
async def graph(request: Request,
    nit_entidad=None, 
    documento_proveedor=None, 
    valor_minimo=None, 
    valor_maximo=None,
    tamano_min:int=5, 
    tamano_max:int =200,
    html_full:bool=False
    ):
    
    nodos, enlaces = generar_nodos_y_enlaces(nit_entidad=nit_entidad,documento_proveedor=documento_proveedor,
                                            tamano_min=tamano_min, tamano_max=tamano_max,
                                            valor_minimo=valor_minimo,valor_maximo=valor_maximo)
    str_url_tail=make_url_tail(nit_entidad=nit_entidad,
    documento_proveedor=documento_proveedor,
    valor_minimo=valor_minimo,
    valor_maximo=valor_maximo,
    html_full=True)
    html_content = generar_html_grafo(
    nodos=nodos,
    enlaces=enlaces,
    titulo="Red de Contratos entidades",
    html_full=html_full,
    str_url_tail=str_url_tail
    )
    
    
    return HTMLResponse(content=html_content)#templates.TemplateResponse("graph_ente_prove.html", context)


@app.get("/html/contratos/total")
async def obtener_total_contratos(
    nit_entidad: Optional[str] = Query(None, description="Filtrar por NIT de entidad"),
    documento_proveedor: Optional[str] = Query(None, description="Filtrar por documento de proveedor"),
    departamento: Optional[str] = Query(None, description="Filtrar por departamento"),
    valor_minimo: Optional[float] = Query(None, description="Valor mínimo del contrato"),
    valor_maximo: Optional[float] = Query(None, description="Valor máximo del contrato"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)")
):
    """
    Retorna el total de contratos según los filtros aplicados.
    """
    
    # Construir query
    query_base = (db.contratos.id > 0)
    
    if nit_entidad:
        query_base &= (db.contratos.nit_entidad == nit_entidad)
    
    if documento_proveedor:
        query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    
    if departamento:
        query_base &= (db.contratos.departamento == departamento)
    
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    if fecha_inicio:
        fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        query_base &= (db.contratos.fecha_inicio >= fecha_inicio_dt)
    
    if fecha_fin:
        fecha_fin_dt = datetime.strptime(fecha_fin, '%Y-%m-%d')
        query_base &= (db.contratos.fecha_fin <= fecha_fin_dt)
    
    total = db(query_base).count()
    
    # Calcular valor total
    suma_valor = db.contratos.valor_contrato.sum()
    resultado_valor = db(query_base).select(suma_valor).first()
    valor_total = int(resultado_valor[suma_valor] or 0)
    str_url_tail=""
    html_content=f"""
    <div class="card">
        <img src="static/img/marker-icon.png" alt="Avatar" style="width:100%">
        <div class="container">
            <h4><b>{total}</b></h4>
            <p>Cantidad de contratos</p>
            <button onclick="location.href='/html/contratos{str_url_tail}'" class="btn primary">Mas detalles</a>
        </div>
    </div>


    <div class="card">
        <img src="static/img/marker-icon.png" alt="Avatar" style="width:100%">
        <div class="container">
            <h4><b>{valor_total}</b></h4>
            <p>Valor total de los pagos de contratos</p>
        
        </div>
    </div>  
"""
    return HTMLResponse(content=html_content)


@app.get("/api/entidades/total")
async def obtener_total_entidades(
    nit_entidad: Optional[str] = Query(None, description="Filtrar por NIT de entidad específico"),
    documento_proveedor: Optional[str] = Query(None, description="Filtrar por documento de proveedor"),
    departamento: Optional[str] = Query(None, description="Filtrar por departamento"),
    valor_minimo: Optional[float] = Query(None, description="Valor mínimo del contrato"),
    valor_maximo: Optional[float] = Query(None, description="Valor máximo del contrato"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)")
):
    """
    Retorna el total de entidades únicas según los filtros aplicados.
    """
    
    # Construir query
    query_base = (db.contratos.nit_entidad != None)
    
    if nit_entidad:
        query_base &= (db.contratos.nit_entidad == nit_entidad)
    
    if documento_proveedor:
        query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    
    if departamento:
        query_base &= (db.contratos.departamento == departamento)
    
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    if fecha_inicio:
        fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        query_base &= (db.contratos.fecha_inicio >= fecha_inicio_dt)
    
    if fecha_fin:
        fecha_fin_dt = datetime.strptime(fecha_fin, '%Y-%m-%d')
        query_base &= (db.contratos.fecha_fin <= fecha_fin_dt)
    
    # Contar entidades únicas
    entidades = db(query_base).select(
        db.contratos.nit_entidad,
        groupby=db.contratos.nit_entidad
    )
    
    total_entidades = len(entidades)
    
    return {
        "total_entidades": total_entidades
    }


@app.get("/api/proveedores/total")
async def obtener_total_proveedores(
    nit_entidad: Optional[str] = Query(None, description="Filtrar por NIT de entidad"),
    documento_proveedor: Optional[str] = Query(None, description="Filtrar por documento de proveedor específico"),
    departamento: Optional[str] = Query(None, description="Filtrar por departamento"),
    valor_minimo: Optional[float] = Query(None, description="Valor mínimo del contrato"),
    valor_maximo: Optional[float] = Query(None, description="Valor máximo del contrato"),
    fecha_inicio: Optional[str] = Query(None, description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: Optional[str] = Query(None, description="Fecha fin (YYYY-MM-DD)")
):
    """
    Retorna el total de proveedores únicos según los filtros aplicados.
    """
    
    # Construir query
    query_base = (db.contratos.documento_proveedor != None)
    
    if nit_entidad:
        query_base &= (db.contratos.nit_entidad == nit_entidad)
    
    if documento_proveedor:
        query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    
    if departamento:
        query_base &= (db.contratos.departamento == departamento)
    
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    if fecha_inicio:
        fecha_inicio_dt = datetime.strptime(fecha_inicio, '%Y-%m-%d')
        query_base &= (db.contratos.fecha_inicio >= fecha_inicio_dt)
    
    if fecha_fin:
        fecha_fin_dt = datetime.strptime(fecha_fin, '%Y-%m-%d')
        query_base &= (db.contratos.fecha_fin <= fecha_fin_dt)
    
    # Contar proveedores únicos
    proveedores = db(query_base).select(
        db.contratos.documento_proveedor,
        groupby=db.contratos.documento_proveedor
    )
    
    total_proveedores = len(proveedores)
    
    return {
        "total_proveedores": total_proveedores
    }

@app.get('/html/rankings', response_class=HTMLResponse)
async def rankings_page(request: Request,
    nit_entidad=None, 
    documento_proveedor=None, 
    valor_minimo=None, 
    valor_maximo=None):
    
    str_url_tail = make_url_tail(
        nit_entidad=nit_entidad,
        documento_proveedor=documento_proveedor,
        valor_minimo=valor_minimo,
        valor_maximo=valor_maximo
    )
    
    context = {
        "request": request,
        "str_url_tail": str_url_tail
    }
    
    return templates.TemplateResponse("rankings.html", context)


@app.get("/api/contratos/ranking-valor")
async def ranking_contratos_valor(
    limit: int = Query(20, description="Límite de resultados"),
    nit_entidad: Optional[str] = Query(None),
    documento_proveedor: Optional[str] = Query(None),
    valor_minimo: Optional[float] = Query(None),
    valor_maximo: Optional[float] = Query(None)
):
    """Retorna el ranking de contratos por valor"""
    
    query_base = (db.contratos.valor_contrato > 0)
    
    if nit_entidad:
        query_base &= (db.contratos.nit_entidad == nit_entidad)
    if documento_proveedor:
        query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    contratos = db(query_base).select(
        db.contratos.ALL,
        orderby=~db.contratos.valor_contrato,
        limitby=(0, limit)
    )
    
    resultado = []
    for c in contratos:
        # Obtener nombre de entidad
        entidad = db(db.entidades_personas.documento == c.nit_entidad).select().first()
        nombre_entidad = entidad.nombre if entidad else "Sin información"
        
        # Obtener nombre de proveedor
        proveedor = db(db.entidades_personas.documento == c.documento_proveedor).select().first()
        nombre_proveedor = proveedor.nombre if proveedor else "Sin información"
        
        resultado.append({
            "id_contrato": c.id_contrato,
            "referencia": c.referencia_contrato or "N/A",
            "nit_entidad": c.nit_entidad,
            "nombre_entidad": nombre_entidad,
            "documento_proveedor": c.documento_proveedor,
            "nombre_proveedor": nombre_proveedor,
            "valor_contrato": float(c.valor_contrato or 0),
            "estado": c.estado_contrato or "N/A",
            "fecha_firma": c.fecha_firma.isoformat() if c.fecha_firma else None
        })
    
    return {"contratos": resultado}


@app.get("/api/proveedores/ranking-contratos")
async def ranking_proveedores_contratos(
    limit: int = Query(20, description="Límite de resultados"),
    nit_entidad: Optional[str] = Query(None),
    valor_minimo: Optional[float] = Query(None),
    valor_maximo: Optional[float] = Query(None)
):
    """Retorna el ranking de proveedores por cantidad de contratos"""
    
    query_base = (db.contratos.documento_proveedor != None)
    
    if nit_entidad:
        query_base &= (db.contratos.nit_entidad == nit_entidad)
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    resultado_count = db.contratos.documento_proveedor.count()
    suma_valor = db.contratos.valor_contrato.sum()
    
    proveedores = db(query_base).select(
        db.contratos.documento_proveedor,
        resultado_count,
        suma_valor,
        groupby=db.contratos.documento_proveedor,
        orderby=~resultado_count,
        limitby=(0, limit)
    )
    
    resultado = []
    for p in proveedores:
        doc = p.contratos.documento_proveedor
        proveedor = db(db.entidades_personas.documento == doc).select().first()
        
        resultado.append({
            "documento": doc,
            "nombre": proveedor.nombre if proveedor else "Sin información",
            "es_pyme": proveedor.es_pyme if proveedor else "N/A",
            "cantidad_contratos": p[resultado_count],
            "valor_total": float(p[suma_valor] or 0)
        })
    
    return {"proveedores": resultado}


@app.get("/api/entidades/ranking-contratos")
async def ranking_entidades_contratos(
    limit: int = Query(20, description="Límite de resultados"),
    documento_proveedor: Optional[str] = Query(None),
    valor_minimo: Optional[float] = Query(None),
    valor_maximo: Optional[float] = Query(None)
):
    """Retorna el ranking de entidades por cantidad de contratos"""
    
    query_base = (db.contratos.nit_entidad != None)
    
    if documento_proveedor:
        query_base &= (db.contratos.documento_proveedor == documento_proveedor)
    if valor_minimo:
        query_base &= (db.contratos.valor_contrato >= valor_minimo)
    if valor_maximo:
        query_base &= (db.contratos.valor_contrato <= valor_maximo)
    
    resultado_count = db.contratos.nit_entidad.count()
    suma_valor = db.contratos.valor_contrato.sum()
    
    entidades = db(query_base).select(
        db.contratos.nit_entidad,
        resultado_count,
        suma_valor,
        groupby=db.contratos.nit_entidad,
        orderby=~resultado_count,
        limitby=(0, limit)
    )
    
    resultado = []
    for e in entidades:
        nit = e.contratos.nit_entidad
        entidad = db(db.entidades_personas.documento == nit).select().first()
        
        resultado.append({
            "nit": nit,
            "nombre": entidad.nombre if entidad else "Sin información",
            "departamento": entidad.departamento if entidad else "N/A",
            "cantidad_contratos": e[resultado_count],
            "valor_total": float(e[suma_valor] or 0)
        })
    
    return {"entidades": resultado}


@app.get("/api/contratos/ranking-retrasos")
async def ranking_contratos_retrasos(
    limit: int = Query(20, description="Límite de resultados"),
    nit_entidad: Optional[str] = Query(None),
    documento_proveedor: Optional[str] = Query(None)
):
    """Retorna el ranking de ejecuciones específicas con mayor retraso"""
    
    # Query base para ejecuciones con porcentajes válidos
    query_ejecuciones = (
        (db.ejecuciones.porcentaje_avance_esperado != None) & 
        (db.ejecuciones.porcentaje_avance_real != None) &
        (db.ejecuciones.porcentaje_avance_esperado > 0)
    )
    
    # Obtener todas las ejecuciones relevantes
    ejecuciones = db(query_ejecuciones).select(db.ejecuciones.ALL)
    
    # Crear lista de ejecuciones con retraso
    ejecuciones_con_retraso = []
    
    for ej in ejecuciones:
        id_contrato = str(ej.id_contrato) if ej.id_contrato else None
        
        if not id_contrato:
            continue
            
        esperado = float(ej.porcentaje_avance_esperado or 0)
        real = float(ej.porcentaje_avance_real or 0)
        retraso = esperado/100 - real
        
        if retraso > 0:  # Solo si hay retraso
            ejecuciones_con_retraso.append({
                'id_contrato': id_contrato,
                'ejecucion': ej,
                'retraso': retraso,
                'esperado': esperado/100,
                'real': real
            })
    
    # Ordenar por retraso descendente
    ejecuciones_con_retraso.sort(key=lambda x: x['retraso'], reverse=True)
    
    # Obtener IDs únicos de contratos para hacer una consulta eficiente
    ids_contratos_unicos = list(set([e['id_contrato'] for e in ejecuciones_con_retraso[:limit * 2]]))
    
    # Crear diccionario de contratos
    contratos_dict = {}
    if ids_contratos_unicos:
        contratos_rows = db(db.contratos.id_contrato.belongs(ids_contratos_unicos)).select()
        for c in contratos_rows:
            contratos_dict[c.id_contrato] = c
    
    # Construir resultado con información completa
    resultado = []
    for datos in ejecuciones_con_retraso:
        if len(resultado) >= limit:
            break
            
        id_contrato = datos['id_contrato']
        ej = datos['ejecucion']
        
        # Buscar el contrato
        contrato = contratos_dict.get(id_contrato)
        
        if not contrato:
            continue
        
        # Aplicar filtros adicionales si se especificaron
        if nit_entidad and contrato.nit_entidad != nit_entidad:
            continue
        if documento_proveedor and contrato.documento_proveedor != documento_proveedor:
            continue
        
        # Obtener nombres de entidad y proveedor
        entidad = db(db.entidades_personas.documento == contrato.nit_entidad).select().first()
        proveedor = db(db.entidades_personas.documento == contrato.documento_proveedor).select().first()
        
        resultado.append({
            "id_contrato": contrato.id_contrato,
            "referencia": contrato.referencia_contrato or "N/A",
            "nombre_entidad": entidad.nombre if entidad else "Sin información",
            "nombre_proveedor": proveedor.nombre if proveedor else "Sin información",
            "valor_contrato": float(contrato.valor_contrato or 0),
            "estado_contrato": contrato.estado_contrato or "N/A",
            "fecha_inicio": contrato.fecha_inicio.isoformat() if contrato.fecha_inicio else None,
            "fecha_fin": contrato.fecha_fin.isoformat() if contrato.fecha_fin else None,
            # Información específica de la ejecución con retraso
            "ejecucion": {
                "tipo_ejecucion": ej.tipo_ejecucion or "N/A",
                "nombre_plan": ej.nombre_plan or "N/A",
                "descripcion": ej.descripcion or "N/A",
                "estado_ejecucion": ej.estado_contrato or "N/A",
                "fecha_entrega_esperada": ej.fecha_entrega_esperada.isoformat() if ej.fecha_entrega_esperada else None,
                "fecha_entrega_real": ej.fecha_entrega_real.isoformat() if ej.fecha_entrega_real else None,
                "porcentaje_avance_esperado": datos['esperado'],
                "porcentaje_avance_real": datos['real'],
                "retraso_porcentual": datos['retraso'],
                "cantidad_planeada": float(ej.cantidad_planeada or 0),
                "cantidad_recibida": float(ej.cantidad_recibida or 0),
                "cantidad_por_recibir": float(ej.cantidad_por_recibir or 0),
                "unidad": ej.unidad or "N/A"
            }
        })
    
    return {"ejecuciones": resultado}


@app.get("/api/contratos/{id_contrato}/ejecuciones")
async def obtener_ejecuciones_contrato(id_contrato: str):
    """Retorna todas las ejecuciones de un contrato específico por su id_contrato"""
    
    # Buscar el contrato por id_contrato
    contrato = db(db.contratos.id_contrato == id_contrato).select().first()
    
    if not contrato:
        raise HTTPException(status_code=404, detail="Contrato no encontrado")
    
    # Obtener ejecuciones usando id_contrato
    ejecuciones = db(db.ejecuciones.id_contrato == id_contrato).select(
        orderby=db.ejecuciones.fecha_entrega_esperada
    )
    
    # Obtener información de entidad y proveedor
    entidad = db(db.entidades_personas.documento == contrato.nit_entidad).select().first()
    proveedor = db(db.entidades_personas.documento == contrato.documento_proveedor).select().first()
    
    resultado = {
        "contrato": {
            "id_contrato": contrato.id_contrato,
            "referencia": contrato.referencia_contrato or "N/A",
            "nombre_entidad": entidad.nombre if entidad else "Sin información",
            "nombre_proveedor": proveedor.nombre if proveedor else "Sin información",
            "valor_contrato": float(contrato.valor_contrato or 0),
            "estado": contrato.estado_contrato or "N/A",
            "fecha_inicio": contrato.fecha_inicio.isoformat() if contrato.fecha_inicio else None,
            "fecha_fin": contrato.fecha_fin.isoformat() if contrato.fecha_fin else None
        },
        "ejecuciones": []
    }
    
    for ej in ejecuciones:
        esperado = float(ej.porcentaje_avance_esperado or 0)
        real = float(ej.porcentaje_avance_real or 0)
        retraso = esperado/100 - real
        print(retraso)
        resultado["ejecuciones"].append({
            "tipo_ejecucion": ej.tipo_ejecucion or "N/A",
            "nombre_plan": ej.nombre_plan or "N/A",
            "fecha_entrega_esperada": ej.fecha_entrega_esperada.isoformat() if ej.fecha_entrega_esperada else None,
            "fecha_entrega_real": ej.fecha_entrega_real.isoformat() if ej.fecha_entrega_real else None,
            "porcentaje_avance_esperado": esperado,
            "porcentaje_avance_real": real,
            "retraso": retraso,
            "estado_contrato": ej.estado_contrato or "N/A",
            "descripcion": ej.descripcion or "N/A",
            "cantidad_adjudicada": float(ej.cantidad_adjudicada or 0),
            "cantidad_planeada": float(ej.cantidad_planeada or 0),
            "cantidad_recibida": float(ej.cantidad_recibida or 0),
            "cantidad_por_recibir": float(ej.cantidad_por_recibir or 0),
            "unidad": ej.unidad or "N/A"
        })
    
    return resultado