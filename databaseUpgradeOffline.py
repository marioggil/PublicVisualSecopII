import polars as pl
from datetime import datetime
import time
from models.db import db

# ==================== MAPEO DE COLUMNAS ====================

# Mapeo para CONTRATOS
MAPEO_CONTRATOS = {
    'Nombre Entidad': 'nombre_entidad',
    'Nit Entidad': 'nit_entidad',
    'Departamento': 'departamento',
    'Ciudad': 'ciudad',
    'Localización': 'localizacion',
    'Orden': 'orden',
    'Sector': 'sector',
    'Rama': 'rama',
    'Entidad Centralizada': 'entidad_centralizada',
    'Proceso de Compra': 'proceso_compra',
    'ID Contrato': 'id_contrato',
    'Referencia del Contrato': 'referencia_contrato',
    'Estado Contrato': 'estado_contrato',
    'Codigo de Categoria Principal': 'codigo_categoria_principal',
    'Descripcion del Proceso': 'descripcion_proceso',
    'Tipo de Contrato': 'tipo_contrato',
    'Modalidad de Contratacion': 'modalidad_contratacion',
    'Justificacion Modalidad de Contratacion': 'justificacion_modalidad',
    'Fecha de Firma': 'fecha_firma',
    'Fecha de Inicio del Contrato': 'fecha_inicio',
    'Fecha de Fin del Contrato': 'fecha_fin',
    'Condiciones de Entrega': 'condiciones_entrega',
    'TipoDocProveedor': 'tipo_doc_proveedor',
    'Documento Proveedor': 'documento_proveedor',
    'Proveedor Adjudicado': 'proveedor_adjudicado',
    'Es Grupo': 'es_grupo',
    'Es Pyme': 'es_pyme',
    'Habilita Pago Adelantado': 'habilita_pago_adelantado',
    'Liquidación': 'liquidacion',
    'Obligación Ambiental': 'obligacion_ambiental',
    'Obligaciones Postconsumo': 'obligaciones_postconsumo',
    'Reversion': 'reversion',
    'Origen de los Recursos': 'origen_recursos',
    'Destino Gasto': 'destino_gasto',
    'Valor del Contrato': 'valor_contrato',
    'Valor de pago adelantado': 'valor_pago_adelantado',
    'Valor Facturado': 'valor_facturado',
    'Valor Pendiente de Pago': 'valor_pendiente_pago',
    'Valor Pagado': 'valor_pagado',
    'Valor Amortizado': 'valor_amortizado',
    'Valor Pendiente de Amortizacion': 'valor_pendiente_amortizacion',
    'Valor Pendiente de Ejecucion': 'valor_pendiente_ejecucion',
    'Estado BPIN': 'estado_bpin',
    'Código BPIN': 'codigo_bpin',
    'Anno BPIN': 'anno_bpin',
    'Saldo CDP': 'saldo_cdp',
    'Saldo Vigencia': 'saldo_vigencia',
    'EsPostConflicto': 'es_postconflicto',
    'Dias adicionados': 'dias_adicionados',
    'Puntos del Acuerdo': 'puntos_acuerdo',
    'Pilares del Acuerdo': 'pilares_acuerdo',
    'URLProceso': 'url_proceso',
    'Nombre Representante Legal': 'nombre_representante_legal',
    'Nacionalidad Representante Legal': 'nacionalidad_representante_legal',
    'Domicilio Representante Legal': 'domicilio_representante_legal',
    'Tipo de Identificación Representante Legal': 'tipo_identificacion_representante_legal',
    'Identificación Representante Legal': 'identificacion_representante_legal',
    'Género Representante Legal': 'genero_representante_legal',
    'Presupuesto General de la Nacion – PGN': 'presupuesto_pgn',
    'Sistema General de Participaciones': 'sistema_participaciones',
    'Sistema General de Regalías': 'sistema_regalias',
    'Recursos Propios (Alcaldías, Gobernaciones y Resguardos Indígenas)': 'recursos_propios_alcaldias',
    'Recursos de Credito': 'recursos_credito',
    'Recursos Propios': 'recursos_propios',
    'Ultima Actualizacion': 'ultima_actualizacion',
    'Codigo Entidad': 'codigo_entidad',
    'Codigo Proveedor': 'codigo_proveedor',
    'Fecha Inicio Liquidacion': 'fecha_inicio_liquidacion',
    'Fecha Fin Liquidacion': 'fecha_fin_liquidacion',
    'Objeto del Contrato': 'objeto_contrato',
    'Duración del contrato': 'duracion_contrato',
    'Nombre del banco': 'nombre_banco',
    'Tipo de cuenta': 'tipo_cuenta',
    'Número de cuenta': 'numero_cuenta',
    'El contrato puede ser prorrogado': 'puede_prorrogarse',
    'Fecha de notificación de prorrogación': 'fecha_notificacion_prorroga',
    'Nombre ordenador del gasto': 'nombre_ordenador_gasto',
    'Tipo de documento Ordenador del gasto': 'tipo_doc_ordenador_gasto',
    'Número de documento Ordenador del gasto': 'num_doc_ordenador_gasto',
    'Nombre supervisor': 'nombre_supervisor',
    'Tipo de documento supervisor': 'tipo_doc_supervisor',
    'Número de documento supervisor': 'num_doc_supervisor',
    'Nombre Ordenador de Pago': 'nombre_ordenador_pago',
    'Tipo de documento Ordenador de Pago': 'tipo_doc_ordenador_pago',
    'Número de documento Ordenador de Pago': 'num_doc_ordenador_pago',
    'Documentos Tipo': 'documentos_tipo',
    'Descripcion Documentos Tipo': 'descripcion_documentos_tipo'
}

# Mapeo para ADICIONES
MAPEO_ADICIONES = {
    'Identificador': 'id_adicion',
    'ID_Contrato': 'id_contrato',
    'Tipo': 'tipo_modificacion',
    'Descripcion': 'descripcion',
    'FechaRegistro': 'fecha_registro'
}

# Mapeo para EJECUCIONES
MAPEO_EJECUCIONES = {
    'Identificador del Contrato': 'id_contrato',
    'Tipo de Ejecucion': 'tipo_ejecucion',
    'Nombre del Plan': 'nombre_plan',
    'Fecha de Entrega Esperada': 'fecha_entrega_esperada',
    'Porcentaje de Avance Esperado': 'porcentaje_avance_esperado',
    'Fecha de Entrega Real': 'fecha_entrega_real',
    'Porcentaje de avance real': 'porcentaje_avance_real',
    'Estado del contrato': 'estado_contrato',
    'Referencia de articulos': 'referencia_articulos',
    'Descripción': 'descripcion',
    'Unidad': 'unidad',
    'Cantidad adjudicada': 'cantidad_adjudicada',
    'Cantidad planeada': 'cantidad_planeada',
    'Cantidad Recibida': 'cantidad_recibida',
    'Cantidad por Recibir': 'cantidad_por_recibir',
    'Fecha Creacion': 'fecha_creacion'
}

# ==================== FUNCIONES DE LIMPIEZA ====================

def limpiar_documento(valor):
    """Limpia documentos removiendo caracteres especiales."""
    if valor is None:
        return None
    if isinstance(valor, str):
        return valor.replace('.', '').replace(',', '').replace("'", '').strip()
    return valor


def limpiar_valores_nulos(valor):
    """Convierte valores como 'No Definido' a None."""
    if valor is None:
        return None
    if isinstance(valor, str):
        valores_nulos = ['no definido', 'no válido', 'sin descripcion', 'sin descripción']
        if valor.strip().lower() in valores_nulos:
            return None
    return valor

def limpiar_porcentaje(valor):
    """
    Limpia valores de porcentaje y los convierte a decimal.
    Ejemplo: '2,500%' -> 2.5 o '25%' -> 25.0
    """
    if valor is None:
        return None
    
    if isinstance(valor, str):
        # Remover el símbolo %
        valor = valor.replace('%', '').strip()
        
        # Si está vacío, retornar None
        if not valor:
            return None
        
        try:
            # Reemplazar coma por punto para decimales
            valor = valor.replace(',', '.')
            return float(valor)
        except:
            return None
    
    # Si ya es numérico, retornarlo
    if isinstance(valor, (int, float)):
        return float(valor)
    
    return None




# ==================== CARGA DE ADICIONES ====================

def cargar_adiciones_desde_polars(archivo_path, formato='csv', encoding='utf-8'):
    """
    Carga adiciones desde un archivo usando Polars.
    
    Args:
        archivo_path (str): Ruta del archivo a cargar
        formato (str): Formato del archivo ('csv', 'parquet', 'excel')
        encoding (str): Codificación del archivo
    
    Returns:
        dict: Estadísticas de la carga
    """
    print(f"Iniciando carga de adiciones desde: {archivo_path}")
    inicio = time.time()
    
    stats = {
        'leidos': 0,
        'insertados': 0,
        'actualizados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    try:
        # Leer archivo según formato
        if formato == 'csv':
            # Leer con try_parse_dates=False para manejar fechas manualmente
            df = pl.read_csv(
                archivo_path, 
                encoding=encoding, 
                ignore_errors=True,
                try_parse_dates=False,
                infer_schema_length=10000
            )
        elif formato == 'parquet':
            df = pl.read_parquet(archivo_path)
        elif formato == 'excel':
            df = pl.read_excel(archivo_path)
        else:
            raise ValueError(f"Formato no soportado: {formato}")
        
        stats['leidos'] = len(df)
        print(f"Registros leídos: {stats['leidos']}")
        
        # Renombrar columnas según mapeo
        df = df.rename(MAPEO_ADICIONES)
        
        # Convertir fechas manualmente si es necesario
        if 'fecha_registro' in df.columns:
            # Intentar parsear fechas con diferentes formatos
            try:
                # Formato DD/MM/YYYY (sin hora)
                df = df.with_columns(
                    pl.col('fecha_registro').str.strptime(
                        pl.Datetime, 
                        format='%d/%m/%Y',
                        strict=False
                    ).alias('fecha_registro')
                )
            except:
                try:
                    # Formato DD/MM/YYYY HH:MM:SS
                    df = df.with_columns(
                        pl.col('fecha_registro').str.strptime(
                            pl.Datetime, 
                            format='%d/%m/%Y %H:%M:%S',
                            strict=False
                        ).alias('fecha_registro')
                    )
                except:
                    try:
                        # Formato MM/DD/YYYY (sin hora)
                        df = df.with_columns(
                            pl.col('fecha_registro').str.strptime(
                                pl.Datetime, 
                                format='%m/%d/%Y',
                                strict=False
                            ).alias('fecha_registro')
                        )
                    except:
                        try:
                            # Formato MM/DD/YYYY HH:MM:SS
                            df = df.with_columns(
                                pl.col('fecha_registro').str.strptime(
                                    pl.Datetime, 
                                    format='%m/%d/%Y %H:%M:%S',
                                    strict=False
                                ).alias('fecha_registro')
                            )
                        except:
                            print("Advertencia: No se pudieron parsear todas las fechas, se mantendrán como texto")
        
        # Convertir a lista de diccionarios
        registros = df.to_dicts()
        
        # Procesar cada registro
        for i, registro in enumerate(registros):
            try:
                registro_limpio = {k: limpiar_valores_nulos(v) for k, v in registro.items()}
                
                # Verificar si la adición ya existe
                id_adicion = registro_limpio.get('id_adicion')
                if id_adicion:
                    existe = db(db.adiciones.id_adicion == id_adicion).select().first()
                    
                    if existe:
                        db(db.adiciones.id_adicion == id_adicion).update(**registro_limpio)
                        stats['actualizados'] += 1
                    else:
                        db.adiciones.insert(**registro_limpio)
                        stats['insertados'] += 1
                
                if (i + 1) % 50 == 0:
                    print(f"Procesados: {i + 1}/{stats['leidos']}")
                    db.commit()
                
            except Exception as e:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'error': str(e),
                    'id_adicion': registro.get('id_adicion', 'N/A')
                })
        
        db.commit()
        
    except Exception as e:
        db.rollback()
        stats['error_general'] = str(e)
        print(f"Error general: {str(e)}")
    
    tiempo_total = time.time() - inicio
    stats['tiempo_total_segundos'] = tiempo_total
    
    print(f"\n=== Resumen de Carga de Adiciones ===")
    print(f"Registros leídos: {stats['leidos']}")
    print(f"Registros insertados: {stats['insertados']}")
    print(f"Registros actualizados: {stats['actualizados']}")
    print(f"Errores: {stats['errores']}")
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    
    return stats


# ==================== CARGA DE EJECUCIONES ====================

def cargar_ejecuciones_desde_polars(archivo_path, formato='csv', encoding='utf-8'):
    """
    Carga ejecuciones desde un archivo usando Polars.
    
    Args:
        archivo_path (str): Ruta del archivo a cargar
        formato (str): Formato del archivo ('csv', 'parquet', 'excel')
        encoding (str): Codificación del archivo
    
    Returns:
        dict: Estadísticas de la carga
    """
    print(f"Iniciando carga de ejecuciones desde: {archivo_path}")
    inicio = time.time()
    
    stats = {
        'leidos': 0,
        'insertados': 0,
        'actualizados': 0,
        'errores': 0,
        'detalles_errores': []
    }
    
    try:
        # Leer archivo según formato
        if formato == 'csv':
            df = pl.read_csv(
                archivo_path, 
                encoding=encoding, 
                ignore_errors=True,
                try_parse_dates=False,
                infer_schema_length=10000
            )
        elif formato == 'parquet':
            df = pl.read_parquet(archivo_path)
        elif formato == 'excel':
            df = pl.read_excel(archivo_path)
        else:
            raise ValueError(f"Formato no soportado: {formato}")
        
        stats['leidos'] = len(df)
        print(f"Registros leídos: {stats['leidos']}")
        
        # Renombrar columnas según mapeo
        df = df.rename(MAPEO_EJECUCIONES)
        
        # Limpiar porcentajes antes de convertir fechas
        if 'porcentaje_avance_esperado' in df.columns:
            df = df.with_columns(
                pl.col('porcentaje_avance_esperado').map_elements(
                    limpiar_porcentaje, 
                    return_dtype=pl.Float64
                ).alias('porcentaje_avance_esperado')
            )
        
        if 'porcentaje_avance_real' in df.columns:
            df = df.with_columns(
                pl.col('porcentaje_avance_real').map_elements(
                    limpiar_porcentaje, 
                    return_dtype=pl.Float64
                ).alias('porcentaje_avance_real')
            )
        
        # Convertir fechas manualmente
        columnas_fecha = ['fecha_entrega_esperada', 'fecha_entrega_real', 'fecha_creacion']
        for col_fecha in columnas_fecha:
            if col_fecha in df.columns:
                try:
                    # Formato DD/MM/YYYY (sin hora)
                    df = df.with_columns(
                        pl.col(col_fecha).str.strptime(
                            pl.Datetime, 
                            format='%m/%d/%Y',
                            strict=False
                        ).alias(col_fecha)
                    )
                except:
                    try:
                        # Formato DD/MM/YYYY HH:MM:SS
                        df = df.with_columns(
                            pl.col(col_fecha).str.strptime(
                                pl.Datetime, 
                                format='%m/%d/%Y %H:%M:%S',
                                strict=False
                            ).alias(col_fecha)
                        )
                    except:
                        try:
                            # Formato MM/DD/YYYY (sin hora)
                            df = df.with_columns(
                                pl.col(col_fecha).str.strptime(
                                    pl.Datetime, 
                                    format='%d/%m/%Y',
                                    strict=False
                                ).alias(col_fecha)
                            )
                        except:
                            try:
                                # Formato MM/DD/YYYY HH:MM:SS
                                df = df.with_columns(
                                    pl.col(col_fecha).str.strptime(
                                        pl.Datetime, 
                                        format='%d/%m/%Y %H:%M:%S',
                                        strict=False
                                    ).alias(col_fecha)
                                )
                            except:
                                print(f"Advertencia: No se pudo parsear la columna {col_fecha}")
        
        # Convertir a lista de diccionarios
        registros = df.to_dicts()
        
        # Procesar cada registro
        for i, registro in enumerate(registros):
            try:
                registro_limpio = {k: limpiar_valores_nulos(v) for k, v in registro.items()}
                
                # Para ejecuciones, insertar siempre (no hay clave única clara)
                db.ejecuciones.insert(**registro_limpio)
                stats['insertados'] += 1
                
                if (i + 1) % 1000 == 0:
                    db.commit()
                    print(f"Procesados: {i + 1}/{stats['leidos']}")
                
            except Exception as e:
                stats['errores'] += 1
                stats['detalles_errores'].append({
                    'indice': i,
                    'error': str(e),
                    'id_contrato': registro.get('id_contrato', 'N/A')
                })
        
        db.commit()
        
    except Exception as e:
        db.rollback()
        stats['error_general'] = str(e)
        print(f"Error general: {str(e)}")
    
    tiempo_total = time.time() - inicio
    stats['tiempo_total_segundos'] = tiempo_total
    
    print(f"\n=== Resumen de Carga de Ejecuciones ===")
    print(f"Registros leídos: {stats['leidos']}")
    print(f"Registros insertados: {stats['insertados']}")
    print(f"Registros actualizados: {stats['actualizados']}")
    print(f"Errores: {stats['errores']}")
    print(f"Tiempo total: {tiempo_total:.2f} segundos")
    
    return stats

if __name__ == "__main__":


    stats_adiciones = cargar_adiciones_desde_polars(
        'data/aprobados/SECOP_II_-_Adiciones_20251120.csv',
        formato='csv',
        encoding='utf-8'
    )

    
    # Ejemplo de uso para cargar ejecuciones
    stats_ejecuciones = cargar_ejecuciones_desde_polars(
        'data/aprobados/SECOP_II_-_Ejecución_Contratos_20251120.csv',
        formato='csv',
        encoding='utf-8'
    )

