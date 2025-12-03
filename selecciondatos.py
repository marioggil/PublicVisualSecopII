from pydal import DAL, Field
import os
import datetime
from models.db import db
from models.db_noclean import db as db_noclean
def setup_database(db_name, folder='databases'):
    """Configura la conexión a la base de datos"""
    pwd = os.getcwd()
    db_folder = pwd + '/' + folder
    try:
        os.stat(db_folder)
    except:
        os.mkdir(db_folder)
    
    return DAL(f'sqlite://{db_name}', folder=folder)




def define_tables(db):
    """Define las tablas necesarias"""
    db.define_table('contratos',
        Field('id_contrato', 'string'),
        Field('proceso_compra', 'string'),
        Field('nit_entidad', 'string'),
        Field('referencia_contrato', 'string'),
        Field('estado_contrato', 'string'),
        Field('tipo_contrato', 'string'),
        Field('modalidad_contratacion', 'string'),
        Field('justificacion_modalidad', 'string'),
        Field('codigo_categoria_principal', 'string'),
        Field('descripcion_proceso', 'text'),
        Field('fecha_firma', 'datetime'),
        Field('fecha_inicio', 'datetime'),
        Field('fecha_fin', 'datetime'),
        Field('condiciones_entrega', 'string'),
        Field('documento_proveedor', 'string'),
        Field('habilita_pago_adelantado', 'string'),
        Field('liquidacion', 'string'),
        Field('obligacion_ambiental', 'string'),
        Field('obligaciones_postconsumo', 'string'),
        Field('reversion', 'string'),
        Field('origen_recursos', 'string'),
        Field('destino_gasto', 'string'),
        Field('valor_contrato', 'decimal(15,2)'),
        Field('valor_pago_adelantado', 'decimal(15,2)'),
        Field('valor_facturado', 'decimal(15,2)'),
        Field('valor_pendiente_pago', 'decimal(15,2)'),
        Field('valor_pagado', 'decimal(15,2)'),
        Field('valor_amortizado', 'decimal(15,2)'),
        Field('valor_pendiente_amortizacion', 'decimal(15,2)'),
        Field('valor_pendiente_ejecucion', 'decimal(15,2)'),
        Field('estado_bpin', 'string'),
        Field('codigo_bpin', 'string'),
        Field('anno_bpin', 'string'),
        Field('saldo_cdp', 'decimal(15,2)'),
        Field('saldo_vigencia', 'decimal(15,2)'),
        Field('es_postconflicto', 'string'),
        Field('dias_adicionados', 'integer'),
        Field('puntos_acuerdo', 'string'),
        Field('pilares_acuerdo', 'string'),
        Field('url_proceso', 'string'),
        Field('identificacion_representante_legal', 'string'),
        Field('presupuesto_pgn', 'decimal(15,2)'),
        Field('sistema_participaciones', 'decimal(15,2)'),
        Field('sistema_regalias', 'decimal(15,2)'),
        Field('recursos_propios_alcaldias', 'decimal(15,2)'),
        Field('recursos_credito', 'decimal(15,2)'),
        Field('recursos_propios', 'decimal(15,2)'),
        Field('ultima_actualizacion', 'datetime'),
        Field('codigo_entidad', 'string'),
        Field('codigo_proveedor', 'string'),
        Field('fecha_inicio_liquidacion', 'datetime'),
        Field('fecha_fin_liquidacion', 'datetime'),
        Field('objeto_contrato', 'text'),
        Field('duracion_contrato', 'string'),
        Field('nombre_banco', 'string'),
        Field('tipo_cuenta', 'string'),
        Field('numero_cuenta', 'string'),
        Field('puede_prorrogarse', 'string'),
        Field('num_doc_ordenador_gasto', 'string'),
        Field('num_doc_supervisor', 'string'),
        Field('num_doc_ordenador_pago', 'string')
    )

    db.define_table('adiciones',
        Field('id_adicion', 'string', unique=True),
        Field('id_contrato', 'string'),
        Field('tipo_modificacion', 'string'),
        Field('descripcion', 'text'),
        Field('fecha_registro', 'datetime')
    )

    db.define_table('ejecuciones',
        Field('id_contrato', 'string'),
        Field('tipo_ejecucion', 'string'),
        Field('nombre_plan', 'string'),
        Field('fecha_entrega_esperada', 'datetime'),
        Field('porcentaje_avance_esperado', 'decimal(10,6)'),
        Field('fecha_entrega_real', 'datetime'),
        Field('porcentaje_avance_real', 'decimal(10,6)'),
        Field('estado_contrato', 'string'),
        Field('referencia_articulos', 'string'),
        Field('descripcion', 'text'),
        Field('unidad', 'string'),
        Field('cantidad_adjudicada', 'decimal(15,6)'),
        Field('cantidad_planeada', 'decimal(15,6)'),
        Field('cantidad_recibida', 'decimal(15,6)'),
        Field('cantidad_por_recibir', 'decimal(15,6)'),
        Field('fecha_creacion', 'datetime')
    )
    db.define_table('entidades_personas',
        Field('documento', 'string', unique=True, notnull=True),
        Field("tipo_documento","string", notnull=True),
        Field('nombre', 'string'),
        Field('departamento', 'string'),
        Field('ciudad', 'string'),
        Field('orden', 'string'),
        Field('sector', 'string'),
        Field('rama', 'string'),
        Field('entidad_centralizada', 'string'),
        Field('nacionalidad', 'string'),
        Field('genero', 'string'),
        Field('domicilio', 'string'),
        Field('es_grupo', 'string'),
        Field('es_pyme', 'string'),
        Field('es_entidad', 'string'),
        Field('es_proveedor', 'string'),
        Field('es_representante_legal', 'string'),
        Field('es_ordenador_del_gasto', 'string'),
        Field('es_supervisor', 'string')
    )

    db.define_table('sancionados',
        Field('documento', 'string', notnull=True),
        Field('tipo_inhabilitacion', 'string'),
        Field('nombre_completo', 'string'),
        Field('sancion', 'text'),
        Field('fecha_efectos_juridicos', 'date'),
        Field('numero_resolucion', 'string'),
        Field('origen', 'string')
    )

def dividir_en_lotes(lista, tamano_lote=100):
    """
    Divide una lista en sublistas de tamaño fijo
    
    Args:
        lista: Lista a dividir
        tamano_lote: Tamaño de cada sublista (default: 50)
    
    Returns:
        Lista de sublistas
    
    Ejemplos:
        >>> dividir_en_lotes([1,2,3,4,5], 2)
        [[1, 2], [3, 4], [5]]
        
        >>> dividir_en_lotes(range(125), 50)
        # Retorna 3 sublistas: [0-49], [50-99], [100-124]
    """
    sublistas = []
    for i in range(0, len(lista), tamano_lote):
        sublistas.append(lista[i:i + tamano_lote])
    return sublistas

def copiar_contratos_multiples(lista_id_contratos, db_origen='contratos.db', db_destino='contratos_copia.db'):
    """
    Copia múltiples contratos y todos sus registros relacionados en una sola operación
    
    Args:
        lista_id_contratos: Lista de IDs de contratos a copiar
        db_origen: Nombre del archivo de base de datos origen
        db_destino: Nombre del archivo de base de datos destino
    
    Returns:
        Dict con estadísticas de la operación
    """
    if not lista_id_contratos:
        print("⚠️  La lista de contratos está vacía")
        return {'exitosos': 0, 'fallidos': 0, 'total': 0}
    
    # Conectar a ambas bases de datos
    db_src = db_noclean
    db_dst = db
    
    # Definir tablas en ambas bases de datos

    
    estadisticas = {
        'exitosos': 0,
        'fallidos': 0,
        'total': len(lista_id_contratos),
        'contratos_copiados': 0,
        'contratos_actualizados': 0,
        'adiciones_copiadas': 0,
        'ejecuciones_copiadas': 0,
        'no_encontrados': []
    }
    
    try:
        print(f"\n{'='*80}")
        print(f"COPIANDO {len(lista_id_contratos)} CONTRATOS")
        print(f"{'='*80}\n")
        
        # 1. BUSCAR TODOS LOS CONTRATOS EN UNA SOLA CONSULTA
        contratos = db_src(db_src.contratos.id_contrato.belongs(lista_id_contratos)).select()
        contratos_dict = {c.id_contrato: c for c in contratos}
        
        # 2. BUSCAR TODAS LAS ADICIONES EN UNA SOLA CONSULTA
        adiciones = db_src(db_src.adiciones.id_contrato.belongs(lista_id_contratos)).select()
        
        # 3. BUSCAR TODAS LAS EJECUCIONES EN UNA SOLA CONSULTA
        ejecuciones = db_src(db_src.ejecuciones.id_contrato.belongs(lista_id_contratos)).select()
        
        # Verificar qué contratos existen en destino (una sola consulta)
        contratos_existentes = db_dst(db_dst.contratos.id_contrato.belongs(lista_id_contratos)).select()
        ids_existentes = {c.id_contrato for c in contratos_existentes}
        
        # Verificar qué adiciones existen en destino
        adiciones_existentes = db_dst(db_dst.adiciones.id_contrato.belongs(lista_id_contratos)).select()
        ids_adiciones_existentes = {a.id_adicion for a in adiciones_existentes}
        
        print(f"Contratos encontrados en origen: {len(contratos_dict)}/{len(lista_id_contratos)}")
        print(f"Adiciones encontradas: {len(adiciones)}")
        print(f"Ejecuciones encontradas: {len(ejecuciones)}\n")
        
        # 4. PROCESAR CADA CONTRATO
        for id_contrato in lista_id_contratos:
            try:
                if id_contrato not in contratos_dict:
                    print(f"⚠️  Contrato {id_contrato}: No encontrado")
                    estadisticas['no_encontrados'].append(id_contrato)
                    estadisticas['fallidos'] += 1
                    continue
                
                contrato = contratos_dict[id_contrato]
                contrato_data = {k: v for k, v in contrato.as_dict().items() if k != 'id'}
                
                # Insertar o actualizar contrato
                if id_contrato in ids_existentes:
                    db_dst(db_dst.contratos.id_contrato == id_contrato).update(**contrato_data)
                    estadisticas['contratos_actualizados'] += 1
                    accion = "actualizado"
                else:
                    db_dst.contratos.insert(**contrato_data)
                    estadisticas['contratos_copiados'] += 1
                    accion = "copiado"
                
                print(f"✓ Contrato {id_contrato}: {accion}")
                estadisticas['exitosos'] += 1
                
            except Exception as e:
                print(f"❌ Error en contrato {id_contrato}: {str(e)}")
                estadisticas['fallidos'] += 1
        
        # 5. COPIAR TODAS LAS ADICIONES
        print(f"\nCopiando adiciones...")
        for adicion in adiciones:
            if adicion.id_adicion not in ids_adiciones_existentes:
                adicion_data = {k: v for k, v in adicion.as_dict().items() if k != 'id'}
                db_dst.adiciones.insert(**adicion_data)
                estadisticas['adiciones_copiadas'] += 1
        
        print(f"  → {estadisticas['adiciones_copiadas']} adiciones copiadas")
        
        # 6. COPIAR TODAS LAS EJECUCIONES (sin verificar duplicados por rendimiento)
        print(f"Copiando ejecuciones...")
        for ejecucion in ejecuciones:
            ejecucion_data = {k: v for k, v in ejecucion.as_dict().items() if k != 'id'}
            db_dst.ejecuciones.insert(**ejecucion_data)
            estadisticas['ejecuciones_copiadas'] += 1
        
        print(f"  → {estadisticas['ejecuciones_copiadas']} ejecuciones copiadas")
        
        # Commit de los cambios
        db_dst.commit()
        
        # Resumen final
        print(f"\n{'='*80}")
        print(f"RESUMEN DE LA OPERACIÓN")
        print(f"{'='*80}")
        print(f"Total de contratos procesados: {estadisticas['total']}")
        print(f"  ✓ Exitosos: {estadisticas['exitosos']}")
        print(f"  ✗ Fallidos: {estadisticas['fallidos']}")
        print(f"\nDetalles:")
        print(f"  - Contratos nuevos copiados: {estadisticas['contratos_copiados']}")
        print(f"  - Contratos actualizados: {estadisticas['contratos_actualizados']}")
        print(f"  - Adiciones copiadas: {estadisticas['adiciones_copiadas']}")
        print(f"  - Ejecuciones copiadas: {estadisticas['ejecuciones_copiadas']}")
        
        if estadisticas['no_encontrados']:
            print(f"\nContratos no encontrados: {', '.join(estadisticas['no_encontrados'])}")
        
        print(f"{'='*80}\n")
        
        return estadisticas
        
    except Exception as e:
        print(f"\n❌ Error general durante la copia: {str(e)}")
        db_dst.rollback()
        return estadisticas
        
    finally:
        pass



def copiar_personas_multiples(lista_documentos, db_origen='contratos.db', db_destino='contratos_copia.db'):
    """
    Copia múltiples personas/entidades y sus sanciones en una sola operación optimizada
    
    Args:
        lista_documentos: Lista de documentos a copiar
        db_origen: Nombre del archivo de base de datos origen
        db_destino: Nombre del archivo de base de datos destino
    
    Returns:
        Dict con estadísticas de la operación
    """
    if not lista_documentos:
        print("⚠️  La lista de documentos está vacía")
        return {'exitosos': 0, 'fallidos': 0, 'total': 0}
    
    # Conectar a ambas bases de datos
    db_src = db_noclean
    db_dst = db
    

    
    estadisticas = {
        'exitosos': 0,
        'fallidos': 0,
        'total': len(lista_documentos),
        'personas_copiadas': 0,
        'personas_actualizadas': 0,
        'personas_no_encontradas': 0,
        'sanciones_copiadas': 0,
        'documentos_no_encontrados': []
    }
    
    try:
        print(f"\n{'='*80}")
        print(f"COPIANDO {len(lista_documentos)} PERSONAS/ENTIDADES Y SUS SANCIONES")
        print(f"{'='*80}\n")
        
        # 1. BUSCAR TODAS LAS PERSONAS EN UNA SOLA CONSULTA
        personas = db_src(db_src.entidades_personas.documento.belongs(lista_documentos)).select()
        personas_dict = {p.documento: p for p in personas}
        
        # 2. BUSCAR TODAS LAS SANCIONES EN UNA SOLA CONSULTA
        sanciones = db_src(db_src.sancionados.documento.belongs(lista_documentos)).select()
        
        # 3. Verificar qué personas existen en destino (una sola consulta)
        personas_existentes = db_dst(db_dst.entidades_personas.documento.belongs(lista_documentos)).select()
        docs_existentes = {p.documento for p in personas_existentes}
        
        # 4. Verificar qué sanciones existen en destino
        sanciones_existentes = db_dst(db_dst.sancionados.documento.belongs(lista_documentos)).select()
        # Crear un identificador único para cada sanción
        sanciones_existentes_ids = {
            (s.documento, s.tipo_inhabilitacion, s.numero_resolucion) 
            for s in sanciones_existentes
        }
        
        print(f"Personas encontradas en origen: {len(personas_dict)}/{len(lista_documentos)}")
        print(f"Sanciones encontradas: {len(sanciones)}")
        print(f"Personas ya existentes en destino: {len(docs_existentes)}\n")
        
        # 5. PROCESAR CADA PERSONA
        for documento in lista_documentos:
            try:
                if documento not in personas_dict:
                    # Persona no encontrada en origen, pero puede tener sanciones
                    estadisticas['documentos_no_encontrados'].append(documento)
                    estadisticas['personas_no_encontradas'] += 1
                    continue
                
                persona = personas_dict[documento]
                persona_data = {k: v for k, v in persona.as_dict().items() if k != 'id'}
                
                # Insertar o actualizar persona
                if documento in docs_existentes:
                    db_dst(db_dst.entidades_personas.documento == documento).update(**persona_data)
                    estadisticas['personas_actualizadas'] += 1
                    accion = "actualizada"
                else:
                    db_dst.entidades_personas.insert(**persona_data)
                    estadisticas['personas_copiadas'] += 1
                    accion = "copiada"
                
                nombre = persona.nombre[:40] if persona.nombre else "Sin nombre"
                print(f"✓ {documento} ({nombre}...): {accion}")
                estadisticas['exitosos'] += 1
                
            except Exception as e:
                print(f"❌ Error en documento {documento}: {str(e)}")
                estadisticas['fallidos'] += 1
        
        # 6. COPIAR TODAS LAS SANCIONES
        print(f"\nCopiando sanciones...")
        for sancion in sanciones:
            sancion_id = (sancion.documento, sancion.tipo_inhabilitacion, sancion.numero_resolucion)
            
            if sancion_id not in sanciones_existentes_ids:
                sancion_data = {k: v for k, v in sancion.as_dict().items() if k != 'id'}
                db_dst.sancionados.insert(**sancion_data)
                estadisticas['sanciones_copiadas'] += 1
        
        print(f"  → {estadisticas['sanciones_copiadas']} sanciones copiadas")
        
        # Commit de los cambios
        db_dst.commit()
        
        # Resumen final
        print(f"\n{'='*80}")
        print(f"RESUMEN DE LA OPERACIÓN")
        print(f"{'='*80}")
        print(f"Total de documentos procesados: {estadisticas['total']}")
        print(f"  ✓ Exitosos: {estadisticas['exitosos']}")
        print(f"  ✗ Fallidos: {estadisticas['fallidos']}")
        print(f"  ⚠ No encontrados: {estadisticas['personas_no_encontradas']}")
        print(f"\nDetalles:")
        print(f"  - Personas nuevas copiadas: {estadisticas['personas_copiadas']}")
        print(f"  - Personas actualizadas: {estadisticas['personas_actualizadas']}")
        print(f"  - Sanciones copiadas: {estadisticas['sanciones_copiadas']}")
        
        if estadisticas['documentos_no_encontrados']:
            print(f"\nDocumentos no encontrados ({len(estadisticas['documentos_no_encontrados'])}):")
            # Mostrar solo los primeros 20
            docs_mostrar = estadisticas['documentos_no_encontrados'][:20]
            print(f"  {', '.join(docs_mostrar)}")
            if len(estadisticas['documentos_no_encontrados']) > 20:
                print(f"  ... y {len(estadisticas['documentos_no_encontrados']) - 20} más")
        
        print(f"{'='*80}\n")
        
        return estadisticas
        
    except Exception as e:
        print(f"\n❌ Error general durante la copia: {str(e)}")
        db_dst.rollback()
        return estadisticas
        
    finally:
        pass


def obtener_contratos_por_entidad_optimizado(limite=None, ordenar_desc=True):
    """
    Versión optimizada usando SQL nativo para mejor rendimiento
    
    Args:
        limite: Número máximo de resultados a mostrar (None para todos)
        ordenar_desc: True para ordenar de mayor a menor cantidad de contratos
    
    Returns:
        Lista de diccionarios con nit_entidad, nombre y cantidad_contratos
    """

    
    try:
        # Construir la consulta SQL para contar contratos por NIT
        orden = "DESC" if ordenar_desc else "ASC"
        limit_clause = f"LIMIT {limite}" if limite else ""
        
        query = f"""
        SELECT 
            c.nit_entidad,
            e.nombre,
            COUNT(*) as cantidad_contratos
        FROM contratos c
        LEFT JOIN entidades_personas e ON c.nit_entidad = e.documento
        WHERE c.nit_entidad IS NOT NULL AND c.nit_entidad != ''
        GROUP BY c.nit_entidad, e.nombre
        ORDER BY cantidad_contratos {orden}
        {limit_clause}
        """
        
        resultados = db.executesql(query, as_dict=True)
        
        # Manejar casos donde el nombre es None
        for resultado in resultados:
            if not resultado['nombre']:
                resultado['nombre'] = "Sin nombre registrado"
        
        return resultados
        
    finally:
        pass

def obtener_contratos_con_filtros(nit_entidad, estado=None, tipo_contrato=None, 
                                fecha_desde=None, fecha_hasta=None, 
                                valor_minimo=None, valor_maximo=None,db_origen='contratos.db',):
    """
    Obtiene contratos de una entidad con filtros adicionales
    
    Args:
        nit_entidad: NIT de la entidad
        estado: Filtrar por estado (ej: 'Liquidado', 'Celebrado')
        tipo_contrato: Filtrar por tipo (ej: 'Prestación de servicios')
        fecha_desde: Filtrar contratos desde esta fecha
        fecha_hasta: Filtrar contratos hasta esta fecha
        valor_minimo: Valor mínimo del contrato
        valor_maximo: Valor máximo del contrato
    
    Returns:
        Lista de diccionarios con la información de los contratos
    """
    db= db_noclean

    
    # Definir tablas en ambas bases de datos

    
    try:
        # Construir la consulta base
        query = (db.contratos.nit_entidad == nit_entidad)
        
        # Aplicar filtros adicionales
        if estado:
            query &= (db.contratos.estado_contrato == estado)
        
        if tipo_contrato:
            query &= (db.contratos.tipo_contrato == tipo_contrato)
        
        if fecha_desde:
            query &= (db.contratos.fecha_firma >= fecha_desde)
        
        if fecha_hasta:
            query &= (db.contratos.fecha_firma <= fecha_hasta)
        
        if valor_minimo:
            query &= (db.contratos.valor_contrato >= valor_minimo)
        
        if valor_maximo:
            query &= (db.contratos.valor_contrato <= valor_maximo)
        
        # Ejecutar consulta
        contratos = db(query).select(orderby=~db.contratos.fecha_firma)
        
        # Obtener nombre de la entidad
        entidad = db(db.entidades_personas.documento == nit_entidad).select().first()
        nombre_entidad = entidad.nombre if entidad else "Desconocida"
        
        lista_contratos = [contrato.as_dict()["id_contrato"] for contrato in contratos]
        
        return  lista_contratos#{
        #     'nit_entidad': nit_entidad,
        #     'nombre_entidad': nombre_entidad,
        #     'filtros_aplicados': {
        #         'estado': estado,
        #         'tipo_contrato': tipo_contrato,
        #         'fecha_desde': fecha_desde,
        #         'fecha_hasta': fecha_hasta,
        #         'valor_minimo': valor_minimo,
        #         'valor_maximo': valor_maximo
        #     },
        #     'total_contratos': len(lista_contratos),
            
        #}
        
    finally:
        pass

def obtener_todos_los_documentos_optimizado():
    """
    Versión optimizada usando SQL nativo para mejor rendimiento
    
    Returns:
        Lista de documentos únicos
    """


    
    # Definir tablas en ambas bases de datos

    
    try:
        # Query SQL que obtiene todos los documentos únicos en una sola consulta
        query = """
        SELECT DISTINCT documento FROM (
            SELECT documento_proveedor as documento FROM contratos
            WHERE documento_proveedor IS NOT NULL AND documento_proveedor != ''
            UNION
            SELECT num_doc_ordenador_gasto as documento FROM contratos
            WHERE num_doc_ordenador_gasto IS NOT NULL AND num_doc_ordenador_gasto != ''
            UNION
            SELECT num_doc_supervisor as documento FROM contratos
            WHERE num_doc_supervisor IS NOT NULL AND num_doc_supervisor != ''
            UNION
            SELECT num_doc_ordenador_pago as documento FROM contratos
            WHERE num_doc_ordenador_pago IS NOT NULL AND num_doc_ordenador_pago != ''
        ) as todos_documentos
        ORDER BY documento
        """
        
        resultados = db.executesql(query)
        
        # Convertir lista de tuplas a lista simple
        lista_documentos = [resultado[0] for resultado in resultados]
        
        return lista_documentos
        
    finally:
        pass

def obtener_entidades_relacionadas(lista_documentos):
    """
    Obtiene lista única de NITs de entidades que tienen relación con los documentos dados
    (como proveedor, ordenador de gasto, supervisor u ordenador de pago)
    
    Args:
        lista_documentos: Lista de documentos de personas/empresas
    
    Returns:
        Lista de NITs únicos de entidades relacionadas
    """

    
    if not lista_documentos:
        return []
    
    try:
        # Construir placeholders para la consulta
        placeholders = ','.join(['?' for _ in lista_documentos])
        
        query = f"""
        SELECT DISTINCT nit_entidad
        FROM contratos
        WHERE nit_entidad IS NOT NULL 
        AND nit_entidad != ''
        AND (
            documento_proveedor IN ({placeholders})
            OR num_doc_ordenador_gasto IN ({placeholders})
            OR num_doc_supervisor IN ({placeholders})
            OR num_doc_ordenador_pago IN ({placeholders})
        )
        ORDER BY nit_entidad
        """
        
        # Ejecutar con los documentos repetidos 4 veces (uno por cada campo)
        params = lista_documentos * 4
        resultados = db.executesql(query, placeholders=params)
        
        # Convertir lista de tuplas a lista simple
        entidades = [resultado[0] for resultado in resultados]
        
        return entidades
        
    finally:
        pass

# Ejemplo de uso
if __name__ == "__main__":
    # Copiar un contrato específico
    #id_contrato_a_copiar = "12345"  # Cambia esto por el ID real
    #copiar_contrato_y_relacionados(id_contrato_a_copiar)
    
    # Copiar una persona/entidad específica
    #documento_a_copiar = "123456789"  # Cambia esto por el documento real
    #copiar_persona_y_sanciones(documento_a_copiar)
    # O copiar múltiples contratos
    print(1)
    results=[]
    Entidades = ["899999118" ,#CAJA DE RETIRO DE LAS FUERZAS MILITARES
    
    "899999073" ,#CAJA DE SUELDOS DE RETIRO DE LA POLICIA NACIONAL
    "899999074" ,#CAJA DE LA VIVIENDA POPULAR//
    "830025267" ,# FONDO NACIONAL AMBIENTAL - ANLA
    "901140004",#JEP
    "899999162" , #AGENCIA LOGISTICA DE LAS FUERZAS MILITARES
    "899999294" ,# SERVICIO GEOLOGICO COLOMBIANO**
    "899999007",#SUPERINTENDENCIA DE NOTARIADO Y REGISTRO
    "860509339" #, CONSEJO PROFESIONAL DE INGENIERIA QUIMICA DE
    "805018833" #,UNIDAD EJECUTORA DE SANEAMIENTO DEL VALLE DEL CAUC
    "890204851", "899999073", "899999074", "901140004", "899999162","901540992","899999296","891380046","8913800897","891380089","899999034","890399011"]

    for entidad in Entidades:
        result=obtener_contratos_con_filtros(entidad)
        results.extend(result)
    print(len(results))
    results=list(set(results))
    print(len(results))
    usados=dividir_en_lotes(results,tamano_lote=5000)
    for usado in usados:
        copiar_contratos_multiples(usado,  db_destino='contratos_copia.db')
    proveedores=obtener_todos_los_documentos_optimizado()
    proveedoresusados=dividir_en_lotes(proveedores,tamano_lote=5000)
    nuevasEntidades=[]
    for lista_documentos in proveedoresusados:
        copiar_personas_multiples(lista_documentos, db_origen='contratos.db', db_destino='contratos_copia.db')
        nuevasEntidades.extend(obtener_entidades_relacionadas(lista_documentos))
    Entidadesfinales=set(nuevasEntidades)-set(Entidades)
    results=[]
    for entidad in Entidadesfinales:
        result=obtener_contratos_con_filtros(entidad)
        results.extend(result)
    usados=dividir_en_lotes(results)
    for usado in usados:
        copiar_contratos_multiples(usado,  db_destino='contratos_copia.db')
    proveedores=obtener_todos_los_documentos_optimizado()
    proveedoresusados=dividir_en_lotes(proveedores,tamano_lote=5000)
    nuevasEntidades=[]
    for lista_documentos in proveedoresusados:
        copiar_personas_multiples(lista_documentos, db_origen='contratos.db', db_destino='contratos_copia.db')
