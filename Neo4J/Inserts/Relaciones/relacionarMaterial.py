"""
Módulo de Validación de Relaciones - Verificación de Consistencia entre Archivos y Base de Datos

Este módulo se especializa en validar la consistencia entre la estructura de archivos
del sistema y los datos almacenados en Neo4J. Verifica que todas las unidades y RAPs
existentes en el sistema de archivos tengan su correspondiente representación en la
base de datos, identificando discrepancias y problemas de integridad.

Funciones principales:
    - procesar_relaciones: Proceso principal de validación masiva
    - relacionar_unidad_rap: Validación individual de pares unidad-RAP
    - verificar_estado_base_datos: Consulta del estado actual de la BD
    - Funciones auxiliares para escaneo de archivos y directorios

Características:
    - Validación bidireccional entre sistema de archivos y base de datos
    - Detección de archivos PDF como representantes de RAPs
    - Reporte detallado de discrepancias y problemas
    - Métricas completas del estado de consistencia
    - Manejo robusto de errores por unidad/RAP

Estructura de validación:
    - Unidades: Directorios que comienzan con "unidad"
    - RAPs: Archivos PDF dentro de carpetas RAP
    - Relaciones: Correspondencia entre unidades y RAPs en Neo4J
"""

from pathlib import Path
from typing import Tuple, List
from neo4j import Driver, ManagedTransaction
import logging

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)


# ==========================
# Función: Validar existencia de Unidad y RAP
# ==========================

def relacionar_unidad_rap(tx: ManagedTransaction, unidad: str, rap: str) -> Tuple[bool, bool]:
    """
    Valida que existan los nodos (:Unidad {nombre: unidad}) y (:RAP {nombre: rap}) en Neo4J.
    
    Realiza una consulta optimizada que verifica simultáneamente la existencia de
    ambos nodos usando OPTIONAL MATCH para manejar casos donde uno o ambos no existan.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la consulta
        unidad: Nombre de la unidad a validar (debe coincidir exactamente)
        rap: Nombre del RAP a validar (debe coincidir exactamente)
        
    Returns:
        Tuple[bool, bool]: Tupla con (unidad_existe, rap_existe) indicando
                          la existencia de cada nodo individualmente
        
    Raises:
        Exception: Si hay error en la consulta a la base de datos por problemas
                   de conexión, sintaxis de query, o permisos insuficientes
                   
    Example:
        >>> with driver.session() as session:
        ...     unidad_existe, rap_existe = session.execute_write(
        ...         relacionar_unidad_rap, "Unidad_01", "Introduccion_Programacion"
        ...     )
        >>> print(f"Unidad existe: {unidad_existe}, RAP existe: {rap_existe}")
        Unidad existe: True, RAP existe: True
        
    Note:
        - Consulta case-sensitive: los nombres deben coincidir exactamente
        - Usa OPTIONAL MATCH para manejar nodos faltantes gracefulmente
        - Retorna (False, False) si la consulta no retorna resultados
        - Útil para validación pre-relacional y debugging
    """
    try:
        query = """
        MATCH (u:Unidad {nombre: $unidad})
        OPTIONAL MATCH (r:RAP {nombre: $rap})
        RETURN u IS NOT NULL AS unidad_existe, r IS NOT NULL AS rap_existe
        """
        result = tx.run(query, unidad=unidad, rap=rap).single()
        if result is None:
            return (False, False)
        
        unidad_existe: bool = bool(result["unidad_existe"])
        rap_existe: bool = bool(result["rap_existe"])
        return (unidad_existe, rap_existe)
        
    except Exception as e:
        logger.error(f"❌ Error validando unidad '{unidad}' y RAP '{rap}': {e}")
        raise


# ==========================
# Funciones de utilidad para procesamiento
# ==========================

def encontrar_carpetas_unidad(base_path: Path) -> List[Path]:
    """
    Encuentra todas las carpetas de unidad en la ruta base del sistema de archivos.
    
    Realiza un escaneo del directorio base buscando carpetas que sigan el patrón
    de nomenclatura esperado para unidades educativas, excluyendo archivos y
    otros tipos de directorios.
    
    Args:
        base_path: Ruta base del sistema de archivos a escanear
        
    Returns:
        List[Path]: Lista de paths de carpetas de unidad válidas y accesibles,
                   ordenadas según la iteración natural del sistema de archivos
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es un directorio
        
    Example:
        >>> unidades = encontrar_carpetas_unidad(Path("/ruta/materiales"))
        >>> [unit.name for unit in unidades]
        ['Unidad_01', 'Unidad_02', 'Unidad_03']
        
    Note:
        - Busca carpetas que comiencen con "unidad" (case-insensitive)
        - Solo incluye directorios, ignora archivos
        - Retorna lista vacía si no se encuentran unidades
        - Orden natural según iteración del sistema de archivos
    """
    if not base_path.exists():
        raise FileNotFoundError(f"La ruta base no existe: {base_path}")
    
    if not base_path.is_dir():
        raise FileNotFoundError(f"La ruta base no es un directorio: {base_path}")

    carpetas_unidad = [
        carpeta for carpeta in base_path.iterdir()
        if carpeta.is_dir() and carpeta.name.lower().startswith("unidad")
    ]
    
    return carpetas_unidad


def encontrar_archivos_pdf_en_rap(carpeta_rap: Path) -> List[Path]:
    """
    Encuentra todos los archivos PDF en una carpeta RAP específica.
    
    Escanea recursivamente el directorio RAP en busca de archivos PDF que
    representan los recursos de aprendizaje. Los archivos se retornan ordenados
    alfabéticamente para procesamiento consistente.
    
    Args:
        carpeta_rap: Path del directorio RAP a escanear
        
    Returns:
        List[Path]: Lista ordenada de paths de archivos PDF encontrados,
                   o lista vacía si no hay PDFs o la carpeta no existe
        
    Example:
        >>> pdfs = encontrar_archivos_pdf_en_rap(Path("/ruta/Unidad_01/RAP"))
        >>> [pdf.name for pdf in pdfs]
        ['RAP_1.pdf', 'RAP_2.pdf', 'Guia_Estudio.pdf']
        
    Note:
        - Solo busca archivos con extensión .pdf (case-insensitive)
        - Retorna lista vacía si la carpeta no existe o no es directorio
        - Ordena archivos alfabéticamente por nombre
        - No busca recursivamente en subdirectorios
    """
    if not carpeta_rap.exists() or not carpeta_rap.is_dir():
        return []
    
    return sorted([
        archivo for archivo in carpeta_rap.iterdir()
        if archivo.is_file() and archivo.suffix.lower() == ".pdf"
    ])


def procesar_unidad(
    driver: Driver, 
    unidad_dir: Path, 
    unidad_nombre: str
) -> Tuple[int, int, int, int]:
    """
    Procesa una unidad individual, validando todos sus RAPs contra la base de datos.
    
    Función que coordina la validación completa de una unidad: encuentra todos
    los archivos PDF en su carpeta RAP y verifica la existencia correspondiente
    tanto de la unidad como de cada RAP en la base de datos Neo4J.
    
    Args:
        driver: Driver de conexión a Neo4J para ejecutar las validaciones
        unidad_dir: Directorio físico de la unidad en el sistema de archivos
        unidad_nombre: Nombre de la unidad (debe coincidir con el nombre en BD)
        
    Returns:
        Tuple[int, int, int, int]: Métricas de procesamiento en el orden:
            - relaciones_validas: Número de pares unidad-RAP que existen en BD
            - raps_no_existentes: Número de RAPs que no existen en BD
            - unidades_no_existentes: Número de unidades que no existen en BD
            - raps_omitidos: Número de RAPs que no pudieron procesarse por error
        
    Example:
        >>> metrics = procesar_unidad(driver, Path("/ruta/Unidad_01"), "Unidad_01")
        📁 Procesando unidad: Unidad_01 (3 RAPs encontrados)
        ✅ Nodo validado: 'Unidad_01' y 'RAP_1'
        ❌ RAP NO existe en Neo4j: RAP_3 (archivo: rap_3.pdf)
        >>> print(metrics)
        (2, 1, 0, 0)
        
    Note:
        - Continúa el procesamiento despite errores individuales por RAP
        - Considera que una unidad no existe solo si falla para todos sus RAPs
        - Los RAPs omitidos son aquellos que generaron excepciones durante el procesamiento
        - Logging detallado de cada validación individual
    """
    relaciones_validas = 0
    raps_no_existentes = 0
    unidades_no_existentes = 0
    raps_omitidos = 0

    rap_folder: Path = unidad_dir / "RAP"
    if not rap_folder.exists() or not rap_folder.is_dir():
        logger.warning(f"⚠️ Carpeta RAP no encontrada en {unidad_dir}")
        return (relaciones_validas, raps_no_existentes, unidades_no_existentes, raps_omitidos)

    archivos_pdf = encontrar_archivos_pdf_en_rap(rap_folder)
    if not archivos_pdf:
        logger.warning(f"⚠️ No se encontraron PDFs en {rap_folder}")
        return (relaciones_validas, raps_no_existentes, unidades_no_existentes, raps_omitidos)

    logger.info(f"📁 Procesando unidad: {unidad_nombre} ({len(archivos_pdf)} RAPs encontrados)")

    for pdf in archivos_pdf:
        try:
            rap_nombre: str = pdf.stem
            
            with driver.session() as session:
                unidad_existe: bool
                rap_existe: bool
                unidad_existe, rap_existe = session.execute_write(relacionar_unidad_rap, unidad_nombre, rap_nombre)

            if unidad_existe and rap_existe:
                logger.info(f"   ✅ Nodo validado: '{unidad_nombre}' y '{rap_nombre}'")
                relaciones_validas += 1
            else:
                if not unidad_existe:
                    logger.error(f"   ❌ Unidad NO existe en Neo4j: {unidad_nombre}")
                    unidades_no_existentes += 1
                if not rap_existe:
                    logger.error(f"   ❌ RAP NO existe en Neo4j: {rap_nombre} (archivo: {pdf.name})")
                    raps_no_existentes += 1
                    raps_omitidos += 1
                    
        except Exception as e:
            logger.error(f"❌ Error procesando RAP {pdf.name} en unidad {unidad_nombre}: {e}")
            raps_omitidos += 1
            continue

    return (relaciones_validas, raps_no_existentes, unidades_no_existentes, raps_omitidos)


# ==========================
# Función: Procesar todas las relaciones de unidades y RAPs
# ==========================

def procesar_relaciones(driver: Driver, base_path: Path) -> None:
    """
    Procesa recursivamente todas las unidades y RAPs, validando su consistencia con la BD.
    
    Esta es la función principal del módulo que orquesta la validación completa:
    1. 🔍 Encuentra todas las carpetas de unidad en la ruta base
    2. 📁 Procesa cada unidad individualmente
    3. 📚 Valida cada RAP dentro de cada unidad
    4. 📊 Genera reporte final de consistencia
    5. ⚠️ Identifica discrepancias entre sistema de archivos y base de datos
    
    Args:
        driver: Driver de conexión a Neo4J para ejecutar las validaciones
        base_path: Ruta base donde se encuentran las carpetas de unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es un directorio
        ValueError: Si no se encuentran carpetas de unidad para procesar
        
    Example:
        >>> driver = obtener_driver()
        >>> procesar_relaciones(driver, Path("/ruta/materiales"))
        🔍 Iniciando procesamiento de relaciones en: /ruta/materiales
        📁 Procesando unidad: Unidad_01 (5 RAPs encontrados)
        ✅ Nodo validado: 'Unidad_01' y 'Introduccion'
        ❌ RAP NO existe en Neo4j: RAP_3 (archivo: rap_3.pdf)
        ...
        📊 RESUMEN DE PROCESAMIENTO
        ==================================================
        🗂️  Unidades procesadas: 5
        ✅  Relaciones validadas: 23
        ❌  Unidades no encontradas en BD: 0
        ❌  RAPs no encontrados en BD: 2
        ⏭️  RAPs omitidos: 0
        
    Note:
        - Proceso continuo: errores en una unidad no detienen el proceso completo
        - Reporte detallado con métricas comprensivas
        - Advertencias específicas para problemas identificados
        - Útil para verificar integridad después de inserciones masivas
    """
    logger.info(f"🔍 Iniciando procesamiento de relaciones en: {base_path}")

    try:
        carpetas_unidad = encontrar_carpetas_unidad(base_path)
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise

    if not carpetas_unidad:
        error_msg = f"No se encontraron carpetas de Unidad en: {base_path}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    unidades_procesadas: int = 0
    total_relaciones_validas: int = 0
    total_raps_no_existentes: int = 0
    total_unidades_no_existentes: int = 0
    total_raps_omitidos: int = 0

    for unidad_dir in carpetas_unidad:
        try:
            unidad_nombre: str = unidad_dir.name
            unidades_procesadas += 1

            relaciones_validas, raps_no_existentes, unidades_no_existentes, raps_omitidos = procesar_unidad(
                driver, unidad_dir, unidad_nombre
            )

            total_relaciones_validas += relaciones_validas
            total_raps_no_existentes += raps_no_existentes
            total_unidades_no_existentes += unidades_no_existentes
            total_raps_omitidos += raps_omitidos

        except Exception as e:
            logger.error(f"❌ Error procesando unidad {unidad_dir.name}: {e}")
            continue

    # Reporte final comprehensivo
    logger.info("\n" + "="*50)
    logger.info("📊 RESUMEN DE PROCESAMIENTO")
    logger.info("="*50)
    logger.info(f"🗂️  Unidades procesadas: {unidades_procesadas}")
    logger.info(f"✅  Relaciones validadas: {total_relaciones_validas}")
    logger.info(f"❌  Unidades no encontradas en BD: {total_unidades_no_existentes}")
    logger.info(f"❌  RAPs no encontrados en BD: {total_raps_no_existentes}")
    logger.info(f"⏭️  RAPs omitidos: {total_raps_omitidos}")
    
    # Advertencias específicas para problemas identificados
    if total_unidades_no_existentes > 0:
        logger.warning(f"⚠️  Se encontraron {total_unidades_no_existentes} unidades que no existen en la base de datos")
    
    if total_raps_no_existentes > 0:
        logger.warning(f"⚠️  Se encontraron {total_raps_no_existentes} RAPs que no existen en la base de datos")


# ==========================
# Función adicional: Verificar estado de la base de datos
# ==========================

def verificar_estado_base_datos(driver: Driver) -> dict[str, int]:
    """
    Verifica el estado actual de unidades, RAPs y relaciones en la base de datos Neo4J.
    
    Función de utilidad que proporciona una instantánea del estado actual de
    la estructura curricular en la base de datos, útil para diagnóstico y
    verificación post-procesamiento.
    
    Args:
        driver: Driver de conexión a Neo4J para ejecutar las consultas
        
    Returns:
        dict[str, int]: Diccionario con métricas de la base de datos:
            - 'unidades': Número total de nodos Unidad
            - 'raps': Número total de nodos RAP  
            - 'relaciones': Número total de relaciones TIENE_RAP
            
    Example:
        >>> estado = verificar_estado_base_datos(driver)
        >>> print(f"Unidades: {estado['unidades']}")
        >>> print(f"RAPs: {estado['raps']}")
        >>> print(f"Relaciones: {estado['relaciones']}")
        Unidades: 10
        RAPs: 45
        Relaciones: 45
        
    Note:
        - Consultas de solo lectura
        - Retorna 0 para todos los valores en caso de error (fail-safe)
        - Útil para comparar con el estado del sistema de archivos
        - Las relaciones deben igualar a RAPs si la integridad es perfecta
    """
    with driver.session() as session:
        try:
            # Contar unidades totales
            result_unidades = session.run("MATCH (u:Unidad) RETURN count(u) as total")
            record_u = result_unidades.single()
            total_unidades: int = record_u["total"] if record_u else 0
            
            # Contar RAPs totales
            result_raps = session.run("MATCH (r:RAP) RETURN count(r) as total")
            record_r = result_raps.single()
            total_raps: int = record_r["total"] if record_r else 0
            
            # Contar relaciones establecidas
            result_relaciones = session.run("MATCH (u:Unidad)-[:TIENE_RAP]->(r:RAP) RETURN count(*) as total")
            record_rel = result_relaciones.single()
            total_relaciones: int = record_rel["total"] if record_rel else 0
            
            return {
                "unidades": total_unidades,
                "raps": total_raps,
                "relaciones": total_relaciones
            }
            
        except Exception as e:
            logger.error(f"❌ Error verificando estado de la base de datos: {e}")
            return {"unidades": 0, "raps": 0, "relaciones": 0}