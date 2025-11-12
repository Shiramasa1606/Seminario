"""
Módulo de Inserción de Material Educativo - Gestión de Unidades y RAPs en Neo4J

Este módulo se encarga de la gestión de la estructura curricular del sistema educativo,
incluyendo unidades de aprendizaje y sus respectivos RAPs (Recursos de Aprendizaje).
Procesa la estructura de carpetas del sistema de archivos y la refleja en la base de datos Neo4J.

Funciones principales:
    - procesar_unidades_y_raps: Proceso principal de inserción masiva
    - insertar_unidad: Inserción individual de unidades
    - insertar_rap: Inserción individual de RAPs con relaciones
    - validar_estructura_carpetas: Validación de estructura de directorios
    - limpiar_unidades_y_raps: Limpieza de datos existentes

Características:
    - Procesamiento recursivo de estructura de carpetas
    - Validación robusta de paths y archivos
    - Prevención de duplicados usando MERGE
    - Manejo graceful de errores por unidad
    - Logging detallado del proceso

Estructura de carpetas esperada:
    base_path/
    ├── Unidad_01/
    │   └── RAP/
    │       ├── RAP_1.pdf
    │       └── RAP_2.pdf
    ├── Unidad_02/
    │   └── RAP/
    │       └── RAP_3.pdf
"""

from pathlib import Path
from typing import List, Optional
from neo4j import Driver, ManagedTransaction
import logging

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)


# ==========================
# Insertar Unidad
# ==========================

def insertar_unidad(tx: ManagedTransaction, nombre_unidad: str) -> None:
    """
    Inserta una Unidad en Neo4J si no existe, utilizando MERGE para evitar duplicados.
    
    Las unidades representan módulos o secciones del curso y actúan como contenedores
    lógicos para los RAPs. Cada unidad es un nodo único identificado por su nombre.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la operación
        nombre_unidad: Nombre único de la unidad a insertar/validar
        
    Raises:
        Exception: Si la operación de base de datos falla por problemas de conexión
                   o restricciones de integridad
                   
    Example:
        >>> with driver.session() as session:
        ...     session.execute_write(insertar_unidad, "Unidad_01")
        ✅ Unidad insertada/validada: Unidad_01
        
    Note:
        - Usa MERGE para operación idempotente
        - El nombre de la unidad debe ser único
        - No crea relaciones en esta función
    """
    try:
        result = tx.run(
            """
            MERGE (u:Unidad {nombre: $nombre})
            RETURN u.nombre as nombre
            """,
            nombre=nombre_unidad
        )
        single_record = result.single()
        if single_record is not None:
            nombre_insertado: str = single_record["nombre"]
            logger.info(f"✅ Unidad insertada/validada: {nombre_insertado}")
        else:
            logger.warning(f"⚠️ No se pudo verificar la inserción de la unidad: {nombre_unidad}")
    except Exception as e:
        logger.error(f"❌ Error insertando unidad {nombre_unidad}: {e}")
        raise


# ==========================
# Insertar RAP
# ==========================

def insertar_rap(tx: ManagedTransaction, nombre_unidad: str, nombre_rap: str) -> None:
    """
    Inserta un RAP en Neo4J y lo asocia con su Unidad padre mediante relación.
    
    Los RAPs (Recursos de Aprendizaje) son materiales educativos específicos que
    pertenecen a una unidad. Esta función crea tanto el nodo RAP como la relación
    con su unidad correspondiente.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la operación
        nombre_unidad: Nombre de la unidad padre a la que pertenece el RAP
        nombre_rap: Nombre único del RAP a insertar/validar
        
    Raises:
        Exception: Si la unidad padre no existe o hay problemas de conexión
        
    Example:
        >>> with driver.session() as session:
        ...     session.execute_write(insertar_rap, "Unidad_01", "Introduccion_Programacion")
        📘 RAP insertado/validado en Unidad_01: Introduccion_Programacion
        
    Note:
        - La unidad debe existir previamente
        - Relación: (Unidad)-[:TIENE_RAP]->(RAP)
        - Operación atómica: crea RAP y relación simultáneamente
    """
    try:
        result = tx.run(
            """
            MATCH (u:Unidad {nombre: $unidad})
            MERGE (r:RAP {nombre: $rap})
            MERGE (u)-[:TIENE_RAP]->(r)
            RETURN u.nombre as unidad, r.nombre as rap
            """,
            unidad=nombre_unidad,
            rap=nombre_rap
        )
        record = result.single()
        if record:
            unidad_nombre: str = record["unidad"]
            rap_nombre: str = record["rap"]
            logger.info(f"   📘 RAP insertado/validado en {unidad_nombre}: {rap_nombre}")
        else:
            logger.warning(f"⚠️ No se pudo insertar RAP {nombre_rap} en unidad {nombre_unidad}")
    except Exception as e:
        logger.error(f"❌ Error insertando RAP {nombre_rap} en unidad {nombre_unidad}: {e}")
        raise


# ==========================
# Validar estructura de carpetas
# ==========================

def validar_estructura_carpetas(base_path: Path) -> List[Path]:
    """
    Valida la estructura de directorios y retorna las carpetas de Unidad válidas.
    
    Realiza verificaciones exhaustivas sobre la ruta base y busca carpetas que
    sigan el patrón de nomenclatura esperado para unidades educativas.
    
    Args:
        base_path: Ruta base del sistema de archivos a validar
        
    Returns:
        List[Path]: Lista de paths de carpetas de Unidad válidas y accesibles
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es un directorio
        ValueError: Si no se encuentran carpetas de Unidad en la ruta base
        
    Example:
        >>> unidades = validar_estructura_carpetas(Path("/ruta/materiales"))
        >>> print(f"Encontradas {len(unidades)} unidades")
        Encontradas 5 unidades
        
    Note:
        - Busca carpetas que comiencen con "unidad" (case-insensitive)
        - Solo incluye directorios, ignora archivos
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
    
    if not carpetas_unidad:
        raise ValueError(f"No se encontraron carpetas de Unidad en: {base_path}")
    
    return carpetas_unidad


def encontrar_carpeta_rap(carpeta_unidad: Path) -> Optional[Path]:
    """
    Encuentra la carpeta RAP dentro de una unidad específica.
    
    Busca recursivamente la subcarpeta "RAP" dentro del directorio de la unidad.
    Esta función implementa la estructura esperada de carpetas del sistema.
    
    Args:
        carpeta_unidad: Path del directorio de la unidad a escanear
        
    Returns:
        Optional[Path]: Path de la carpeta RAP si existe, None en caso contrario
        
    Example:
        >>> carpeta = encontrar_carpeta_rap(Path("/ruta/Unidad_01"))
        >>> if carpeta:
        ...     print(f"RAP encontrado en: {carpeta}")
        RAP encontrado en: /ruta/Unidad_01/RAP
        
    Note:
        - Estructura esperada: Unidad_XX/RAP/
        - Solo busca en el primer nivel de la unidad
        - Retorna None si no existe la carpeta RAP
    """
    rap_folder = carpeta_unidad / "RAP"
    if rap_folder.exists() and rap_folder.is_dir():
        return rap_folder
    return None


def obtener_archivos_rap(carpeta_rap: Path) -> List[str]:
    """
    Obtiene los nombres de los archivos RAP (sin extensión) de una carpeta.
    
    Escanea el directorio RAP y extrae los nombres de todos los archivos válidos,
    excluyendo archivos ocultos y conservando solo el nombre base sin extensión.
    
    Args:
        carpeta_rap: Path del directorio RAP a escanear
        
    Returns:
        List[str]: Lista de nombres de archivos RAP sin extensión
        
    Example:
        >>> archivos = obtener_archivos_rap(Path("/ruta/Unidad_01/RAP"))
        >>> print(archivos)
        ['RAP_1', 'RAP_2', 'Guia_Estudio']
        
    Note:
        - Excluye archivos que comiencen con '.' (ocultos)
        - Usa .stem para remover extensiones de archivo
        - Incluye todos los tipos de archivo no ocultos
    """
    return [
        archivo.stem for archivo in carpeta_rap.iterdir() 
        if archivo.is_file() and not archivo.name.startswith('.')
    ]


# ==========================
# Procesar Unidades y RAPs
# ==========================

def procesar_unidades_y_raps(driver: Driver, base_path: Path) -> None:
    """
    Procesa todas las carpetas de Unidad y sus RAPs, insertándolos en Neo4J.
    
    Esta es la función principal del módulo que orquesta todo el proceso:
    1. 🔍 Valida la estructura de carpetas base
    2. 📁 Procesa cada unidad encontrada
    3. 📚 Inserta unidades en la base de datos
    4. 📘 Busca y procesa RAPs dentro de cada unidad
    5. 📊 Genera reporte final del proceso
    
    Args:
        driver: Driver de conexión a Neo4J para ejecutar las operaciones
        base_path: Ruta base donde se encuentran las carpetas de unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no es válida o no existe
        ValueError: Si no se encuentran unidades para procesar
        Exception: Para errores específicos durante el procesamiento de unidades
        
    Example:
        >>> driver = obtener_driver()
        >>> procesar_unidades_y_raps(driver, Path("/ruta/materiales"))
        🔍 Iniciando procesamiento de unidades en: /ruta/materiales
        ✅ Unidad insertada/validada: Unidad_01
        📘 RAP insertado/validado en Unidad_01: Introduccion
        ...
        ✅ Procesamiento completado: 5 unidades, 23 RAPs procesados
        
    Note:
        - Proceso continuo: errores en una unidad no detienen el proceso completo
        - Operaciones atómicas por unidad
        - Logging detallado de progreso y errores
    """
    logger.info(f"🔍 Iniciando procesamiento de unidades en: {base_path}")
    
    try:
        carpetas_unidad = validar_estructura_carpetas(base_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"❌ Error validando estructura: {e}")
        raise

    unidades_procesadas = 0
    raps_procesados = 0

    with driver.session() as session:
        for carpeta_unidad in carpetas_unidad:
            try:
                # Insertar la Unidad
                session.execute_write(insertar_unidad, carpeta_unidad.name)
                unidades_procesadas += 1

                # Buscar y procesar carpeta RAP
                carpeta_rap = encontrar_carpeta_rap(carpeta_unidad)
                if carpeta_rap:
                    archivos_rap = obtener_archivos_rap(carpeta_rap)
                    for nombre_rap in archivos_rap:
                        session.execute_write(insertar_rap, carpeta_unidad.name, nombre_rap)
                        raps_procesados += 1
                else:
                    logger.warning(f"⚠️ Carpeta RAP no encontrada en {carpeta_unidad.name}")
                    
            except Exception as e:
                logger.error(f"❌ Error procesando unidad {carpeta_unidad.name}: {e}")
                # Continuar con la siguiente unidad en lugar de fallar completamente
                continue

    logger.info(f"✅ Procesamiento completado: {unidades_procesadas} unidades, {raps_procesados} RAPs procesados")


# ==========================
# Función de utilidad para limpiar datos (opcional)
# ==========================

def limpiar_unidades_y_raps(driver: Driver) -> None:
    """
    Limpia todas las unidades y RAPs de la base de datos Neo4J.
    
    Función de utilidad diseñada para entornos de testing o para reiniciar
    completamente la estructura curricular. Elimina todos los nodos Unidad
    y RAP junto con sus relaciones.
    
    Args:
        driver: Driver de conexión a Neo4J para ejecutar la limpieza
        
    Raises:
        Exception: Si la operación de limpieza falla por problemas de conexión
                   o permisos insuficientes
                   
    Example:
        >>> driver = obtener_driver()
        >>> limpiar_unidades_y_raps(driver)
        🗑️ Unidades y RAPs eliminados: 45 nodos
        
    Warning:
        - Operación destructiva e irreversible
        - Elimina TODAS las unidades y RAPs existentes
        - Útil solo para testing o reset completo
    """
    with driver.session() as session:
        try:
            result = session.run(
                """
                MATCH (u:Unidad)
                DETACH DELETE u
                """
            )
            summary = result.consume()
            logger.info(f"🗑️ Unidades y RAPs eliminados: {summary.counters.nodes_deleted} nodos")
        except Exception as e:
            logger.error(f"❌ Error limpiando unidades y RAPs: {e}")
            raise