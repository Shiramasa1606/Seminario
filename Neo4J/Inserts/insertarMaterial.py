from pathlib import Path
from typing import List, Optional
from neo4j import Driver, ManagedTransaction
import logging

# Setup logging
logger = logging.getLogger(__name__)

# ==========================
# Insertar Unidad
# ==========================
def insertar_unidad(tx: ManagedTransaction, nombre_unidad: str) -> None:
    """
    Inserta una Unidad en Neo4j si no existe, utilizando MERGE.
    
    Args:
        tx: Transacción de Neo4j
        nombre_unidad: Nombre de la unidad a insertar
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
    Inserta un RAP en Neo4j y lo asocia con su Unidad.
    
    Args:
        tx: Transacción de Neo4j
        nombre_unidad: Nombre de la unidad padre
        nombre_rap: Nombre del RAP a insertar
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
    Valida y retorna las carpetas de Unidad válidas.
    
    Args:
        base_path: Ruta base a validar
        
    Returns:
        Lista de paths de carpetas de Unidad válidas
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es directorio
        ValueError: Si no se encuentran carpetas de Unidad
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
    Encuentra la carpeta RAP dentro de una unidad.
    
    Args:
        carpeta_unidad: Path de la carpeta de unidad
        
    Returns:
        Path de la carpeta RAP o None si no existe
    """
    rap_folder = carpeta_unidad / "RAP"
    if rap_folder.exists() and rap_folder.is_dir():
        return rap_folder
    return None


def obtener_archivos_rap(carpeta_rap: Path) -> List[str]:
    """
    Obtiene los nombres de los archivos RAP (sin extensión).
    
    Args:
        carpeta_rap: Path de la carpeta RAP
        
    Returns:
        Lista de nombres de archivos RAP
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
    Procesa todas las carpetas que comienzan con 'Unidad' en la ruta base.
    Inserta las Unidades y sus respectivos RAPs en Neo4j.
    
    Args:
        driver: Driver de Neo4j
        base_path: Ruta base donde buscar las unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no es válida
        ValueError: Si no se encuentran unidades para procesar
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
    Limpia todas las unidades y RAPs de la base de datos.
    Útil para testing o reinicios.
    
    Args:
        driver: Driver de Neo4j
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