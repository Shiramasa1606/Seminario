from pathlib import Path
from typing import Tuple, List
from neo4j import Driver, ManagedTransaction
import logging

# Setup logging
logger = logging.getLogger(__name__)

# ==========================
# Función: Validar existencia de Unidad y RAP
# ==========================
def relacionar_unidad_rap(tx: ManagedTransaction, unidad: str, rap: str) -> Tuple[bool, bool]:
    """
    Valida que existan los nodos (:Unidad {nombre: unidad}) y (:RAP {nombre: rap}).
    
    Args:
        tx: Transacción de Neo4j
        unidad: Nombre de la unidad a validar
        rap: Nombre del RAP a validar
        
    Returns:
        Tupla con (unidad_existe, rap_existe)
        
    Raises:
        Exception: Si hay error en la consulta a la base de datos
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
    Encuentra todas las carpetas de unidad en la ruta base.
    
    Args:
        base_path: Ruta base donde buscar unidades
        
    Returns:
        Lista de paths de carpetas de unidad
        
    Raises:
        FileNotFoundError: Si la ruta base no existe
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
    Encuentra todos los archivos PDF en una carpeta RAP.
    
    Args:
        carpeta_rap: Carpeta RAP donde buscar archivos PDF
        
    Returns:
        Lista de paths de archivos PDF
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
    Procesa una unidad individual, validando sus RAPs.
    
    Args:
        driver: Driver de Neo4j
        unidad_dir: Directorio de la unidad
        unidad_nombre: Nombre de la unidad
        
    Returns:
        Tupla con (relaciones_validas, raps_no_existentes, unidades_no_existentes, raps_omitidos)
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
    Recorre todas las carpetas 'Unidad X' dentro de base_path,
    entra a 'RAP' y valida cada PDF con su Unidad en Neo4j.
    
    Args:
        driver: Driver de Neo4j
        base_path: Ruta base donde buscar las unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es directorio
        ValueError: Si no se encuentran unidades para procesar
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

    # Reporte final
    logger.info("\n" + "="*50)
    logger.info("📊 RESUMEN DE PROCESAMIENTO")
    logger.info("="*50)
    logger.info(f"🗂️  Unidades procesadas: {unidades_procesadas}")
    logger.info(f"✅  Relaciones validadas: {total_relaciones_validas}")
    logger.info(f"❌  Unidades no encontradas en BD: {total_unidades_no_existentes}")
    logger.info(f"❌  RAPs no encontrados en BD: {total_raps_no_existentes}")
    logger.info(f"⏭️  RAPs omitidos: {total_raps_omitidos}")
    
    # Advertencias si hay problemas
    if total_unidades_no_existentes > 0:
        logger.warning(f"⚠️  Se encontraron {total_unidades_no_existentes} unidades que no existen en la base de datos")
    
    if total_raps_no_existentes > 0:
        logger.warning(f"⚠️  Se encontraron {total_raps_no_existentes} RAPs que no existen en la base de datos")


# ==========================
# Función adicional: Verificar estado de la base de datos
# ==========================
def verificar_estado_base_datos(driver: Driver) -> dict[str, int]:
    """
    Verifica el estado actual de unidades y RAPs en la base de datos.
    
    Args:
        driver: Driver de Neo4j
        
    Returns:
        Diccionario con conteos de unidades y RAPs
    """
    with driver.session() as session:
        try:
            # Contar unidades
            result_unidades = session.run("MATCH (u:Unidad) RETURN count(u) as total")
            record_u = result_unidades.single()
            total_unidades: int = record_u["total"] if record_u else 0
            
            # Contar RAPs
            result_raps = session.run("MATCH (r:RAP) RETURN count(r) as total")
            record_r = result_raps.single()
            total_raps: int = record_r["total"] if record_r else 0
            
            # Contar relaciones
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