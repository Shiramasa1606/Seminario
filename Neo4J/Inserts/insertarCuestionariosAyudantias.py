from pathlib import Path
from typing import Union, Optional, Callable
from neo4j import Driver, ManagedTransaction
import re
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Type alias para funciones de transacción
TransactionFunction = Callable[[ManagedTransaction, str, str], None]

# ==========================
# Función: limpiar nombre de archivo
# ==========================
def limpiar_nombre_archivo(nombre_archivo: str) -> str:
    """
    Limpia el nombre del archivo removiendo extensión y sufijos como '-calificaciones'.
    
    Args:
        nombre_archivo: Nombre del archivo a limpiar
        
    Returns:
        Nombre limpio sin extensión ni sufijos
    """
    try:
        nombre_raw = Path(nombre_archivo).stem  # elimina extensión si la tiene
        # quitar sufijo "-calificaciones" (insensible a mayúsc/minúsc)
        nombre_limpio = re.sub(r'(?i)\s*-calificaciones\s*$', '', nombre_raw).strip()
        return nombre_limpio
    except Exception as e:
        logger.error(f"❌ Error limpiando nombre de archivo '{nombre_archivo}': {e}")
        # Fallback: usar el nombre original sin extensión
        return Path(nombre_archivo).stem


# ==========================
# Función: insertar un Cuestionario
# ==========================
def insertar_cuestionario(tx: ManagedTransaction, unidad: str, nombre_archivo: str) -> None:
    """
    Inserta (MERGE) un nodo :Cuestionario con su relación a :Unidad.
    
    Args:
        tx: Transacción de Neo4j
        unidad: Nombre de la unidad
        nombre_archivo: Nombre del archivo (se limpiará automáticamente)
        
    Raises:
        Exception: Si hay error en la inserción
    """
    try:
        nombre_limpio = limpiar_nombre_archivo(nombre_archivo)
        
        result = tx.run(
            """
            MERGE (u:Unidad {nombre: $unidad})
            MERGE (c:Cuestionario {nombre: $nombre})
            MERGE (u)-[:TIENE_CUESTIONARIO]->(c)
            RETURN u.nombre as unidad, c.nombre as cuestionario
            """,
            unidad=unidad,
            nombre=nombre_limpio,
        )
        
        record = result.single()
        if record:
            unidad_nombre: str = record["unidad"]
            cuestionario_nombre: str = record["cuestionario"]
            logger.info(f"✅ Cuestionario insertado/validado: {cuestionario_nombre} en {unidad_nombre}")
        else:
            logger.warning(f"⚠️ No se pudo verificar la inserción del cuestionario: {nombre_limpio}")
            
    except Exception as e:
        logger.error(f"❌ Error insertando cuestionario {nombre_archivo} en unidad {unidad}: {e}")
        raise


# ==========================
# Función: insertar una Ayudantía
# ==========================
def insertar_ayudantia(tx: ManagedTransaction, unidad: str, nombre_archivo: str) -> None:
    """
    Inserta (MERGE) un nodo :Ayudantia con su relación a :Unidad.
    
    Args:
        tx: Transacción de Neo4j
        unidad: Nombre de la unidad
        nombre_archivo: Nombre del archivo (se limpiará automáticamente)
        
    Raises:
        Exception: Si hay error en la inserción
    """
    try:
        nombre_limpio = limpiar_nombre_archivo(nombre_archivo)
        
        result = tx.run(
            """
            MERGE (u:Unidad {nombre: $unidad})
            MERGE (a:Ayudantia {nombre: $nombre})
            MERGE (u)-[:TIENE_AYUDANTIA]->(a)
            RETURN u.nombre as unidad, a.nombre as ayudantia
            """,
            unidad=unidad,
            nombre=nombre_limpio,
        )
        
        record = result.single()
        if record:
            unidad_nombre: str = record["unidad"]
            ayudantia_nombre: str = record["ayudantia"]
            logger.info(f"✅ Ayudantía insertada/validada: {ayudantia_nombre} en {unidad_nombre}")
        else:
            logger.warning(f"⚠️ No se pudo verificar la inserción de la ayudantía: {nombre_limpio}")
            
    except Exception as e:
        logger.error(f"❌ Error insertando ayudantía {nombre_archivo} en unidad {unidad}: {e}")
        raise


# ==========================
# Funciones de utilidad para procesamiento de archivos
# ==========================
def encontrar_carpeta_unidades(base_path: Path) -> list[Path]:
    """
    Encuentra todas las carpetas de unidad en la ruta base.
    
    Args:
        base_path: Ruta base donde buscar unidades
        
    Returns:
        Lista de paths de carpetas de unidad
    """
    return [
        carpeta for carpeta in sorted(base_path.iterdir())
        if carpeta.is_dir() 
        and not carpeta.name.lower() == "alumnos"
        and carpeta.name.lower().startswith("unidad")
    ]


def procesar_archivos_en_carpeta(
    tx_funcion: TransactionFunction, 
    driver: Driver, 
    unidad_nombre: str, 
    carpeta: Optional[Path], 
    tipo_archivo: str
) -> int:
    """
    Procesa archivos CSV en una carpeta específica.
    
    Args:
        tx_funcion: Función de transacción a ejecutar
        driver: Driver de Neo4j
        unidad_nombre: Nombre de la unidad
        carpeta: Carpeta donde buscar archivos
        tipo_archivo: Tipo de archivo para logging
        
    Returns:
        Número de archivos procesados
    """
    if not carpeta or not carpeta.exists() or not carpeta.is_dir():
        logger.warning(f"⚠️ Carpeta de {tipo_archivo} no encontrada en {unidad_nombre}")
        return 0
    
    archivos_procesados = 0
    for archivo in sorted(carpeta.iterdir()):
        if archivo.is_file() and archivo.suffix.lower() == ".csv":
            try:
                logger.debug(f"   📄 Procesando {tipo_archivo}: {archivo.name}")
                with driver.session() as session:
                    session.execute_write(tx_funcion, unidad_nombre, archivo.name)
                archivos_procesados += 1
            except Exception as e:
                logger.error(f"❌ Error procesando {tipo_archivo} {archivo.name}: {e}")
                # Continuar con el siguiente archivo
                continue
                
    return archivos_procesados


# ==========================
# Función: procesar carpetas de Unidades y llamar a los inserts
# ==========================
def procesar_cuestionarios_y_ayudantias(driver: Driver, base_path: Union[str, Path]) -> None:
    """
    Recorre las carpetas bajo base_path que comiencen con 'Unidad' (ignora 'Alumnos'),
    busca carpetas 'Cuestionarios' y 'Ayudantías' y para cada .csv llama al insert
    correspondiente usando session.execute_write.
    
    Args:
        driver: Instancia neo4j.Driver provista por el main
        base_path: Path o str a la carpeta que contiene 'Unidad 1', 'Unidad 2', ...
        
    Raises:
        FileNotFoundError: Si la ruta base no es válida
        ValueError: Si no se encuentran unidades para procesar
    """
    base = Path(base_path)
    if not base.exists():
        raise FileNotFoundError(f"La ruta base no existe: {base}")
    
    if not base.is_dir():
        raise FileNotFoundError(f"La ruta base no es un directorio: {base}")

    logger.info(f"🔍 Iniciando procesamiento de cuestionarios y ayudantías en: {base}")
    
    carpetas_unidad = encontrar_carpeta_unidades(base)
    
    if not carpetas_unidad:
        error_msg = f"No se encontraron carpetas de Unidad en: {base}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    total_cuestionarios = 0
    total_ayudantias = 0

    for carpeta_unidad in carpetas_unidad:
        unidad_nombre = carpeta_unidad.name
        logger.info(f"📁 Procesando unidad: {unidad_nombre}")

        # Procesar cuestionarios
        cuestionarios_dir = carpeta_unidad / "Cuestionarios"
        cuestionarios_procesados = procesar_archivos_en_carpeta(
            insertar_cuestionario, driver, unidad_nombre, cuestionarios_dir, "cuestionario"
        )
        total_cuestionarios += cuestionarios_procesados

        # Procesar ayudantías
        ayudantias_dir = carpeta_unidad / "Ayudantías"
        ayudantias_procesadas = procesar_archivos_en_carpeta(
            insertar_ayudantia, driver, unidad_nombre, ayudantias_dir, "ayudantía"
        )
        total_ayudantias += ayudantias_procesadas

    logger.info(f"✅ Procesamiento completado: {total_cuestionarios} cuestionarios, {total_ayudantias} ayudantías procesados")


# ==========================
# Función: contar cuestionarios y ayudantías (para verificación)
# ==========================
def contar_cuestionarios_y_ayudantias(driver: Driver) -> dict[str, int]:
    """
    Cuenta el número total de cuestionarios y ayudantías en la base de datos.
    
    Args:
        driver: Driver de Neo4j
        
    Returns:
        Diccionario con conteos de cuestionarios y ayudantías
    """
    with driver.session() as session:
        try:
            # Contar cuestionarios
            result_cuestionarios = session.run("MATCH (c:Cuestionario) RETURN count(c) as total")
            record_c = result_cuestionarios.single()
            total_cuestionarios: int = record_c["total"] if record_c else 0
            
            # Contar ayudantías
            result_ayudantias = session.run("MATCH (a:Ayudantia) RETURN count(a) as total")
            record_a = result_ayudantias.single()
            total_ayudantias: int = record_a["total"] if record_a else 0
            
            return {
                "cuestionarios": total_cuestionarios,
                "ayudantias": total_ayudantias
            }
        except Exception as e:
            logger.error(f"❌ Error contando cuestionarios y ayudantías: {e}")
            return {"cuestionarios": 0, "ayudantias": 0}