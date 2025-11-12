"""
Módulo de Inserción de Cuestionarios y Ayudantías - Gestión de Actividades en Neo4J

Este módulo se encarga de la gestión de actividades educativas (cuestionarios y ayudantías)
en la base de datos Neo4J. Procesa archivos CSV que representan estas actividades y las
relaciona con sus unidades correspondientes en la estructura curricular.

Funciones principales:
    - procesar_cuestionarios_y_ayudantias: Proceso principal de inserción masiva
    - insertar_cuestionario: Inserción individual de cuestionarios
    - insertar_ayudantia: Inserción individual de ayudantías
    - limpiar_nombre_archivo: Normalización de nombres de archivo
    - contar_cuestionarios_y_ayudantias: Verificación de datos insertados

Características:
    - Procesamiento recursivo de estructura de carpetas
    - Limpieza automática de nombres de archivo
    - Prevención de duplicados usando MERGE
    - Manejo robusto de errores por archivo
    - Validación de tipos de archivo (CSV exclusivamente)

Estructura de carpetas esperada:
    base_path/
    ├── Unidad_01/
    │   ├── Cuestionarios/
    │   │   ├── cuestionario_1.csv
    │   │   └── cuestionario_2.csv
    │   └── Ayudantías/
    │       └── ayudantia_1.csv
    ├── Unidad_02/
    │   ├── Cuestionarios/
    │   │   └── cuestionario_3.csv
    │   └── Ayudantías/
    │       └── ayudantia_2.csv
"""

from pathlib import Path
from typing import Union, Optional, Callable
from neo4j import Driver, ManagedTransaction
import re
import logging

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)

# Type alias para funciones de transacción que procesan archivos
TransactionFunction = Callable[[ManagedTransaction, str, str], None]


# ==========================
# Función: limpiar nombre de archivo
# ==========================

def limpiar_nombre_archivo(nombre_archivo: str) -> str:
    """
    Limpia el nombre del archivo removiendo extensión y sufijos como '-calificaciones'.
    
    Esta función es crucial para normalizar los nombres de actividades antes de
    insertarlos en la base de datos, asegurando consistencia en la nomenclatura.
    
    Args:
        nombre_archivo: Nombre original del archivo a limpiar (puede incluir extensión)
        
    Returns:
        str: Nombre limpio sin extensión ni sufijos, normalizado y trimmeado
        
    Example:
        >>> limpiar_nombre_archivo("cuestionario_1-calificaciones.csv")
        'cuestionario_1'
        >>> limpiar_nombre_archivo("ayudantia_final.CSV")
        'ayudantia_final'
        
    Note:
        - Remueve cualquier extensión de archivo
        - Elimina sufijos '-calificaciones' (case-insensitive)
        - Retorna nombre base trimmeado
        - Fallback seguro al nombre sin extensión en caso de error
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
    
    Los cuestionarios representan actividades de evaluación que pertenecen a una
    unidad específica. Esta función asegura que tanto la unidad como el cuestionario
    existan y estén relacionados correctamente.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la operación
        unidad: Nombre de la unidad padre a la que pertenece el cuestionario
        nombre_archivo: Nombre del archivo CSV (se limpiará automáticamente)
        
    Raises:
        Exception: Si hay error en la inserción por problemas de conexión
                   o restricciones de integridad
                   
    Example:
        >>> with driver.session() as session:
        ...     session.execute_write(insertar_cuestionario, "Unidad_01", "evaluacion_1.csv")
        ✅ Cuestionario insertado/validado: evaluacion_1 en Unidad_01
        
    Note:
        - Usa MERGE para operación idempotente
        - Relación: (Unidad)-[:TIENE_CUESTIONARIO]->(Cuestionario)
        - El nombre se limpia automáticamente de extensiones y sufijos
        - La unidad se crea si no existe
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
    
    Las ayudantías representan sesiones de apoyo académico que pertenecen a una
    unidad específica. Esta función asegura la creación y relación correcta
    entre la unidad y la ayudantía.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la operación
        unidad: Nombre de la unidad padre a la que pertenece la ayudantía
        nombre_archivo: Nombre del archivo CSV (se limpiará automáticamente)
        
    Raises:
        Exception: Si hay error en la inserción por problemas de conexión
                   o restricciones de integridad
                   
    Example:
        >>> with driver.session() as session:
        ...     session.execute_write(insertar_ayudantia, "Unidad_01", "sesion_apoyo.csv")
        ✅ Ayudantía insertada/validada: sesion_apoyo en Unidad_01
        
    Note:
        - Usa MERGE para operación idempotente
        - Relación: (Unidad)-[:TIENE_AYUDANTIA]->(Ayudantia)
        - El nombre se limpia automáticamente de extensiones y sufijos
        - La unidad se crea si no existe
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
    Encuentra todas las carpetas de unidad en la ruta base, excluyendo carpetas no relevantes.
    
    Realiza un escaneo del directorio base buscando carpetas que sigan el patrón
    de nomenclatura de unidades, excluyendo específicamente la carpeta 'Alumnos'.
    
    Args:
        base_path: Ruta base del sistema de archivos a escanear
        
    Returns:
        list[Path]: Lista ordenada de paths de carpetas de unidad válidas
        
    Example:
        >>> unidades = encontrar_carpeta_unidades(Path("/ruta/actividades"))
        >>> [unit.name for unit in unidades]
        ['Unidad_01', 'Unidad_02', 'Unidad_03']
        
    Note:
        - Excluye carpeta 'Alumnos' (case-insensitive)
        - Solo incluye directorios que comiencen con 'unidad'
        - Retorna lista ordenada alfabéticamente
        - Ignora archivos y otros directorios
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
    Procesa archivos CSV en una carpeta específica usando la función de transacción proporcionada.
    
    Esta función auxiliar maneja el procesamiento de archivos individuales dentro de
    una carpeta, aplicando la función de inserción correspondiente y manejando errores
    de manera granular por archivo.
    
    Args:
        tx_funcion: Función de transacción a ejecutar (insertar_cuestionario o insertar_ayudantia)
        driver: Driver de conexión a Neo4J
        unidad_nombre: Nombre de la unidad a la que pertenecen los archivos
        carpeta: Carpeta donde buscar archivos CSV (puede ser None si no existe)
        tipo_archivo: Tipo de archivo para logging descriptivo ('cuestionario' o 'ayudantía')
        
    Returns:
        int: Número de archivos procesados exitosamente
        
    Example:
        >>> procesados = procesar_archivos_en_carpeta(
        ...     insertar_cuestionario, driver, "Unidad_01", 
        ...     Path("/ruta/Unidad_01/Cuestionarios"), "cuestionario"
        ... )
        📄 Procesando cuestionario: evaluacion_1.csv
        ✅ Cuestionario insertado/validado: evaluacion_1 en Unidad_01
        >>> print(procesados)
        1
        
    Note:
        - Solo procesa archivos con extensión .csv (case-insensitive)
        - Ordena archivos alfabéticamente antes de procesar
        - Continúa procesamiento despite errores individuales
        - Retorna 0 si la carpeta no existe o está vacía
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
    Procesa recursivamente todas las unidades, cuestionarios y ayudantías en la ruta base.
    
    Esta es la función principal del módulo que orquesta todo el proceso de inserción:
    1. 🔍 Valida la ruta base y encuentra unidades
    2. 📁 Procesa cada unidad individualmente
    3. 📝 Busca y procesa cuestionarios en cada unidad
    4. 👥 Busca y procesa ayudantías en cada unidad
    5. 📊 Genera reporte final del proceso
    
    Args:
        driver: Instancia de driver Neo4J para ejecutar las operaciones
        base_path: Path o string a la carpeta que contiene las unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es un directorio
        ValueError: Si no se encuentran carpetas de unidad para procesar
        
    Example:
        >>> driver = obtener_driver()
        >>> procesar_cuestionarios_y_ayudantias(driver, "/ruta/actividades")
        🔍 Iniciando procesamiento de cuestionarios y ayudantías en: /ruta/actividades
        📁 Procesando unidad: Unidad_01
        ✅ Cuestionario insertado/validado: evaluacion_1 en Unidad_01
        ✅ Ayudantía insertada/validada: sesion_1 en Unidad_01
        ...
        ✅ Procesamiento completado: 15 cuestionarios, 8 ayudantías procesados
        
    Note:
        - Proceso continuo: errores en una unidad no detienen el proceso completo
        - Excluye automáticamente la carpeta 'Alumnos'
        - Solo procesa archivos CSV
        - Logging detallado de progreso y errores
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
    
    Función de utilidad para verificar que las inserciones se realizaron correctamente
    y obtener métricas del estado actual de la base de datos.
    
    Args:
        driver: Driver de conexión a Neo4J para ejecutar las consultas
        
    Returns:
        dict[str, int]: Diccionario con los conteos:
                       - 'cuestionarios': Número total de cuestionarios
                       - 'ayudantias': Número total de ayudantías
                       
    Example:
        >>> conteos = contar_cuestionarios_y_ayudantias(driver)
        >>> print(f"Cuestionarios: {conteos['cuestionarios']}")
        >>> print(f"Ayudantías: {conteos['ayudantias']}")
        Cuestionarios: 15
        Ayudantías: 8
        
    Note:
        - Consultas de solo lectura
        - Retorna 0 para ambos valores en caso de error (fail-safe)
        - Útil para validación post-procesamiento
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