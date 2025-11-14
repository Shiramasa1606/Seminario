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
from typing import Union, Optional, Callable, Dict
from neo4j import Driver, ManagedTransaction
import re
import logging

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)

# Type alias para funciones de transacción que procesan archivos
TransactionFunction = Callable[[ManagedTransaction, str, str, str], None]


# ==========================
# Función: limpiar nombre de archivo
# ==========================

def limpiar_nombre_archivo(nombre_archivo: str, paralelo_objetivo: str) -> Optional[str]:
    """
    Limpia el nombre del archivo y FILTRA solo el paralelo objetivo.
    Retorna None si no es del paralelo que nos interesa.
    
    Args:
        nombre_archivo: Nombre original del archivo a procesar
        paralelo_objetivo: Paralelo específico a filtrar (ej: 'P01')
        
    Returns:
        Optional[str]: Nombre limpio del archivo o None si no corresponde al paralelo
        
    Example:
        >>> limpiar_nombre_archivo("INF1211-1234-(1S2025)-P01_Cuestionario1-calificaciones.csv", "P01")
        'Cuestionario1'
    """
    try:
        nombre_raw = Path(nombre_archivo).stem
        
        # ✅ FILTRAR: Solo procesar archivos del paralelo objetivo
        if paralelo_objetivo.upper() not in nombre_raw.upper():
            return None  # No procesar
        
        # ✅ LIMPIAR: Remover información de paralelo y código del curso
        patron_inicio = r'INF1211-1234-\(1S2025\)-' + re.escape(paralelo_objetivo) + r'_'
        nombre_sin_prefijo = re.sub(patron_inicio, '', nombre_raw, flags=re.IGNORECASE)
        
        # ✅ ELIMINAR SUFIJO "-calificaciones"
        patron_final = r'[\s_-]*calificaciones[\s_-]*$'
        nombre_sin_sufijo = re.sub(patron_final, '', nombre_sin_prefijo, flags=re.IGNORECASE)
        
        # ✅ LIMPIEZA FINAL
        nombre_limpio = nombre_sin_sufijo.strip()
        nombre_limpio = re.sub(r'\s+', ' ', nombre_limpio).strip()
        
        return nombre_limpio
        
    except Exception as e:
        logger.error(f"Error limpiando nombre de archivo '{nombre_archivo}': {e}")
        return None


# ==========================
# Función: Contar Cuestionarios y Ayudantias para ver que paralelo tiene todos los archivos
# ==========================

def encontrar_paralelo_completo(base_path: Path, cuestionarios_esperados: int = 33, ayudantias_esperadas: int = 14) -> Optional[str]:
    """
    Encuentra qué paralelo tiene la cantidad exacta de actividades esperadas.
    
    Escanea recursivamente la estructura de carpetas para identificar qué paralelo
    contiene la cantidad completa de cuestionarios y ayudantías esperadas.
    
    Args:
        base_path: Ruta base donde buscar las unidades
        cuestionarios_esperados: Número esperado de cuestionarios por paralelo
        ayudantias_esperadas: Número esperado de ayudantías por paralelo
        
    Returns:
        Optional[str]: Nombre del paralelo completo o None si no se encuentra
        
    Example:
        >>> paralelo = encontrar_paralelo_completo(Path("/ruta/actividades"))
        >>> print(paralelo)
        'P01'
    """
    print(f"Buscando paralelo con {cuestionarios_esperados} cuestionarios y {ayudantias_esperadas} ayudantías...")
    
    conteo_por_paralelo: Dict[str, Dict[str, int]] = {}
    
    # Escanear todos los archivos
    for unidad_path in base_path.glob("Unidad*"):
        if not unidad_path.is_dir():
            continue
            
        # Contar cuestionarios por paralelo
        cuestionarios_dir = unidad_path / "Cuestionarios"
        if cuestionarios_dir.exists():
            for archivo in cuestionarios_dir.glob("*.csv"):
                nombre = archivo.stem
                match = re.search(r'([Pp]0\d)', nombre)
                if match:
                    paralelo = match.group(1).upper()
                    if paralelo not in conteo_por_paralelo:
                        conteo_por_paralelo[paralelo] = {'cuestionarios': 0, 'ayudantias': 0}
                    conteo_por_paralelo[paralelo]['cuestionarios'] += 1
        
        # Contar ayudantías por paralelo
        ayudantias_dir = unidad_path / "Ayudantías"
        if ayudantias_dir.exists():
            for archivo in ayudantias_dir.glob("*.csv"):
                nombre = archivo.stem
                match = re.search(r'([Pp]0\d)', nombre)
                if match:
                    paralelo = match.group(1).upper()
                    if paralelo not in conteo_por_paralelo:
                        conteo_por_paralelo[paralelo] = {'cuestionarios': 0, 'ayudantias': 0}
                    conteo_por_paralelo[paralelo]['ayudantias'] += 1
    
    # Mostrar resultados
    print("CONTEOS POR PARALELO:")
    paralelo_elegido: Optional[str] = None
    for paralelo, conteos in sorted(conteo_por_paralelo.items()):
        status = "✅" if (conteos['cuestionarios'] == cuestionarios_esperados and 
                         conteos['ayudantias'] == ayudantias_esperadas) else "❌"
        print(f"   {paralelo}: {conteos['cuestionarios']} cuestionarios, {conteos['ayudantias']} ayudantías {status}")
        
        if (conteos['cuestionarios'] == cuestionarios_esperados and 
            conteos['ayudantias'] == ayudantias_esperadas):
            paralelo_elegido = paralelo
    
    if paralelo_elegido:
        print(f"PARALELO ELEGIDO: {paralelo_elegido}")
        return paralelo_elegido
    else:
        print("Ningún paralelo tiene la cantidad exacta esperada")
        # Elegir el que más se acerque
        mejor_paralelo: Optional[str] = None
        mejor_puntaje = -1
        for paralelo, conteos in conteo_por_paralelo.items():
            puntaje = min(conteos['cuestionarios'], cuestionarios_esperados) + min(conteos['ayudantias'], ayudantias_esperadas)
            if puntaje > mejor_puntaje:
                mejor_puntaje = puntaje
                mejor_paralelo = paralelo
        if mejor_paralelo:
            print(f"Usando el más cercano: {mejor_paralelo}")
            return mejor_paralelo
        else:
            print("No se encontró ningún paralelo")
            return None


# ==========================
# Función: insertar un Cuestionario
# ==========================

def insertar_cuestionario(tx: ManagedTransaction, unidad: str, nombre_archivo: str, paralelo_objetivo: str) -> None:
    """
    Inserta cuestionario SOLO si es del paralelo objetivo.
    
    Crea un nodo Cuestionario en la base de datos y lo relaciona con su Unidad
    correspondiente, filtrando únicamente los archivos del paralelo especificado.
    
    Args:
        tx: Transacción de Neo4J
        unidad: Nombre de la unidad a la que pertenece el cuestionario
        nombre_archivo: Nombre del archivo CSV del cuestionario
        paralelo_objetivo: Paralelo específico a procesar
        
    Example:
        >>> insertar_cuestionario(tx, "Unidad_01", "cuestionario1.csv", "P01")
    """
    try:
        nombre_limpio = limpiar_nombre_archivo(nombre_archivo, paralelo_objetivo)
        
        # Si retorna None, es porque no es del paralelo objetivo
        if nombre_limpio is None:
            return  # No procesar
            
        # Verificar si ya existe
        result_check = tx.run(
            "MATCH (c:Cuestionario {nombre: $nombre}) RETURN c",
            nombre=nombre_limpio
        )
        
        if result_check.single():
            logger.debug(f"Cuestionario duplicado - Saltando: '{nombre_limpio}'")
            return
            
        logger.info(f"Insertando cuestionario {paralelo_objetivo}: '{nombre_limpio}'")
        
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
        
        if result.single():
            logger.debug(f"Cuestionario confirmado {paralelo_objetivo}: '{nombre_limpio}'")
            
    except Exception as e:
        logger.error(f"Error insertando cuestionario: {e}")


# ==========================
# Función: insertar una Ayudantía
# ==========================

def insertar_ayudantia(tx: ManagedTransaction, unidad: str, nombre_archivo: str, paralelo_objetivo: str) -> None:
    """
    Inserta ayudantía SOLO si es del paralelo objetivo.
    
    Crea un nodo Ayudantia en la base de datos y lo relaciona con su Unidad
    correspondiente, filtrando únicamente los archivos del paralelo especificado.
    
    Args:
        tx: Transacción de Neo4J
        unidad: Nombre de la unidad a la que pertenece la ayudantía
        nombre_archivo: Nombre del archivo CSV de la ayudantía
        paralelo_objetivo: Paralelo específico a procesar
        
    Example:
        >>> insertar_ayudantia(tx, "Unidad_01", "ayudantia1.csv", "P01")
    """
    try:
        nombre_limpio = limpiar_nombre_archivo(nombre_archivo, paralelo_objetivo)
        
        # Si retorna None, es porque no es del paralelo objetivo
        if nombre_limpio is None:
            return  # No procesar
            
        # Verificar si ya existe
        result_check = tx.run(
            "MATCH (a:Ayudantia {nombre: $nombre}) RETURN a",
            nombre=nombre_limpio
        )
        
        if result_check.single():
            logger.debug(f"Ayudantía duplicada - Saltando: '{nombre_limpio}'")
            return
            
        logger.info(f"Insertando ayudantía {paralelo_objetivo}: '{nombre_limpio}'")
        
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
        
        if result.single():
            logger.debug(f"Ayudantía confirmada {paralelo_objetivo}: '{nombre_limpio}'")
            
    except Exception as e:
        logger.error(f"Error insertando ayudantía: {e}")


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
    tipo_archivo: str,
    paralelo_objetivo: str
) -> int:
    """
    Procesa archivos CSV en una carpeta específica usando el paralelo objetivo.
    
    Args:
        tx_funcion: Función de transacción a ejecutar (insertar_cuestionario o insertar_ayudantia)
        driver: Driver de conexión a Neo4J
        unidad_nombre: Nombre de la unidad actual
        carpeta: Path de la carpeta a procesar
        tipo_archivo: Tipo de archivo para logging ('cuestionario' o 'ayudantía')
        paralelo_objetivo: Paralelo específico a procesar
        
    Returns:
        int: Número de archivos procesados exitosamente
    """
    if not carpeta or not carpeta.exists() or not carpeta.is_dir():
        logger.warning(f"Carpeta de {tipo_archivo} no encontrada en {unidad_nombre}")
        return 0
    
    archivos_procesados = 0
    for archivo in sorted(carpeta.iterdir()):
        if archivo.is_file() and archivo.suffix.lower() == ".csv":
            try:
                logger.debug(f"Procesando {tipo_archivo}: {archivo.name}")
                with driver.session() as session:
                    session.execute_write(tx_funcion, unidad_nombre, archivo.name, paralelo_objetivo)
                archivos_procesados += 1
            except Exception as e:
                logger.error(f"Error procesando {tipo_archivo} {archivo.name}: {e}")
                continue
                
    return archivos_procesados


# ==========================
# Función: procesar carpetas de Unidades y llamar a los inserts
# ==========================

def procesar_cuestionarios_y_ayudantias(driver: Driver, base_path: Union[str, Path]) -> None:
    """
    Procesa recursivamente todas las unidades, cuestionarios y ayudantías en la ruta base.
    USA SOLO EL PARALELO QUE TENGA LA CANTIDAD EXACTA DE ACTIVIDADES.
    
    Args:
        driver: Driver de conexión a Neo4J
        base_path: Ruta base donde se encuentran las carpetas de unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es un directorio
        ValueError: Si no se encuentran carpetas de unidad o paralelo completo
        
    Example:
        >>> procesar_cuestionarios_y_ayudantias(driver, "/ruta/actividades")
    """
    base = Path(base_path)
    if not base.exists():
        raise FileNotFoundError(f"La ruta base no existe: {base}")
    
    if not base.is_dir():
        raise FileNotFoundError(f"La ruta base no es un directorio: {base}")

    # ✅ ENCONTRAR PARALELO COMPLETO
    paralelo_objetivo = encontrar_paralelo_completo(base, 33, 14)
    
    if not paralelo_objetivo:
        raise ValueError("No se encontró un paralelo con la cantidad exacta de actividades")
    
    print(f"Procesando actividades del paralelo: {paralelo_objetivo}")

    logger.info(f"Iniciando procesamiento de cuestionarios y ayudantías en: {base}")
    
    carpetas_unidad = encontrar_carpeta_unidades(base)
    
    if not carpetas_unidad:
        error_msg = f"No se encontraron carpetas de Unidad en: {base}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    total_cuestionarios = 0
    total_ayudantias = 0

    for carpeta_unidad in carpetas_unidad:
        unidad_nombre = carpeta_unidad.name
        logger.info(f"Procesando unidad: {unidad_nombre}")

        # Procesar cuestionarios
        cuestionarios_dir = carpeta_unidad / "Cuestionarios"
        cuestionarios_procesados = procesar_archivos_en_carpeta(
            insertar_cuestionario, driver, unidad_nombre, cuestionarios_dir, "cuestionario", paralelo_objetivo
        )
        total_cuestionarios += cuestionarios_procesados

        # Procesar ayudantías
        ayudantias_dir = carpeta_unidad / "Ayudantías"
        ayudantias_procesadas = procesar_archivos_en_carpeta(
            insertar_ayudantia, driver, unidad_nombre, ayudantias_dir, "ayudantía", paralelo_objetivo
        )
        total_ayudantias += ayudantias_procesadas

    logger.info(f"Procesamiento completado: {total_cuestionarios} cuestionarios, {total_ayudantias} ayudantías procesados del paralelo {paralelo_objetivo}")


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
            logger.error(f"Error contando cuestionarios y ayudantías: {e}")
            return {"cuestionarios": 0, "ayudantias": 0}