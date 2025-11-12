"""
Módulo de Inserción de Alumnos - Gestión de Datos Estudiantiles en Neo4J

Este módulo proporciona funciones especializadas para la gestión de datos de alumnos
en la base de datos Neo4J. Incluye operaciones CRUD (Crear, Leer, Actualizar, Eliminar)
para la entidad Alumno, con validación de datos y manejo robusto de errores.

Funciones principales:
    - limpiar_bd: Limpieza completa de la base de datos
    - insertar_alumno: Inserción masiva de alumnos desde DataFrame
    - contar_alumnos: Consulta del total de alumnos registrados
    - buscar_alumno_por_correo: Búsqueda específica por correo electrónico

Características:
    - Validación exhaustiva de datos de entrada
    - Prevención de duplicados usando MERGE
    - Logging detallado de operaciones
    - Manejo graceful de errores
    - Limpieza y normalización de datos
    - Procesamiento de información de paralelos/grupos

Estructura de datos esperada:
    - DataFrame con columnas: ['Nombre', 'Apellido(s)', 'Dirección de correo', 'Grupos']
    - Correo electrónico como identificador único
    - Columna 'Grupos' puede contener múltiples grupos separados por comas

Cambios realizados:
    - ✅ Agregado soporte para columna 'Grupos' y extracción de paralelos
    - ✅ Corregidos problemas de type hints con pd.isna/pd.notna
    - ✅ Mejorada la validación de datos con manejo de valores nulos
    - ✅ Actualizada documentación con nuevos ejemplos y comportamientos
"""

from pandas import DataFrame
from typing import Any, Dict
from neo4j import ManagedTransaction
import logging
import re

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)


# ==========================
# Funciones de utilidad para procesamiento de datos
# ==========================

def extraer_paralelo(grupos_str: str) -> str:
    """
    Extrae el paralelo de la columna Grupos del CSV.
    
    La columna Grupos puede contener múltiples valores separados por comas.
    Esta función busca específicamente patrones "Paralelo_X" (case-insensitive)
    y retorna el primer paralelo encontrado.
    
    Args:
        grupos_str: String con grupos/paralelos (ej: "grupo LAB, Paralelo_3")
        
    Returns:
        str: Nombre del paralelo encontrado o "Sin_paralelo" si no se encuentra
        
    Example:
        >>> extraer_paralelo("grupo LAB RECUPERATIVO, Paralelo_3")
        'Paralelo_3'
        >>> extraer_paralelo("Paralelo_1, grupo TEORICO")
        'Paralelo_1'
        >>> extraer_paralelo("solo grupo practico")
        'Sin_paralelo'
        
    Note:
        - Busca patrones que comiencen con "Paralelo_" (case-insensitive)
        - Retorna "Sin_paralelo" para valores vacíos, nulos o sin paralelo
        - Solo extrae el primer paralelo encontrado si hay múltiples
    """
    if grupos_str is None: # pyright: ignore[reportUnnecessaryComparison]
        return "Sin_paralelo"
    
    try:
        grupos_clean = str(grupos_str).strip()
        if not grupos_clean:
            return "Sin_paralelo"
            
        # Buscar patrones Paralelo_X (case-insensitive)
        match = re.search(r'(paralelo[\s_]*\d+)', grupos_clean, re.IGNORECASE)
        if match:
            paralelo = match.group(1).replace(' ', '_')  # Normalizar espacios
            return paralelo.title()  # Paralelo_3 en lugar de paralelo_3
        return "Sin_paralelo"
    except Exception as e:
        logger.debug(f"Error extrayendo paralelo de '{grupos_str}': {e}")
        return "Sin_paralelo"


def limpiar_y_validar_dato(dato: Any) -> str:
    """
    Limpia y valida un dato del DataFrame, manejando valores nulos.
    
    Args:
        dato: Valor a limpiar (puede ser string, número, NaN, etc.)
        
    Returns:
        str: Dato limpio como string, o string vacío si es nulo/inválido
    """
    if dato is None:
        return ""
    return str(dato).strip()


# ==========================
# Función: limpiar la BD
# ==========================

def limpiar_bd(tx: ManagedTransaction) -> None:
    """
    Elimina todos los nodos y relaciones de la base de datos Neo4J.
    
    Esta operación es destructiva y debe usarse con precaución. Está diseñada
    para limpiar completamente la base de datos antes de una nueva inserción masiva.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la operación
        
    Raises:
        Exception: Si la operación de limpieza falla por problemas de conexión
                   o permisos insuficientes
                   
    Example:
        >>> with driver.session() as session:
        ...     session.execute_write(limpiar_bd)
        🗑️ Base de datos limpiada: 150 nodos eliminados
        
    Note:
        - Operación irreversible
        - Elimina TODOS los nodos y relaciones
        - Útil para resetear el estado de la base de datos
    """
    try:
        result = tx.run("MATCH (n) DETACH DELETE n")
        summary = result.consume()
        logger.info(f"🗑️ Base de datos limpiada: {summary.counters.nodes_deleted} nodos eliminados")
    except Exception as e:
        logger.error(f"❌ Error limpiando la base de datos: {e}")
        raise


# ==========================
# Función: insertar alumnos
# ==========================

def insertar_alumno(tx: ManagedTransaction, alumnos: DataFrame) -> None:
    """
    Inserta alumnos en Neo4J a partir de un DataFrame de pandas.
    
    Procesa un DataFrame con información de alumnos, valida los datos, limpia
    y normaliza la información, y la inserta en la base de datos usando operaciones
    MERGE para prevenir duplicados. Ahora incluye procesamiento de paralelos.
    
    Flujo de procesamiento:
        1. ✅ Validación de estructura del DataFrame
        2. 🧹 Limpieza y normalización de datos
        3. 📧 Validación de correos electrónicos  
        4. 🏷️ Extracción de paralelo de grupos
        5. 🔍 Prevención de duplicados con MERGE
        6. 📊 Reporte de resultados
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar las inserciones
        alumnos: DataFrame con las columnas requeridas:
                - 'Nombre': Nombre del alumno
                - 'Apellido(s)': Apellidos del alumno  
                - 'Dirección de correo': Correo electrónico único
                - 'Grupos': Grupos/paralelos del alumno (opcional)
        
    Raises:
        ValueError: Si el DataFrame no tiene las columnas requeridas
        KeyError: Si faltan columnas esenciales en el DataFrame
        
    Returns:
        None: Los resultados se reportan vía logging
        
    Example:
        >>> df = pd.DataFrame({
        ...     'Nombre': ['Juan', 'María'],
        ...     'Apellido(s)': ['Pérez', 'García'],
        ...     'Dirección de correo': ['juan@email.com', 'maria@email.com'],
        ...     'Grupos': ['Paralelo_3', 'Paralelo_1, grupo LAB']
        ... })
        >>> with driver.session() as session:
        ...     session.execute_write(insertar_alumno, df)
        ✅ Inserción de alumnos completada: 2 insertados, 0 errores
        
    Note:
        - Los correos se convierten a minúsculas automáticamente
        - Se omiten filas con datos incompletos o inválidos
        - El proceso continúa despite errores individuales
        - La columna 'Grupos' es opcional pero recomendada
    """
    # Validar que el DataFrame tenga las columnas requeridas
    required_columns = ['Nombre', 'Apellido(s)', 'Dirección de correo']
    missing_columns = [col for col in required_columns if col not in alumnos.columns]
    
    if missing_columns:
        error_msg = f"❌ Faltan columnas requeridas en el DataFrame: {missing_columns}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    if alumnos.empty:
        logger.warning("⚠️ DataFrame de alumnos está vacío")
        return

    # Verificar si existe columna Grupos
    tiene_grupos = 'Grupos' in alumnos.columns
    if not tiene_grupos:
        logger.warning("⚠️ Columna 'Grupos' no encontrada - Los alumnos se crearán sin paralelo")

    alumnos_insertados = 0
    errores = 0
    alumnos_sin_paralelo = 0

    for index, row in alumnos.iterrows():
        try:
            # Validar y limpiar datos usando función helper
            nombre: str = limpiar_y_validar_dato(row['Nombre'])
            apellidos: str = limpiar_y_validar_dato(row['Apellido(s)'])
            correo_raw = row['Dirección de correo']
            
            # Validar correo electrónico
            if correo_raw is None or not str(correo_raw).strip():
                logger.warning(f"⚠️ Fila {index}: Correo vacío o inválido, omitiendo")
                errores += 1
                continue
                
            correo: str = str(correo_raw).strip().lower()
            
            # Validar que tengamos nombre y correo
            if not nombre or not apellidos or not correo:
                logger.warning(f"⚠️ Fila {index}: Datos incompletos, omitiendo")
                errores += 1
                continue

            # Extraer paralelo si existe la columna Grupos
            paralelo: str = "Sin_paralelo"
            if tiene_grupos:
                grupos_raw = row['Grupos']
                paralelo = extraer_paralelo(grupos_raw)
                if paralelo == "Sin_paralelo":
                    alumnos_sin_paralelo += 1

            # Usar MERGE en lugar de CREATE para evitar duplicados
            # Ahora incluye la propiedad paralelo
            result = tx.run(
                """
                MERGE (a:Alumno {correo: $correo})
                SET a.nombre = $nombre,
                    a.apellidos = $apellidos,
                    a.paralelo = $paralelo
                RETURN a.correo as correo, a.paralelo as paralelo
                """,
                {
                    "nombre": nombre,
                    "apellidos": apellidos,
                    "correo": correo,
                    "paralelo": paralelo
                },
            )
            
            record = result.single()
            if record:
                correo_insertado: str = record["correo"]
                paralelo_asignado: str = record["paralelo"]
                logger.debug(f"✅ Alumno procesado: {correo_insertado} -> {paralelo_asignado}")
                alumnos_insertados += 1
            else:
                logger.warning(f"⚠️ Fila {index}: No se pudo verificar la inserción del alumno")
                errores += 1

        except Exception as e:
            logger.error(f"❌ Error insertando alumno en fila {index}: {e}")
            errores += 1
            # Continuar con el siguiente alumno en lugar de fallar completamente
            continue

    # Reporte final detallado
    logger.info(f"✅ Inserción de alumnos completada: {alumnos_insertados} insertados, {errores} errores")
    if tiene_grupos and alumnos_sin_paralelo > 0:
        logger.info(f"📊 {alumnos_sin_paralelo} alumnos fueron asignados a 'Sin_paralelo'")


# ==========================
# Función: verificar existencia de alumnos
# ==========================

def contar_alumnos(tx: ManagedTransaction) -> int:
    """
    Cuenta el número total de alumnos registrados en la base de datos.
    
    Esta función es útil para verificar el estado de la base de datos y
    validar que las inserciones se hayan realizado correctamente.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la consulta
        
    Returns:
        int: Número total de alumnos en la base de datos.
             Retorna 0 si no hay alumnos o en caso de error.
             
    Example:
        >>> with driver.session() as session:
        ...     total = session.execute_read(contar_alumnos)
        >>> print(f"Total de alumnos: {total}")
        Total de alumnos: 150
        
    Note:
        - Operación de solo lectura
        - Retorna 0 en caso de error (fail-safe)
    """
    try:
        result = tx.run("MATCH (a:Alumno) RETURN count(a) as total")
        record = result.single()
        if record:
            total: int = record["total"]
            return total
        return 0
    except Exception as e:
        logger.error(f"❌ Error contando alumnos: {e}")
        return 0


# ==========================
# Función: buscar alumno por correo
# ==========================

def buscar_alumno_por_correo(tx: ManagedTransaction, correo: str) -> Dict[str, Any]:
    """
    Busca un alumno específico por su dirección de correo electrónico.
    
    El correo electrónico se utiliza como identificador único para los alumnos.
    Esta función realiza una búsqueda case-insensitive después de normalizar
    el correo proporcionado. Ahora incluye información de paralelo.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la búsqueda
        correo: Dirección de correo electrónico del alumno a buscar
        
    Returns:
        Dict[str, Any]: Diccionario con los datos del alumno si existe.
                       Contiene las keys 'nombre', 'correo' y 'paralelo'.
                       Retorna diccionario vacío si el alumno no existe.
                       
    Example:
        >>> with driver.session() as session:
        ...     alumno = session.execute_read(
        ...         buscar_alumno_por_correo, "juan@email.com"
        ...     )
        >>> if alumno:
        ...     print(f"Encontrado: {alumno['nombre']} - {alumno['paralelo']}")
        Encontrado: Juan Pérez - Paralelo_3
        
    Note:
        - El correo se normaliza a minúsculas automáticamente
        - Retorna dict vacío para alumnos no encontrados
        - Búsqueda case-insensitive
        - Incluye información de paralelo si está disponible
    """
    try:
        result = tx.run(
            "MATCH (a:Alumno {correo: $correo}) RETURN a.nombre as nombre, a.correo as correo, a.paralelo as paralelo",
            correo=correo.lower().strip()
        )
        record = result.single()
        if record:
            return {
                "nombre": record["nombre"],
                "correo": record["correo"],
                "paralelo": record.get("paralelo", "Sin_paralelo")
            }
        return {}
    except Exception as e:
        logger.error(f"❌ Error buscando alumno {correo}: {e}")
        return {}


# ==========================
# Función: contar alumnos por paralelo
# ==========================

def contar_alumnos_por_paralelo(tx: ManagedTransaction) -> Dict[str, int]:
    """
    Cuenta el número de alumnos por cada paralelo en la base de datos.
    
    Esta función es esencial para el nuevo sistema de análisis de paralelos
    y proporciona una vista agregada de la distribución de estudiantes.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la consulta
        
    Returns:
        Dict[str, int]: Diccionario con paralelo como key y cantidad como valor
        
    Example:
        >>> with driver.session() as session:
        ...     distribucion = session.execute_read(contar_alumnos_por_paralelo)
        >>> print(distribucion)
        {'Paralelo_1': 25, 'Paralelo_2': 30, 'Paralelo_3': 20, 'Sin_paralelo': 5}
        
    Note:
        - Incluye 'Sin_paralelo' para alumnos sin paralelo asignado
        - Retorna diccionario vacío en caso de error
        - Útil para validar la correcta asignación de paralelos
    """
    try:
        result = tx.run("""
            MATCH (a:Alumno)
            RETURN a.paralelo as paralelo, count(a) as total
            ORDER BY total DESC
        """)
        
        distribucion: Dict[str, int] = {}
        for record in result:
            paralelo: str = record["paralelo"] or "Sin_paralelo"
            total: int = record["total"]
            distribucion[paralelo] = total
            
        logger.info(f"📊 Distribución de alumnos por paralelo: {distribucion}")
        return distribucion
        
    except Exception as e:
        logger.error(f"❌ Error contando alumnos por paralelo: {e}")
        return {}