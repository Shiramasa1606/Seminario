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

Estructura de datos esperada:
    - DataFrame con columnas: ['Nombre', 'Apellido(s)', 'Dirección de correo']
    - Correo electrónico como identificador único
"""

import pandas as pd
from pandas import DataFrame
from typing import Any, Dict
from neo4j import ManagedTransaction
import logging

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)


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
    MERGE para prevenir duplicados.
    
    Flujo de procesamiento:
        1. ✅ Validación de estructura del DataFrame
        2. 🧹 Limpieza y normalización de datos
        3. 📧 Validación de correos electrónicos
        4. 🔍 Prevención de duplicados con MERGE
        5. 📊 Reporte de resultados
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar las inserciones
        alumnos: DataFrame con las columnas requeridas:
                - 'Nombre': Nombre del alumno
                - 'Apellido(s)': Apellidos del alumno  
                - 'Dirección de correo': Correo electrónico único
        
    Raises:
        ValueError: Si el DataFrame no tiene las columnas requeridas
        KeyError: Si faltan columnas esenciales en el DataFrame
        
    Returns:
        None: Los resultados se reportan vía logging
        
    Example:
        >>> df = pd.DataFrame({
        ...     'Nombre': ['Juan', 'María'],
        ...     'Apellido(s)': ['Pérez', 'García'],
        ...     'Dirección de correo': ['juan@email.com', 'maria@email.com']
        ... })
        >>> with driver.session() as session:
        ...     session.execute_write(insertar_alumno, df)
        ✅ Inserción de alumnos completada: 2 insertados, 0 errores
        
    Note:
        - Los correos se convierten a minúsculas automáticamente
        - Se omiten filas con datos incompletos o inválidos
        - El proceso continúa despite errores individuales
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

    alumnos_insertados = 0
    errores = 0

    for index, row in alumnos.iterrows():
        try:
            # Validar y limpiar datos
            nombre: str = str(row['Nombre']).strip() if pd.notna(row['Nombre']) else ""
            apellidos: str = str(row['Apellido(s)']).strip() if pd.notna(row['Apellido(s)']) else ""
            correo_raw = row['Dirección de correo']
            
            # Validar correo electrónico
            if pd.isna(correo_raw) or not correo_raw:
                logger.warning(f"⚠️ Fila {index}: Correo vacío o inválido, omitiendo")
                errores += 1
                continue
                
            correo: str = str(correo_raw).strip().lower()
            
            # Validar que tengamos nombre y correo
            if not nombre or not apellidos or not correo:
                logger.warning(f"⚠️ Fila {index}: Datos incompletos, omitiendo")
                errores += 1
                continue

            nombre_completo = f"{nombre} {apellidos}".strip()

            # Usar MERGE en lugar de CREATE para evitar duplicados
            result = tx.run(
                """
                MERGE (a:Alumno {correo: $correo})
                SET a.nombre = $nombre
                RETURN a.correo as correo
                """,
                {
                    "nombre": nombre_completo,
                    "correo": correo,
                },
            )
            
            record = result.single()
            if record:
                correo_insertado: str = record["correo"]
                logger.debug(f"✅ Alumno procesado: {correo_insertado}")
                alumnos_insertados += 1
            else:
                logger.warning(f"⚠️ Fila {index}: No se pudo verificar la inserción del alumno")
                errores += 1

        except Exception as e:
            logger.error(f"❌ Error insertando alumno en fila {index}: {e}")
            errores += 1
            # Continuar con el siguiente alumno en lugar de fallar completamente
            continue

    logger.info(f"✅ Inserción de alumnos completada: {alumnos_insertados} insertados, {errores} errores")


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
    el correo proporcionado.
    
    Args:
        tx: Transacción activa de Neo4J para ejecutar la búsqueda
        correo: Dirección de correo electrónico del alumno a buscar
        
    Returns:
        Dict[str, Any]: Diccionario con los datos del alumno si existe.
                       Contiene las keys 'nombre' y 'correo'.
                       Retorna diccionario vacío si el alumno no existe.
                       
    Example:
        >>> with driver.session() as session:
        ...     alumno = session.execute_read(
        ...         buscar_alumno_por_correo, "juan@email.com"
        ...     )
        >>> if alumno:
        ...     print(f"Encontrado: {alumno['nombre']}")
        Encontrado: Juan Pérez
        
    Note:
        - El correo se normaliza a minúsculas automáticamente
        - Retorna dict vacío para alumnos no encontrados
        - Búsqueda case-insensitive
    """
    try:
        result = tx.run(
            "MATCH (a:Alumno {correo: $correo}) RETURN a.nombre as nombre, a.correo as correo",
            correo=correo.lower().strip()
        )
        record = result.single()
        if record:
            return {
                "nombre": record["nombre"],
                "correo": record["correo"]
            }
        return {}
    except Exception as e:
        logger.error(f"❌ Error buscando alumno {correo}: {e}")
        return {}