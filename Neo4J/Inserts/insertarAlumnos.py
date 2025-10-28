import pandas as pd
from pandas import DataFrame
from typing import Any, Dict
from neo4j import ManagedTransaction
import logging

# Setup logging
logger = logging.getLogger(__name__)

# ==========================
# Función: limpiar la BD
# ==========================
def limpiar_bd(tx: ManagedTransaction) -> None:
    """
    Elimina todos los nodos y relaciones de la base de datos.
    
    Args:
        tx: Transacción activa de Neo4j
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
    Inserta alumnos en Neo4j a partir de un DataFrame.
    
    Args:
        tx: Transacción activa de Neo4j
        alumnos: DataFrame con columnas ['Nombre', 'Apellido(s)', 'Dirección de correo']
        
    Raises:
        ValueError: Si el DataFrame no tiene las columnas requeridas
        KeyError: Si faltan columnas en el DataFrame
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
    Cuenta el número total de alumnos en la base de datos.
    
    Args:
        tx: Transacción activa de Neo4j
        
    Returns:
        Número total de alumnos
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
    Busca un alumno por su correo electrónico.
    
    Args:
        tx: Transacción activa de Neo4j
        correo: Correo electrónico del alumno
        
    Returns:
        Diccionario con los datos del alumno o diccionario vacío si no existe
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