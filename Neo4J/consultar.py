from neo4j import Driver, ManagedTransaction
from Neo4J.conn import obtener_driver
from typing import List, Optional, Dict
from datetime import date

# ===================== Recomendaciones ===========================

def obtener_recomendaciones(tx: ManagedTransaction, alumno_id: str, limite: int = 5) -> List[str]:
    query_desbloqueadas = """
    MATCH (a:Alumno {id: $alumno_id})
    MATCH (a)-[:COMPLETA]->(:Actividad)-[:DESBLOQUEA]->(act2:Actividad)
    WHERE NOT (a)-[:COMPLETA]->(act2)
    RETURN DISTINCT act2.id AS recomendacion
    LIMIT $limite
    """
    result = tx.run(query_desbloqueadas, alumno_id=alumno_id, limite=limite)
    recomendaciones = [r["recomendacion"] for r in result]

    if not recomendaciones:
        query_sin_prerequisitos = """
        MATCH (a:Alumno {id: $alumno_id})
        MATCH (act:Actividad)
        WHERE NOT ( ()-[:DESBLOQUEA]->(act) )
          AND NOT (a)-[:COMPLETA]->(act)
        RETURN act.id AS recomendacion
        LIMIT $limite
        """
        result = tx.run(query_sin_prerequisitos, alumno_id=alumno_id, limite=limite)
        recomendaciones = [r["recomendacion"] for r in result]

    return recomendaciones

# ===================== Actividades ===========================

def obtener_todas_las_actividades(tx: ManagedTransaction) -> List[str]:
    query = """
    MATCH (a:Actividad)
    RETURN a.id AS actividad_id
    """
    result = tx.run(query)
    return [record["actividad_id"] for record in result if record.get("actividad_id")]

def obtener_actividades_por_modulo(tx: ManagedTransaction, modulo_id: str) -> List[str]:
    query = """
    MATCH (act:Actividad)-[:PERTENECE_A]->(mod:Modulo {id: $modulo_id})
    RETURN act.id AS actividad_id
    """
    result = tx.run(query, modulo_id=modulo_id)
    return [record["actividad_id"] for record in result if record.get("actividad_id")]

# ===================== Completitud ===========================

def porcentaje_completitud_por_alumno(tx: ManagedTransaction, alumno_id: str, modulo_id: str) -> float:
    query = """
    MATCH (mod:Modulo {id: $modulo_id})
    MATCH (act:Actividad)-[:PERTENECE_A]->(mod)
    WITH collect(act) AS actividades_modulo
    MATCH (a:Alumno {id: $alumno_id})
    OPTIONAL MATCH (a)-[:COMPLETA]->(actComp:Actividad)-[:PERTENECE_A]->(mod)
    WITH size(actividades_modulo) AS total, count(actComp) AS completadas
    RETURN CASE WHEN total = 0 THEN 0 ELSE (toFloat(completadas) / total) * 100 END AS porcentaje
    """
    result = tx.run(query, alumno_id=alumno_id, modulo_id=modulo_id)
    record = result.single()
    return float(record["porcentaje"]) if record and record["porcentaje"] is not None else 0.0

def porcentaje_completitud_general(tx: ManagedTransaction, modulo_id: str) -> float:
    query = """
    MATCH (mod:Modulo {id: $modulo_id})
    MATCH (act:Actividad)-[:PERTENECE_A]->(mod)
    WITH collect(act) AS actividades_modulo
    MATCH (a:Alumno)
    OPTIONAL MATCH (a)-[:COMPLETA]->(actComp:Actividad)-[:PERTENECE_A]->(mod)
    WITH size(actividades_modulo) AS total_actividades,
         count(actComp) AS total_completadas,
         count(DISTINCT a) AS total_alumnos
    RETURN CASE
        WHEN (total_actividades * total_alumnos) = 0 THEN 0
        ELSE (toFloat(total_completadas) / (total_actividades * total_alumnos)) * 100
    END AS porcentaje
    """
    result = tx.run(query, modulo_id=modulo_id)
    record = result.single()
    return float(record["porcentaje"]) if record and record["porcentaje"] is not None else 0.0

# ===================== Gestión de alumnos ===========================

def crear_alumno(tx: ManagedTransaction, alumno_id: str, nombre: Optional[str] = None) -> None:
    query = """
    MERGE (a:Alumno {id: $alumno_id})
    SET a.nombre = coalesce($nombre, a.nombre)
    """
    tx.run(query, alumno_id=alumno_id, nombre=nombre)

def marcar_actividad_completada(tx: ManagedTransaction, alumno_id: str, actividad_id: str, fecha: Optional[date] = None, a_tiempo: Optional[bool] = None) -> None:
    query = """
    MATCH (a:Alumno {id: $alumno_id})
    MATCH (act:Actividad {id: $actividad_id})
    MERGE (a)-[r:COMPLETA]->(act)
    SET r.fecha = $fecha,
        r.a_tiempo = $a_tiempo
    """
    fecha_str = fecha.isoformat() if fecha else None
    tx.run(query, alumno_id=alumno_id, actividad_id=actividad_id, fecha=fecha_str, a_tiempo=a_tiempo)

def quitar_completitud(tx: ManagedTransaction, alumno_id: str, actividad_id: str) -> None:
    query = """
    MATCH (a:Alumno {id: $alumno_id})-[r:COMPLETA]->(act:Actividad {id: $actividad_id})
    DELETE r
    """
    tx.run(query, alumno_id=alumno_id, actividad_id=actividad_id)

# ===================== Funciones auxiliares de consulta ================

def buscar_alumno(tx: ManagedTransaction, alumno_id: str) -> Optional[Dict[str, Optional[str]]]:
    query = """
    MATCH (a:Alumno {id: $alumno_id})
    RETURN a.id AS id, a.nombre AS nombre
    """
    result = tx.run(query, alumno_id=alumno_id)
    record = result.single()
    if record is None:
        return None
    return {
        "id": record.get("id"),
        "nombre": record.get("nombre")
    }

def listar_alumnos(tx: ManagedTransaction) -> List[str]:
    query = """
    MATCH (a:Alumno)
    RETURN a.id AS alumno_id
    """
    result = tx.run(query)
    return [record["alumno_id"] for record in result if record.get("alumno_id")]

# ===================== Ejemplos de ejecución rápida ==================

def demo_recomendaciones(alumno_id: str, limite: int = 5) -> None:
    driver: Driver = obtener_driver()
    with driver.session() as session:
        recomendaciones = session.execute_read(obtener_recomendaciones, alumno_id, limite)
        print(f"Recomendaciones para {alumno_id}: {recomendaciones}")

def demo_completitud_alumno(alumno_id: str, modulo_id: str) -> None:
    driver: Driver = obtener_driver()
    with driver.session() as session:
        porcentaje = session.execute_read(porcentaje_completitud_por_alumno, alumno_id, modulo_id)
        print(f"Completitud de {alumno_id} en módulo {modulo_id}: {porcentaje:.2f}%")

def demo_completitud_general(modulo_id: str) -> None:
    driver: Driver = obtener_driver()
    with driver.session() as session:
        porcentaje = session.execute_read(porcentaje_completitud_general, modulo_id)
        print(f"Completitud general del módulo {modulo_id}: {porcentaje:.2f}%")
