from neo4j import ManagedTransaction, Driver
from conn import obtener_driver

def obtener_recomendaciones(tx: ManagedTransaction, alumno_id: str) -> list[str]:
    """
    Consulta actividades recomendadas para un alumno específico.

    Lógica de la consulta:
    - Encuentra actividades (act1) que el alumno ya completó.
    - Busca actividades (act2) que están desbloqueadas por las actividades completadas.
    - Filtra aquellas actividades (act2) que el alumno todavía no completó.
    - Devuelve las actividades recomendadas como lista de ids.

    Args:
        tx (ManagedTransaction): Transacción para ejecutar la consulta.
        alumno_id (str): Identificador único del alumno.

    Returns:
        list[str]: Lista con los ids de actividades recomendadas.
    """
    query = """
    MATCH (a:Alumno {id: $alumno_id})-[:COMPLETA]->(act1)-[:DESBLOQUEA]->(act2)
    WHERE NOT (a)-[:COMPLETA]->(act2)
    RETURN DISTINCT act2.id AS recomendacion
    """
    result = tx.run(query, alumno_id=alumno_id)
    return [r["recomendacion"] for r in result]

def consultar_para(alumno_id: str = "Alumno_001") -> None:
    """
    Función de utilidad para ejecutar la consulta de recomendaciones
    e imprimir los resultados en consola.

    Args:
        alumno_id (str): Identificador del alumno para consultar.
    """
    driver: Driver = obtener_driver()
    with driver.session() as session:
        recomendaciones = session.execute_read(obtener_recomendaciones, alumno_id)
        print(f"🔍 Recomendaciones para {alumno_id}: {recomendaciones}")
