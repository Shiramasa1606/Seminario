from conn import obtener_driver
from neo4j import ManagedTransaction, Driver

def obtener_recomendaciones(tx: ManagedTransaction, alumno_id: str) -> list[str]:
    query = """
    MATCH (a:Alumno {id: $alumno_id})-[:COMPLETA]->(act1)-[:DESBLOQUEA]->(act2)
    WHERE NOT (a)-[:COMPLETA]->(act2)
    RETURN DISTINCT act2.id AS recomendacion
    """
    result = tx.run(query, alumno_id=alumno_id)
    return [r["recomendacion"] for r in result]

def consultar_para(alumno_id: str = "Alumno_001") -> None:
    driver: Driver = obtener_driver()
    with driver.session() as session:
        recomendaciones = session.execute_read(obtener_recomendaciones, alumno_id)
        print(f"🔍 Recomendaciones para {alumno_id}: {recomendaciones}")
