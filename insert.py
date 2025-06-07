from neo4j import ManagedTransaction

def crear_datos(tx: ManagedTransaction) -> None:
    tx.run("""
        CREATE (:Alumno {id: 'Alumno_001'}),
               (:Alumno {id: 'Alumno_002'}),
               (:Actividad {id: 'Actividad_001'}),
               (:Actividad {id: 'Actividad_002'}),
               (:Actividad {id: 'Actividad_003'})
    """)
    tx.run("""
        MATCH (a1:Alumno {id: 'Alumno_001'}), (a2:Alumno {id: 'Alumno_002'}),
              (act1:Actividad {id: 'Actividad_001'}),
              (act2:Actividad {id: 'Actividad_002'}),
              (act3:Actividad {id: 'Actividad_003'})
        CREATE (a1)-[:COMPLETA]->(act1),
               (a1)-[:COMPLETA]->(act2),
               (a2)-[:COMPLETA]->(act1),
               (act1)-[:DESBLOQUEA]->(act2),
               (act2)-[:DESBLOQUEA]->(act3)
    """)


