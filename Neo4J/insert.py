from neo4j import ManagedTransaction

def crear_datos(tx: ManagedTransaction) -> None:
    """
    Inserta datos de ejemplo en la base Neo4j para crear un grafo inicial.

    - Crea nodos de tipo Alumno con identificadores únicos.
    - Crea nodos de tipo Actividad con identificadores únicos.
    - Establece relaciones:
        * COMPLETA: indica que un alumno completó una actividad.
        * DESBLOQUEA: indica que completar una actividad desbloquea otra.

    Estos datos permiten luego hacer consultas de recomendación basadas en actividades
    que un alumno puede realizar a partir de las que ya completó.
    """

    # Creación de nodos Alumno y Actividad
    tx.run("""
        CREATE (:Alumno {id: 'Alumno_001'}),
               (:Alumno {id: 'Alumno_002'}),
               (:Actividad {id: 'Actividad_001'}),
               (:Actividad {id: 'Actividad_002'}),
               (:Actividad {id: 'Actividad_003'})
    """)

    # Creación de relaciones entre nodos
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
