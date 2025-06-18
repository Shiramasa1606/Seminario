from neo4j import ManagedTransaction
from typing import List, Dict
from datetime import datetime, timedelta

def crear_nodos_base(tx: ManagedTransaction) -> bool:
    try:
        # Crear curso y profesoras
        tx.run("""
            MERGE (curso:Curso {id: 'Curso_Python'})
            SET curso.nombre = 'Curso Compartido Python'

            MERGE (prof1:Profesora {id: 'Prof_001'})
            SET prof1.nombre = 'Prof. Ana'

            MERGE (prof2:Profesora {id: 'Prof_002'})
            SET prof2.nombre = 'Prof. Beatriz'

            MERGE (curso)-[:TIENE]->(:Modulo {id: 'Modulo_1', nombre: 'Basico'})
            MERGE (curso)-[:TIENE]->(:Modulo {id: 'Modulo_2', nombre: 'Funciones'})
            MERGE (curso)-[:TIENE]->(:Modulo {id: 'Modulo_3', nombre: 'Listas y Cadenas'})
        """)

        # Crear paralelos y relaciones con curso
        for id in ['1', '2', '3', '4']:
            paralelo_id = f"Paralelo_{id}"
            tx.run("""
                MERGE (p:Paralelo {id: $paralelo_id})
                MERGE (curso:Curso {id: 'Curso_Python'})
                MERGE (curso)-[:INCLUYE]->(p)
            """, paralelo_id=paralelo_id)

            # Crear ayudantes por paralelo y relaciones
            for i in [1, 2]:
                ayudante_id = f"Ayudante_{id}_{i}"
                prof_id = 'Prof_001' if id in ['1', '3', '4'] else 'Prof_002'

                tx.run("""
                    MERGE (a:Ayudante {id: $ayudante_id})
                    MERGE (p:Paralelo {id: $paralelo_id})
                    MERGE (prof:Profesora {id: $prof_id})
                    MERGE (a)-[:APOYA_A]->(p)
                    MERGE (a)-[:REPORTA_A]->(prof)
                """, ayudante_id=ayudante_id, paralelo_id=paralelo_id, prof_id=prof_id)

        # Asignar profesores a paralelos
        tx.run("""
            MATCH (p1:Paralelo {id: 'Paralelo_1'}), (p3:Paralelo {id: 'Paralelo_3'}), (p4:Paralelo {id: 'Paralelo_4'}),
                  (p2:Paralelo {id: 'Paralelo_2'}),
                  (prof1:Profesora {id: 'Prof_001'}), (prof2:Profesora {id: 'Prof_002'})
            MERGE (prof1)-[:IMPARTE]->(p1)
            MERGE (prof1)-[:IMPARTE]->(p3)
            MERGE (prof1)-[:IMPARTE]->(p4)
            MERGE (prof2)-[:IMPARTE]->(p2)
        """)
        return True
    except Exception as e:
        print(f"Error en crear_nodos_base: {e}")
        return False

def crear_alumnos(tx: ManagedTransaction) -> bool:
    try:
        for paralelo in range(1, 5):
            alumnos = [f"Alumno_{paralelo}_{i:03}" for i in range(1, 41)]
            tx.run("""
                UNWIND $alumnos AS alumno_id
                MATCH (p:Paralelo {id: $paralelo_id})
                MERGE (a:Alumno {id: alumno_id})
                MERGE (a)-[:PERTENECE_A]->(p)
                """,
                alumnos=alumnos,
                paralelo_id=f"Paralelo_{paralelo}"
            )
        return True
    except Exception as e:
        print(f"Error en crear_alumnos: {e}")
        return False

def crear_pdfs_actividades(tx: ManagedTransaction) -> bool:
    try:
        for modulo in range(1, 4):
            relaciones: List[Dict[str, str]] = []
            for clase in range(1, 4):
                relaciones.append({
                    "mod_id": f"Modulo_{modulo}",
                    "pdf_id": f"PDF_{modulo}_{clase}",
                    "act_id": f"Actividad_{modulo}_{clase}"
                })

            tx.run("""
                UNWIND $relaciones AS item
                MATCH (mod:Modulo {id: item.mod_id})
                MERGE (pdf:PDF {id: item.pdf_id})
                MERGE (act:Actividad {id: item.act_id})
                MERGE (pdf)-[:PERTENECE_A]->(mod)
                MERGE (act)-[:PERTENECE_A]->(mod)
                MERGE (pdf)-[:ES_DE_CLASE]->(act)
                """,
                relaciones=relaciones
            )

            for clase in range(1, 3):
                tx.run("""
                    MATCH (a1:Actividad {id: $a1}), (a2:Actividad {id: $a2})
                    MERGE (a1)-[:DESBLOQUEA]->(a2)
                    """,
                    a1=f"Actividad_{modulo}_{clase}",
                    a2=f"Actividad_{modulo}_{clase + 1}"
                )
        return True
    except Exception as e:
        print(f"Error en crear_pdfs_actividades: {e}")
        return False

def asignar_actividades_a_alumnos(tx: ManagedTransaction) -> bool:
    try:
        for paralelo in range(1, 5):
            for i in range(1, 41):
                alumno_id = f"Alumno_{paralelo}_{i:03}"
                for modulo in range(1, 4):
                    for clase in range(1, 4):
                        act_id = f"Actividad_{modulo}_{clase}"
                        fecha_completado = datetime(2024, 7, clase + 1) + timedelta(days=(i % 3))
                        desbloqueo_siguiente = datetime(2024, 7, clase + 2)
                        a_tiempo = fecha_completado < desbloqueo_siguiente

                        tx.run("""
                            MATCH (a:Alumno {id: $alumno_id}), (act:Actividad {id: $act_id})
                            MERGE (a)-[:PUEDE_ACCEDER]->(act)
                            MERGE (a)-[c:COMPLETA]->(act)
                            SET c.fecha = date($fecha), c.a_tiempo = $a_tiempo
                            """,
                            alumno_id=alumno_id,
                            act_id=act_id,
                            fecha=fecha_completado.strftime("%Y-%m-%d"),
                            a_tiempo=a_tiempo
                        )
        return True
    except Exception as e:
        print(f"Error en asignar_actividades_a_alumnos: {e}")
        return False

def crear_datos(tx: ManagedTransaction) -> bool:
    return (
        crear_nodos_base(tx) and
        crear_alumnos(tx) and
        crear_pdfs_actividades(tx) and
        asignar_actividades_a_alumnos(tx)
    )
