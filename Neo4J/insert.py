from neo4j import ManagedTransaction
from typing import List, Dict

def crear_nodos_base(tx: ManagedTransaction) -> bool:
    try:
        tx.run("""
            MERGE (aula:Aula {id: 'Aula_Python'})
            SET aula.nombre = 'Aula Compartida Python'
            
            MERGE (prof1:Profesora {id: 'Prof_001'})
            SET prof1.nombre = 'Prof. Ana'
            
            MERGE (prof2:Profesora {id: 'Prof_002'})
            SET prof2.nombre = 'Prof. Beatriz'
            
            MERGE (p1:Paralelo {id: 'Paralelo_1'})
            MERGE (p2:Paralelo {id: 'Paralelo_2'})
            MERGE (p3:Paralelo {id: 'Paralelo_3'})
            MERGE (p4:Paralelo {id: 'Paralelo_4'})
            
            MERGE (mod1:Modulo {id: 'Modulo_1'})
            SET mod1.nombre = 'Basico'
            MERGE (mod2:Modulo {id: 'Modulo_2'})
            SET mod2.nombre = 'Funciones'
            MERGE (mod3:Modulo {id: 'Modulo_3'})
            SET mod3.nombre = 'Listas y Cadenas'
            
            MERGE (prof1)-[:IMPARTE]->(p1)
            MERGE (prof1)-[:IMPARTE]->(p3)
            MERGE (prof1)-[:IMPARTE]->(p4)
            MERGE (prof2)-[:IMPARTE]->(p2)
            
            MERGE (aula)-[:INCLUYE]->(p1)
            MERGE (aula)-[:INCLUYE]->(p2)
            MERGE (aula)-[:INCLUYE]->(p3)
            MERGE (aula)-[:INCLUYE]->(p4)
            
            MERGE (aula)-[:TIENE]->(mod1)
            MERGE (aula)-[:TIENE]->(mod2)
            MERGE (aula)-[:TIENE]->(mod3)
        """)
        return True
    except Exception as e:
        print(f"Error en crear_nodos_base: {e}")
        return False

def crear_alumnos(tx: ManagedTransaction) -> bool:
    try:
        for paralelo in range(1, 5):
            alumnos: List[str] = [f"Alumno_{paralelo}_{i:03}" for i in range(1, 41)]
            tx.run(
                """
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

            tx.run(
                """
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

        for modulo in range(1, 4):
            tx.run(
                """
                MATCH (a1:Actividad {id: $act1_id}), (a2:Actividad {id: $act2_id})
                MERGE (a1)-[:DESBLOQUEA]->(a2)
                """,
                act1_id=f"Actividad_{modulo}_1",
                act2_id=f"Actividad_{modulo}_2"
            )
            tx.run(
                """
                MATCH (a1:Actividad {id: $act1_id}), (a2:Actividad {id: $act2_id})
                MERGE (a1)-[:DESBLOQUEA]->(a2)
                """,
                act1_id=f"Actividad_{modulo}_2",
                act2_id=f"Actividad_{modulo}_3"
            )
        return True
    except Exception as e:
        print(f"Error en crear_pdfs_actividades: {e}")
        return False

def crear_datos(tx: ManagedTransaction) -> bool:
    if not crear_nodos_base(tx):
        return False
    if not crear_alumnos(tx):
        return False
    if not crear_pdfs_actividades(tx):
        return False
    return True
