from pathlib import Path
from neo4j import Driver, ManagedTransaction

# ==========================
# Insertar Unidad
# ==========================
def insertar_unidad(tx: ManagedTransaction, nombre_unidad: str) -> None:
    """
    Inserta una Unidad en Neo4j si no existe, utilizando MERGE.
    """
    tx.run(
        """
        MERGE (u:Unidad {nombre: $nombre})
        """,
        nombre=nombre_unidad
    )
    print(f"✅ Unidad insertada/validada: {nombre_unidad}")


# ==========================
# Insertar RAP
# ==========================
def insertar_rap(tx: ManagedTransaction, nombre_unidad: str, nombre_rap: str) -> None:
    """
    Inserta un RAP en Neo4j y lo asocia con su Unidad.
    """
    tx.run(
        """
        MATCH (u:Unidad {nombre: $unidad})
        MERGE (r:RAP {nombre: $rap})
        MERGE (u)-[:TIENE_RAP]->(r)
        """,
        unidad=nombre_unidad,
        rap=nombre_rap
    )
    print(f"   📘 RAP insertado/validado en {nombre_unidad}: {nombre_rap}")


# ==========================
# Procesar Unidades y RAPs
# ==========================
def procesar_unidades_y_raps(driver: Driver, base_path: Path) -> None:
    """
    Procesa todas las carpetas que comienzan con 'Unidad' en la ruta base.
    Inserta las Unidades y sus respectivos RAPs en Neo4j.
    """
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"La ruta base no es válida: {base_path}")

    with driver.session() as session:
        for carpeta in base_path.iterdir():
            # Procesar solo directorios que empiecen con "Unidad"
            if carpeta.is_dir() and carpeta.name.lower().startswith("unidad"):
                # Insertar la Unidad
                session.execute_write(insertar_unidad, carpeta.name)

                # Buscar carpeta RAP dentro de la Unidad
                rap_folder = carpeta / "RAP"
                if rap_folder.exists() and rap_folder.is_dir():
                    for archivo in rap_folder.iterdir():
                        if archivo.is_file():
                            rap_name = archivo.stem  # Nombre sin extensión
                            session.execute_write(insertar_rap, carpeta.name, rap_name)
                else:
                    print(f"⚠️ Carpeta RAP no encontrada en {carpeta}")
            else:
                print(f"⏭️ Carpeta ignorada: {carpeta.name}")
