from pandas import DataFrame
from neo4j import ManagedTransaction

# ==========================
# Función: limpiar la BD
# ==========================
def limpiar_bd(tx: ManagedTransaction) -> None:
    """
    Elimina todos los nodos y relaciones de la base de datos.
    """
    tx.run("MATCH (n) DETACH DELETE n")


# ==========================
# Función: insertar alumnos
# ==========================
def insertar_alumno(tx: ManagedTransaction, alumnos: DataFrame) -> None:
    """
    Inserta alumnos en Neo4j a partir de un DataFrame.
    
    Parámetros:
        tx: ManagedTransaction -> Transacción activa de Neo4j.
        alumnos: DataFrame -> DataFrame con columnas ['Nombre', 'Apellido(s)', 'Dirección de correo'].
    """
    for _, row in alumnos.iterrows():
        nombre_completo = f"{row['Nombre']} {row['Apellido(s)']}".strip()
        correo = row['Dirección de correo'].strip().lower()

        tx.run(
            """
            CREATE (a:Alumno {
                nombre: $nombre,
                correo: $correo
            })
            """,
            {
                "nombre": nombre_completo,
                "correo": correo,
            },
        )
