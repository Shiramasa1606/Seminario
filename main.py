from typing import Any
from Neo4J.conn import obtener_driver
from Neo4J.insert import crear_datos
from Neo4J.consultar import demo_recomendaciones, demo_completitud_alumno, demo_completitud_general


def datos_existen(tx: Any) -> bool:
    result = tx.run("MATCH (aula:Nodo {id: 'Aula_Python'}) RETURN aula LIMIT 1")
    return result.single() is not None


def main() -> None:
    driver = obtener_driver()
    try:
        with driver.session() as session:
            existe: bool = session.execute_read(datos_existen)
            if existe:
                print("⚠️ Los datos ya existen en la base, se omite carga.")
                exito = True
            else:
                print("ℹ️ No se encontraron datos, cargando...")
                exito = session.execute_write(crear_datos)
                if exito:
                    print("✅ Datos cargados correctamente.")
                else:
                    print("❌ Error al cargar los datos.")
                    return

        if exito:
            demo_recomendaciones("Alumno_001", limite=5)
            demo_completitud_alumno("Alumno_001", "Modulo_1")
            demo_completitud_general("Modulo_1")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
