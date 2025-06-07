from Neo4J.conn import obtener_driver
from Neo4J.insert import crear_datos
from Neo4J.consultar import consultar_para

if __name__ == "__main__":
    """
    Punto de entrada principal de la aplicación.

    Pasos:
    1. Obtiene el driver para conectarse a Neo4j.
    2. Abre una sesión y ejecuta la función crear_datos para cargar
       datos de ejemplo en la base.
    3. Imprime un mensaje de confirmación.
    4. Ejecuta la función consultar_para para obtener recomendaciones
       para un alumno específico y muestra el resultado por consola.
    """

    driver = obtener_driver()

    # Ejecutar la inserción de datos dentro de una sesión de escritura
    with driver.session() as session:
        session.execute_write(crear_datos)
        print("✅ Datos cargados correctamente.")

    # Ejecutar la consulta de recomendación para el alumno 'Alumno_001'
    consultar_para("Alumno_001")
