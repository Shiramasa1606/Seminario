# main.py
from conn import obtener_driver
from insert import crear_datos
from consultar import consultar_para

if __name__ == "__main__":
    driver = obtener_driver()

    with driver.session() as session:
        session.execute_write(crear_datos)
        print("✅ Datos cargados correctamente.")

    consultar_para("Alumno_001")
