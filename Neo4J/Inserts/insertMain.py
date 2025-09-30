# insertMain.py
import os
from pathlib import Path
from typing import List
import pandas as pd
from dotenv import load_dotenv
from neo4j import Driver
from conn import obtener_driver
# ==========================
# Importar módulos internos
# ==========================
from insertarAlumnos import insertar_alumno, limpiar_bd
from insertarMaterial import procesar_unidades_y_raps
from insertarCuestionariosAyudantias import procesar_cuestionarios_y_ayudantias
from Relaciones.relacionarAlumnos import procesar_unidades as relacionar_alumnos
from Relaciones.relacionarMaterial import procesar_relaciones as validar_relaciones_material

# ==========================
# Cargar variables de entorno
# ==========================
load_dotenv()
BASE_PATH_STR: str = os.getenv("BASE_PATH", "")
if not BASE_PATH_STR:
    raise RuntimeError("❌ BASE_PATH no definido en .env")
BASE_PATH: Path = Path(BASE_PATH_STR)
if not BASE_PATH.exists():
    raise FileNotFoundError(f"❌ BASE_PATH no existe: {BASE_PATH}")

# ==========================
# Función principal
# ==========================
def main() -> None:
    driver: Driver = obtener_driver()

    try:
        # Abrimos una sola sesión para todo el proceso
        with driver.session() as session:
            # --------------------------
            # Limpiar la BD
            # --------------------------
            session.execute_write(limpiar_bd)
            print("🧹 Base de datos limpiada correctamente.")

            # --------------------------
            # Insertar Alumnos
            # --------------------------
            rutas_csv: List[Path] = [
                BASE_PATH / "Alumnos" / "Alumnos_Paralelo_03.csv",
            ]
            for ruta in rutas_csv:
                df: pd.DataFrame = pd.read_csv(ruta)  # type: ignore
                session.execute_write(insertar_alumno, df)
                print(f"✅ Alumnos insertados desde: {ruta.name}")

            # --------------------------
            # Insertar Unidades y RAPs
            # --------------------------
            procesar_unidades_y_raps(driver, BASE_PATH)

            # --------------------------
            # Insertar Cuestionarios y Ayudantías
            # --------------------------
            procesar_cuestionarios_y_ayudantias(driver, BASE_PATH)

            # --------------------------
            # Validar relaciones de material (Unidades y RAPs)
            # --------------------------
            validar_relaciones_material(driver, BASE_PATH)

            # --------------------------
            # Relacionar alumnos con cuestionarios y ayudantías
            # --------------------------
            relacionar_alumnos(driver, BASE_PATH)


    finally:
        driver.close()
        print("🔒 Conexión a Neo4j cerrada correctamente.")

# ==========================
# Entrada principal
# ==========================
if __name__ == "__main__":
    main()
