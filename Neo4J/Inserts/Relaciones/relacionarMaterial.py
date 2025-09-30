from pathlib import Path
from typing import Tuple
from neo4j import Driver, ManagedTransaction

# ==========================
# Función: Validar existencia de Unidad y RAP
# ==========================
def relacionar_unidad_rap(tx: ManagedTransaction, unidad: str, rap: str) -> Tuple[bool, bool]:
    """
    Solo valida que existan los nodos (:Unidad {nombre: unidad}) y (:RAP {nombre: rap}).
    Retorna (unidad_existe, rap_existe) para mensajes de validación.
    """
    query = """
    MATCH (u:Unidad {nombre: $unidad})
    OPTIONAL MATCH (r:RAP {nombre: $rap})
    RETURN u IS NOT NULL AS unidad_existe, r IS NOT NULL AS rap_existe
    """
    result = tx.run(query, unidad=unidad, rap=rap).single()
    if result is None:
        return (False, False)
    return (bool(result["unidad_existe"]), bool(result["rap_existe"]))


# ==========================
# Función: Procesar todas las relaciones de unidades y RAPs
# ==========================
def procesar_relaciones(driver: Driver, base_path: Path) -> None:
    """
    Recorre todas las carpetas 'Unidad X' dentro de base_path,
    entra a 'RAP' y valida cada PDF con su Unidad en Neo4j.
    """
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"No existe la ruta base: {base_path}")

    unidades_procesadas: int = 0
    relaciones_validas: int = 0
    raps_no_existentes: int = 0
    unidades_no_existentes: int = 0
    raps_omitidos: int = 0

    with driver.session() as session:
        for unidad_dir in base_path.iterdir():
            if not unidad_dir.is_dir() or not unidad_dir.name.lower().startswith("unidad"):
                continue

            unidad_nombre: str = unidad_dir.name
            unidades_procesadas += 1
            print(f"🔎 Procesando {unidad_nombre}")

            rap_folder: Path = unidad_dir / "RAP"
            if not rap_folder.exists() or not rap_folder.is_dir():
                print(f"⚠️ Carpeta RAP no encontrada en {unidad_dir}")
                continue

            archivos_pdf = sorted([p for p in rap_folder.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"])
            if not archivos_pdf:
                print(f"⚠️ No se encontraron PDFs en {rap_folder}")
                continue

            for pdf in archivos_pdf:
                rap_nombre: str = pdf.stem
                unidad_existe, rap_existe = session.execute_write(relacionar_unidad_rap, unidad_nombre, rap_nombre)

                if unidad_existe and rap_existe:
                    print(f"   ✅ Nodo validado: '{unidad_nombre}' y '{rap_nombre}'")
                    relaciones_validas += 1
                else:
                    if not unidad_existe:
                        print(f"   ❌ Unidad NO existe en Neo4j: {unidad_nombre}")
                        unidades_no_existentes += 1
                    if not rap_existe:
                        print(f"   ❌ RAP NO existe en Neo4j: {rap_nombre} (archivo: {pdf.name})")
                        raps_no_existentes += 1
                        raps_omitidos += 1

    print("\n===== Resumen =====")
    print(f"🗂️ Unidades procesadas: {unidades_procesadas}")
    print(f"🔍 Nodos validados: {relaciones_validas}")
    print(f"❗ Unidades no encontradas: {unidades_no_existentes}")
    print(f"❗ RAPs no encontrados: {raps_no_existentes}")
    print(f"⏭️ RAPs omitidos (por inexistencia de nodo): {raps_omitidos}")
