import os
from typing import Mapping, Any, List
from Neo4J.conn import obtener_driver
from Neo4J.consultar import listar_alumnos, progreso_alumno, recomendar_siguiente, generar_roadmap
from Neo4J.Inserts.insertMain import rellenarGrafo

driver = obtener_driver()

# ============================================================
# Funciones de consola
# ============================================================

def limpiar_consola():
    os.system('cls' if os.name == 'nt' else 'clear')

def mostrar_menu_principal() -> str:
    print("\n=== MENÚ PRINCIPAL ===")
    print("1. Ejecutar inserción de datos (rellenar grafo)")
    print("2. Consultar alumnos y progreso")
    print("0. Salir")
    return input("Seleccione una opción: ").strip()

def mostrar_menu_alumno(nombre: str) -> str:
    print(f"\n=== Alumno: {nombre} ===")
    print("1. Ver progreso")
    print("2. Siguiente actividad recomendada")
    print("3. Roadmap completo")
    print("0. Volver al menú principal")
    return input("Seleccione una opción: ").strip()

# ============================================================
# Funciones de opciones
# ============================================================

def ver_progreso_alumno(correo: str):
    with driver.session() as session:
        progreso = session.execute_read(progreso_alumno, correo)
        if not progreso:
            print("⚠️ No hay progreso registrado para este alumno")
            return
        print("\n--- Progreso del alumno ---")
        for p in progreso:
            print(f"{p['tipo']}: {p['nombre']} → {p['estado']}")

def ver_siguiente_actividad_alumno(correo: str):
    with driver.session() as session:
        sugerencia = session.execute_read(recomendar_siguiente, correo)
        if not sugerencia:
            print("⚠️ No hay recomendaciones disponibles")
            return
        act: Mapping[str, Any] = sugerencia['actividad']
        print("\n--- Siguiente actividad recomendada ---")
        print(f"Estrategia: {sugerencia['estrategia']}")
        print(f"Actividad: {act.get('nombre', 'Sin nombre')} ({act.get('tipo', 'Desconocido')})")

def ver_roadmap_alumno(correo: str):
    with driver.session() as session:
        roadmap = session.execute_read(generar_roadmap, correo)
        if not roadmap:
            print("✅ Todas las actividades completadas o perfectas")
            return
        print("\n--- Roadmap completo ---")
        for r in roadmap:
            act: Mapping[str, Any] = r['actividad']
            print(f"{r['estrategia']} → {act.get('nombre', 'Sin nombre')} ({act.get('tipo', 'Desconocido')})")

# ============================================================
# Bucle principal
# ============================================================

def main():
    while True:
        limpiar_consola()
        opcion: str = mostrar_menu_principal()

        if opcion == "1":
            print("\n🔹 Ejecutando inserción de datos...")
            rellenarGrafo()
            input("\n✅ Inserción completada. Presione Enter para continuar...")

        elif opcion == "2":
            with driver.session() as session:
                alumnos: List[str] = session.execute_read(listar_alumnos)

            if not alumnos:
                print("⚠️ No hay alumnos registrados")
                input("\nPresione Enter para continuar...")
                continue

            while True:
                limpiar_consola()
                print("\n--- Alumnos ---")
                print("0. Volver al menú principal")
                for idx, nombre in enumerate(alumnos, start=1):
                    print(f"{idx}. {nombre}")

                try:
                    seleccion = int(input(f"\nSeleccione un alumno (0-{len(alumnos)}): ").strip())
                except ValueError:
                    print("❌ Ingrese un número válido")
                    input("\nPresione Enter para continuar...")
                    continue

                if seleccion == 0:
                    break  # volver al menú principal
                elif 1 <= seleccion <= len(alumnos):
                    alumno_nombre = alumnos[seleccion - 1]

                    while True:
                        limpiar_consola()
                        print(f"\n--- Alumno: {alumno_nombre} ---")
                        print("1. Ver progreso")
                        print("2. Siguiente actividad recomendada")
                        print("3. Roadmap completo")
                        print("0. Volver al listado de alumnos")
                        opcion_alumno: str = input("Seleccione una opción: ").strip()

                        if opcion_alumno == "1":
                            ver_progreso_alumno(alumno_nombre)
                        elif opcion_alumno == "2":
                            ver_siguiente_actividad_alumno(alumno_nombre)
                        elif opcion_alumno == "3":
                            ver_roadmap_alumno(alumno_nombre)
                        elif opcion_alumno == "0":
                            break  # volver al listado de alumnos
                        else:
                            print("❌ Opción no válida")

                        input("\nPresione Enter para continuar...")

                else:
                    print(f"❌ Ingrese un número entre 0 y {len(alumnos)}")
                    input("\nPresione Enter para continuar...")

        elif opcion == "0":
            print("👋 Saliendo...")
            driver.close()
            break

        else:
            print("❌ Opción no válida")
            input("\nPresione Enter para continuar...")


if __name__ == "__main__":
    main()