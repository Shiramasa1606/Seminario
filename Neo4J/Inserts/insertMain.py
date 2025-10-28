# insertMain.py
import os
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from dotenv import load_dotenv
from neo4j import Driver
from Neo4J.conn import obtener_driver

# ==========================
# Importar módulos internos
# ==========================
from Neo4J.Inserts.insertarAlumnos import insertar_alumno, limpiar_bd
from Neo4J.Inserts.insertarMaterial import procesar_unidades_y_raps
from Neo4J.Inserts.insertarCuestionariosAyudantias import procesar_cuestionarios_y_ayudantias
from Neo4J.Inserts.Relaciones.relacionarAlumnos import procesar_unidades as relacionar_alumnos
from Neo4J.Inserts.Relaciones.relacionarMaterial import procesar_relaciones as validar_relaciones_material

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
# Funciones para estadísticas
# ==========================
def obtener_estadisticas_bd(driver: Driver) -> Dict[str, Any]:
    """
    Obtiene estadísticas actuales de la base de datos
    """
    with driver.session() as session:
        try:
            result = session.run("""
                MATCH (n)
                RETURN 
                    COUNT(n) as total_nodos,
                    COUNT { MATCH (a:Alumno) RETURN a } as total_alumnos,
                    COUNT { MATCH (u:Unidad) RETURN u } as total_unidades,
                    COUNT { MATCH (r:RAP) RETURN r } as total_raps,
                    COUNT { MATCH (c:Cuestionario) RETURN c } as total_cuestionarios,
                    COUNT { MATCH (ay:Ayudantia) RETURN ay } as total_ayudantias,
                    COUNT { MATCH ()-[r]->() RETURN r } as total_relaciones
            """)
            record = result.single()
            if record:
                return dict(record)
            return {}
        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {}

def calcular_porcentaje(actual: int, anterior: int) -> str:
    """
    Calcula el porcentaje de incremento
    """
    if anterior == 0:
        return "N/A"
    incremento = actual - anterior
    porcentaje = (incremento / anterior) * 100 if anterior > 0 else 0
    return f"{porcentaje:+.1f}%"

def mostrar_estadisticas_finales(estadisticas_iniciales: Dict[str, Any], 
                               estadisticas_finales: Dict[str, Any]) -> None:
    """
    Muestra las estadísticas finales con comparativas
    """
    print("\n" + "="*60)
    print("📊 ESTADÍSTICAS FINALES DEL PROCESO")
    print("="*60)
    
    # Totales por tipo
    tipos = [
        ("Alumnos", "total_alumnos", "👥"),
        ("Unidades", "total_unidades", "📁"), 
        ("RAPs", "total_raps", "📚"),
        ("Cuestionarios", "total_cuestionarios", "📝"),
        ("Ayudantías", "total_ayudantias", "👥"),
        ("Relaciones", "total_relaciones", "🔗")
    ]
    
    for nombre, clave, emoji in tipos:
        inicial = estadisticas_iniciales.get(clave, 0)
        final = estadisticas_finales.get(clave, 0)
        diferencia = final - inicial
        
        if diferencia > 0:
            porcentaje = calcular_porcentaje(final, inicial)
            print(f"{emoji} {nombre}: {final} (+{diferencia}) [{porcentaje}]")
        else:
            print(f"{emoji} {nombre}: {final}")
    
    # Estadísticas generales
    total_nodos_inicial = estadisticas_iniciales.get('total_nodos', 0)
    total_nodos_final = estadisticas_finales.get('total_nodos', 0)
    total_nuevos_nodos = total_nodos_final - total_nodos_inicial
    
    print(f"\n🎯 RESUMEN GENERAL:")
    print(f"   • Nodos totales en BD: {total_nodos_final}")
    print(f"   • Nuevos nodos insertados: {total_nuevos_nodos}")
    
    # Calcular porcentaje de completitud por tipo de actividad
    if total_nodos_final > 0:
        raps = estadisticas_finales.get('total_raps', 0)
        cuestionarios = estadisticas_finales.get('total_cuestionarios', 0)
        ayudantias = estadisticas_finales.get('total_ayudantias', 0)
        total_actividades = raps + cuestionarios + ayudantias
        
        print(f"\n📈 DISTRIBUCIÓN DE ACTIVIDADES:")
        if total_actividades > 0:
            print(f"   • RAPs: {raps} ({raps/total_actividades*100:.1f}%)")
            print(f"   • Cuestionarios: {cuestionarios} ({cuestionarios/total_actividades*100:.1f}%)")
            print(f"   • Ayudantías: {ayudantias} ({ayudantias/total_actividades*100:.1f}%)")
            print(f"   • Total actividades: {total_actividades}")

# ==========================
# Función auxiliar para procesar alumnos
# ==========================
def procesar_alumnos_con_driver(driver: Driver, rutas_csv: List[Path]) -> int:
    """
    Procesa alumnos usando driver en lugar de session.
    
    Args:
        driver: Driver de Neo4j
        rutas_csv: Lista de rutas a archivos CSV de alumnos
        
    Returns:
        Número de alumnos procesados
    """
    total_alumnos = 0
    for ruta in rutas_csv:
        if not ruta.exists():
            print(f"⚠️ Archivo no encontrado: {ruta}")
            continue
            
        try:
            df: pd.DataFrame = pd.read_csv(ruta)  # type: ignore
            # Contar alumnos en el CSV
            alumnos_en_csv = len(df)
            print(f"📄 Procesando {alumnos_en_csv} alumnos desde: {ruta.name}")
            
            # Usar driver directamente en lugar de session.execute_write
            with driver.session() as session:
                session.execute_write(insertar_alumno, df)
            
            total_alumnos += alumnos_en_csv
            print(f"✅ {alumnos_en_csv} alumnos insertados desde: {ruta.name}")
            
        except Exception as e:
            print(f"❌ Error procesando alumnos desde {ruta}: {e}")
    
    return total_alumnos

# ==========================
# Función auxiliar para limpiar BD con driver
# ==========================
def limpiar_bd_con_driver(driver: Driver) -> None:
    """
    Limpia la base de datos usando driver.
    
    Args:
        driver: Driver de Neo4j
    """
    try:
        with driver.session() as session:
            session.execute_write(limpiar_bd)
        print("🧹 Base de datos limpiada correctamente.")
    except Exception as e:
        print(f"❌ Error limpiando la base de datos: {e}")

# ==========================
# Función principal
# ==========================
def rellenarGrafo() -> None:
    driver: Driver = obtener_driver()

    try:
        # --------------------------
        # Obtener estadísticas iniciales
        # --------------------------
        print("📊 Obteniendo estadísticas iniciales...")
        estadisticas_iniciales = obtener_estadisticas_bd(driver)
        
        # --------------------------
        # Limpiar la BD
        # --------------------------
        print("\n🧹 LIMPIANDO BASE DE DATOS...")
        limpiar_bd_con_driver(driver)

        # --------------------------
        # Insertar Alumnos
        # --------------------------
        print("\n👥 INSERTANDO ALUMNOS...")
        rutas_csv: List[Path] = [
            BASE_PATH / "Alumnos" / "Alumnos_Paralelo_03.csv",
        ]
        total_alumnos_procesados = procesar_alumnos_con_driver(driver, rutas_csv)
        print(f"✅ Total alumnos procesados: {total_alumnos_procesados}")

        # --------------------------
        # Insertar Unidades y RAPs
        # --------------------------
        print("\n📚 INSERTANDO UNIDADES Y RAPS...")
        procesar_unidades_y_raps(driver, BASE_PATH)

        # --------------------------
        # Insertar Cuestionarios y Ayudantías
        # --------------------------
        print("\n📝 INSERTANDO CUESTIONARIOS Y AYUDANTÍAS...")
        procesar_cuestionarios_y_ayudantias(driver, BASE_PATH)

        # --------------------------
        # Validar relaciones de material (Unidades y RAPs)
        # --------------------------
        print("\n🔍 VALIDANDO RELACIONES DE MATERIAL...")
        validar_relaciones_material(driver, BASE_PATH)

        # --------------------------
        # Relacionar alumnos con cuestionarios y ayudantías
        # --------------------------
        print("\n👥 RELACIONANDO ALUMNOS CON ACTIVIDADES...")
        relacionar_alumnos(driver, BASE_PATH)

        # --------------------------
        # Obtener estadísticas finales y mostrar resumen
        # --------------------------
        print("\n📊 CALCULANDO ESTADÍSTICAS FINALES...")
        estadisticas_finales = obtener_estadisticas_bd(driver)
        mostrar_estadisticas_finales(estadisticas_iniciales, estadisticas_finales)

        print("\n🎉 ¡PROCESO COMPLETADO EXITOSAMENTE!")

    except Exception as e:
        print(f"❌ Error en el proceso principal: {e}")
        raise
    finally:
        # Cerrar el driver al finalizar
        from Neo4J.conn import cerrar_driver
        cerrar_driver()


# ==========================
# Función para mostrar estadísticas rápidas
# ==========================
def mostrar_estadisticas_rapidas() -> None:
    """
    Muestra estadísticas rápidas sin ejecutar el proceso completo
    """
    driver = obtener_driver()
    try:
        print("\n📊 ESTADÍSTICAS RÁPIDAS DE LA BASE DE DATOS")
        print("=" * 50)
        
        estadisticas = obtener_estadisticas_bd(driver)
        
        if not estadisticas:
            print("❌ No se pudieron obtener las estadísticas")
            return
        
        for clave, valor in estadisticas.items():
            nombre = clave.replace('total_', '').replace('_', ' ').title()
            print(f"   • {nombre}: {valor}")
            
    finally:
        driver.close()