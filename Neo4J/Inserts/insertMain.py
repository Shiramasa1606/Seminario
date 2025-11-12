"""
Módulo de Inserción Principal - Población del Grafo Neo4J

Este módulo es el punto de entrada principal para la población completa del grafo de Neo4J
con datos educativos. Coordina todas las operaciones de inserción y relación de datos
provenientes de archivos CSV estructurados.

Funciones principales:
    - rellenarGrafo(): Proceso completo de población del grafo
    - mostrar_estadisticas_rapidas(): Consulta rápida del estado actual
    - Funciones auxiliares para estadísticas y procesamiento

Características:
    - Proceso idempotente que puede ejecutarse múltiples veces
    - Limpieza automática de datos existentes
    - Validación de relaciones y consistencia de datos
    - Generación de reportes estadísticos detallados
    - Manejo robusto de errores y cierre seguro de conexiones

Estructura de datos manejada:
    - Alumnos: Datos de estudiantes y su progreso
    - Unidades: Estructura curricular del curso
    - RAPs: Materiales de aprendizaje (Recursos de Aprendizaje)
    - Cuestionarios: Actividades de evaluación
    - Ayudantías: Sesiones de apoyo académico
"""

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
    Obtiene estadísticas actuales de la base de datos Neo4J.
    
    Consulta y retorna métricas clave sobre nodos y relaciones en la base de datos,
    incluyendo conteos por tipo de entidad y relaciones totales.
    
    Args:
        driver: Driver de conexión a Neo4J
        
    Returns:
        Dict[str, Any]: Diccionario con estadísticas de la base de datos.
                       Contiene keys como 'total_nodos', 'total_alumnos', 
                       'total_unidades', 'total_raps', etc.
                       
    Example:
        >>> stats = obtener_estadisticas_bd(driver)
        >>> print(stats['total_alumnos'])
        150
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
    Calcula el porcentaje de incremento entre dos valores.
    
    Args:
        actual: Valor actual o final
        anterior: Valor anterior o inicial
        
    Returns:
        str: Porcentaje formateado como string con signo (+/-) y símbolo %.
             Retorna "N/A" si el valor anterior es cero.
             
    Example:
        >>> calcular_porcentaje(150, 100)
        '+50.0%'
        >>> calcular_porcentaje(80, 100)  
        '-20.0%'
        >>> calcular_porcentaje(50, 0)
        'N/A'
    """
    if anterior == 0:
        return "N/A"
    incremento = actual - anterior
    porcentaje = (incremento / anterior) * 100 if anterior > 0 else 0
    return f"{porcentaje:+.1f}%"


def mostrar_estadisticas_finales(estadisticas_iniciales: Dict[str, Any], 
                               estadisticas_finales: Dict[str, Any]) -> None:
    """
    Muestra las estadísticas finales del proceso con comparativas visuales.
    
    Presenta un reporte detallado mostrando la diferencia entre el estado inicial
    y final de la base de datos, incluyendo incrementos y porcentajes de cambio.
    
    Args:
        estadisticas_iniciales: Estadísticas antes del proceso de inserción
        estadisticas_finales: Estadísticas después del proceso de inserción
        
    Example:
        >>> mostrar_estadisticas_finales(estadisticas_iniciales, estadisticas_finales)
        📊 ESTADÍSTICAS FINALES DEL PROCESO
        ============================================================
        👥 Alumnos: 150 (+150) [+inf%]
        📁 Unidades: 10 (+10) [+inf%]
        ...
    """
    print("\n" + "="*60)
    print("📊 ESTADÍSTICAS FINALES DEL PROCESO")
    print("="*60)
    
    # Totales por tipo de entidad
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
    Procesa alumnos desde archivos CSV usando driver de Neo4J.
    
    Lee múltiples archivos CSV de alumnos y los inserta en la base de datos.
    Maneja errores individuales por archivo sin detener el proceso completo.
    
    Args:
        driver: Driver de conexión a Neo4J
        rutas_csv: Lista de rutas a archivos CSV con datos de alumnos
        
    Returns:
        int: Número total de alumnos procesados exitosamente
        
    Raises:
        FileNotFoundError: Si algún archivo CSV no existe
        Exception: Para errores durante la lectura o inserción de datos
        
    Example:
        >>> rutas = [Path("alumnos1.csv"), Path("alumnos2.csv")]
        >>> total = procesar_alumnos_con_driver(driver, rutas)
        📄 Procesando 50 alumnos desde: alumnos1.csv
        ✅ 50 alumnos insertados desde: alumnos1.csv
        >>> print(total)
        100
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
    Limpia completamente la base de datos Neo4J.
    
    Ejecuta una operación de limpieza que elimina todos los nodos y relaciones
    de la base de datos, dejándola en un estado inicial limpio.
    
    Args:
        driver: Driver de conexión a Neo4J
        
    Raises:
        Exception: Si la operación de limpieza falla
        
    Example:
        >>> limpiar_bd_con_driver(driver)
        🧹 Base de datos limpiada correctamente.
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
    """
    Función principal que ejecuta el proceso completo de población del grafo Neo4J.
    
    Este proceso orquesta la secuencia completa de inserción de datos en la base de datos,
    incluyendo: limpieza inicial, inserción de alumnos, unidades, RAPs, cuestionarios,
    ayudantías, y establecimiento de todas las relaciones entre entidades.
    
    Flujo del proceso:
        1. 📊 Obtención de estadísticas iniciales
        2. 🧹 Limpieza completa de la base de datos
        3. 👥 Inserción de alumnos desde archivos CSV
        4. 📚 Inserción de unidades y materiales RAP
        5. 📝 Inserción de cuestionarios y ayudantías
        6. 🔍 Validación de relaciones entre materiales
        7. 👥 Establecimiento de relaciones alumno-actividades
        8. 📊 Generación de estadísticas finales y reporte
    
    El proceso está diseñado para ser idempotente - puede ejecutarse múltiples veces
    resultando en el mismo estado final de la base de datos.
    
    Raises:
        RuntimeError: Si hay problemas de conexión con la base de datos
        FileNotFoundError: Si los archivos CSV de entrada no existen
        Exception: Para cualquier otro error durante el proceso
    
    Example:
        >>> rellenarGrafo()
        📊 Obteniendo estadísticas iniciales...
        🧹 LIMPIANDO BASE DE DATOS...
        👥 INSERTANDO ALUMNOS...
        ...
        🎉 ¡PROCESO COMPLETADO EXITOSAMENTE!
    """
    driver: Driver = obtener_driver()

    try:
        # --------------------------
        # FASE 1: ESTADÍSTICAS INICIALES
        # --------------------------
        print("📊 Obteniendo estadísticas iniciales...")
        estadisticas_iniciales = obtener_estadisticas_bd(driver)
        
        # --------------------------
        # FASE 2: LIMPIEZA DE BASE DE DATOS
        # --------------------------
        print("\n🧹 LIMPIANDO BASE DE DATOS...")
        limpiar_bd_con_driver(driver)

        # --------------------------
        # FASE 3: INSERCIÓN DE ALUMNOS
        # --------------------------
        print("\n👥 INSERTANDO ALUMNOS...")
        rutas_csv: List[Path] = [
            BASE_PATH / "Alumnos" / "Alumnos_Paralelo_03.csv",
        ]
        total_alumnos_procesados = procesar_alumnos_con_driver(driver, rutas_csv)
        print(f"✅ Total alumnos procesados: {total_alumnos_procesados}")

        # --------------------------
        # FASE 4: INSERCIÓN DE UNIDADES Y MATERIALES
        # --------------------------
        print("\n📚 INSERTANDO UNIDADES Y RAPS...")
        procesar_unidades_y_raps(driver, BASE_PATH)

        # --------------------------
        # FASE 5: INSERCIÓN DE ACTIVIDADES
        # --------------------------
        print("\n📝 INSERTANDO CUESTIONARIOS Y AYUDANTÍAS...")
        procesar_cuestionarios_y_ayudantias(driver, BASE_PATH)

        # --------------------------
        # FASE 6: VALIDACIÓN DE RELACIONES DE MATERIAL
        # --------------------------
        print("\n🔍 VALIDANDO RELACIONES DE MATERIAL...")
        validar_relaciones_material(driver, BASE_PATH)

        # --------------------------
        # FASE 7: RELACIONES ALUMNO-ACTIVIDADES
        # --------------------------
        print("\n👥 RELACIONANDO ALUMNOS CON ACTIVIDADES...")
        relacionar_alumnos(driver, BASE_PATH)

        # --------------------------
        # FASE 8: ESTADÍSTICAS FINALES Y REPORTE
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
    Muestra estadísticas rápidas de la base de datos sin ejecutar el proceso completo.
    
    Esta función es útil para verificar el estado actual de la base de datos
    sin realizar operaciones de inserción o modificación.
    
    Example:
        >>> mostrar_estadisticas_rapidas()
        📊 ESTADÍSTICAS RÁPIDAS DE LA BASE DE DATOS
        ==================================================
           • Total Nodos: 500
           • Total Alumnos: 150
           • Total Unidades: 10
           ...
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