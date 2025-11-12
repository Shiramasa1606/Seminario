"""
Módulo de consultas Neo4J - Núcleo del Sistema

Interface principal para operaciones sobre el grafo de conocimientos de Neo4J.
Coordina todas las interacciones con la base de datos y implementa la lógica
de recomendaciones y análisis del sistema educativo.

Funciones principales:
    - Gestión de alumnos y progreso
    - Sistema de recomendaciones inteligentes
    - Generación de roadmaps de aprendizaje
    - Análisis estadísticos y métricas
    - Búsqueda de actividades siguientes

Características:
    - Excluye actividades RAP en todas las consultas
    - Implementa estrategias de aprendizaje adaptativo
    - Proporciona analytics detallados
    - Gestiona estados: Intento, Completado, Perfecto

Estrategias de recomendación:
    - Refuerzo: Para actividades con intentos fallidos
    - Mejora: Para actividades completadas pero no perfectas
    - Avance: Para actividades perfectas, sugiere nuevas
"""

from typing import List, Dict, Any, Optional, Callable, cast

from neo4j import Driver

from Neo4J.conn import obtener_driver

# Define type aliases for better clarity
ActivityDict = Dict[str, Any]
ProgressItem = Dict[str, Any]
RecommendationResult = Optional[Dict[str, Any]]
FetchNextFunction = Callable[[], Optional[ActivityDict]]


# ============================================================================
# FUNCIONES DE CONSULTA BÁSICAS
# ============================================================================

def fetch_alumnos() -> List[Dict[str, str]]:
    """
    Obtiene lista completa de todos los alumnos registrados.
    
    Returns:
        List[Dict[str, str]]: Lista de alumnos con correo y nombre
    """
    driver: Driver = obtener_driver()
    cypher = """
    MATCH (a:Alumno)
    RETURN a.correo AS correo, a.nombre AS nombre
    ORDER BY a.nombre
    """
    try:
        with driver.session() as session:
            result = session.run(cypher)
            alumnos: List[Dict[str, str]] = [
                {"correo": str(record["correo"]), "nombre": str(record["nombre"])}
                for record in result
                if record.get("correo") and record.get("nombre")
            ]
            return alumnos
    finally:
        driver.close()


def fetch_progreso_alumno(correo: str) -> List[Dict[str, Any]]:
    """
    Obtiene el progreso completo de un alumno excluyendo actividades RAP.
    
    Args:
        correo: Correo del alumno a consultar
        
    Returns:
        List[Dict[str, Any]]: Lista de actividades con estado, duración y puntaje
    """
    driver: Driver = obtener_driver()
    cypher = """
    MATCH (a:Alumno {correo: $correo})-[r]->(act)
    WHERE type(r) IN ["Intento","Completado","Perfecto"]
    AND NOT 'RAP' IN labels(act)  // EXCLUIR RAPs
    RETURN labels(act) AS labels, act.nombre AS nombre,
           type(r) AS estado_relacion,
           r.start AS start, r.end AS end, r.duration_seconds AS duration_seconds,
           r.score AS score, r.estado AS estado_raw
    """
    try:
        with driver.session() as session:
            result = session.run(cypher, correo=correo)
            progreso: List[Dict[str, Any]] = []

            for record in result:
                labels: List[str] = list(record.get("labels") or [])
                tipo: str = labels[0] if labels else "Desconocido"

                progreso.append({
                    "tipo": tipo,
                    "nombre": record.get("nombre"),
                    "estado": record.get("estado_relacion"),
                    "start": record.get("start"),
                    "end": record.get("end"),
                    "duration_seconds": record.get("duration_seconds"),
                    "score": record.get("score"),
                    "estado_raw": record.get("estado_raw")
                })
            return progreso
    finally:
        driver.close()


# ============================================================================
# FUNCIONES DE RECOMENDACIÓN Y ROADMAP (EXCLUYENDO RAPs)
# ============================================================================

def fetch_recomendacion_siguiente(progreso: List[ProgressItem]) -> RecommendationResult:
    """
    Analiza el progreso y recomienda siguiente actividad con estrategia específica.
    
    Estrategias:
        - Refuerzo: Si hay actividades con estado "Intento"
        - Mejora: Si hay actividades con estado "Completado" 
        - Avance: Si hay actividades con estado "Perfecto"
    
    Args:
        progreso: Lista del progreso actual del alumno
        
    Returns:
        RecommendationResult: Recomendación con estrategia y actividad, o None
    """
    if not progreso:
        return None

    # Ya no necesitamos filtrar RAPs porque fetch_progreso_alumno ya los excluye
    intentos = [p for p in progreso if p.get("estado") == "Intento"]
    if intentos:
        return {"estrategia": "refuerzo", "actividad": intentos[0]}

    completados = [p for p in progreso if p.get("estado") == "Completado"]
    if completados:
        return {"estrategia": "mejora", "actividad": completados[0]}

    perfectos = [p for p in progreso if p.get("estado") == "Perfecto"]
    if perfectos:
        return {"estrategia": "avance", "actividad": perfectos[0]}

    return None


def fetch_roadmap_desde_progreso(
    progreso: List[ProgressItem],
    fetch_next_for_avance: FetchNextFunction
) -> List[Dict[str, Any]]:
    """
    Genera secuencia de actividades recomendadas (roadmap) basado en progreso actual.
    
    Simula el avance del alumno aplicando estrategias de recomendación de forma
    iterativa hasta completar un camino de aprendizaje.
    
    Args:
        progreso: Progreso inicial del alumno
        fetch_next_for_avance: Función para obtener siguiente actividad
        
    Returns:
        List[Dict[str, Any]]: Secuencia de actividades recomendadas con estrategias
    """
    roadmap: List[Dict[str, Any]] = []
    seen: set[tuple[Optional[str], Optional[str], str]] = set()

    # copia en memoria para simular progresos (ya excluye RAPs por fetch_progreso_alumno)
    prog_map: Dict[tuple[Optional[str], Optional[str]], ActivityDict] = {}
    for p in progreso:
        key = (p.get("tipo"), p.get("nombre"))
        prog_map[key] = p

    while True:
        rec = fetch_recomendacion_siguiente(list(prog_map.values()))
        if not rec:
            break

        estrategia = rec["estrategia"]
        actividad = cast(ActivityDict, rec["actividad"])

        # Si avance, necesitarás resolver la siguiente actividad en el grafo
        if estrategia == "avance":
            siguiente = fetch_next_for_avance()
            if not siguiente:
                break
            actividad = siguiente

        # Extract variables to help with type inference
        act_tipo = actividad.get("tipo")
        act_nombre = actividad.get("nombre")
        act_key = (act_tipo, act_nombre, estrategia)
        
        if act_key in seen:
            break
        seen.add(act_key)
        roadmap.append({"estrategia": estrategia, "actividad": actividad})

        # simular avance en prog_map
        prog_key = (act_tipo, act_nombre)
        if prog_key in prog_map:
            if estrategia == "refuerzo":
                prog_map[prog_key]["estado"] = "Completado"
            elif estrategia == "mejora":
                prog_map[prog_key]["estado"] = "Perfecto"
            else:
                prog_map[prog_key]["estado"] = "Perfecto"
        else:
            prog_map[prog_key] = {
                "tipo": act_tipo, 
                "nombre": act_nombre, 
                "estado": "Completado"
            }

    return roadmap


# ============================================================================
# FUNCIONES DE ACTIVIDADES SIGUIENTES (EXCLUYENDO RAPs)
# ============================================================================

def fetch_siguiente_actividad_mejorada(correo: str) -> Optional[Dict[str, Any]]:
    """
    Encuentra siguiente actividad usando estrategia mejorada que considera:
    - Unidad actual del alumno
    - Prioridad por actividades en misma unidad
    - Exclusión de actividades RAP
    
    Args:
        correo: Correo del alumno
        
    Returns:
        Optional[Dict[str, Any]]: Siguiente actividad recomendada con prioridad
    """
    driver: Driver = obtener_driver()
    
    cypher = """
    // ESTRATEGIA MEJORADA - EXCLUIR RAPs completamente
    MATCH (a:Alumno {correo: $correo})
    
    // Buscar última actividad del alumno (excluyendo RAPs)
    OPTIONAL MATCH (a)-[r:Intento|Completado|Perfecto]->(ultima_act)
    WHERE NOT 'RAP' IN labels(ultima_act)
    WITH a, ultima_act
    ORDER BY r.end DESC
    LIMIT 1
    
    // Buscar su unidad
    OPTIONAL MATCH (unidad_ultima:Unidad)-[:TIENE_CUESTIONARIO|TIENE_AYUDANTIA]->(ultima_act)
    
    // Buscar actividades en esta unidad (EXCLUYENDO RAPs)
    OPTIONAL MATCH (unidad_ultima)-[:TIENE_CUESTIONARIO|TIENE_AYUDANTIA]->(siguiente_misma_unidad)
    WHERE siguiente_misma_unidad IS NOT NULL 
      AND NOT 'RAP' IN labels(siguiente_misma_unidad)  // EXCLUIR RAPs
      AND NOT (a)-[:Completado|Perfecto]->(siguiente_misma_unidad)
    
    // Si no hay en la misma unidad, buscar en cualquier unidad (EXCLUYENDO RAPs)
    OPTIONAL MATCH (cualquier_unidad:Unidad)-[:TIENE_CUESTIONARIO|TIENE_AYUDANTIA]->(siguiente_cualquier)
    WHERE siguiente_cualquier IS NOT NULL 
      AND NOT 'RAP' IN labels(siguiente_cualquier)  // EXCLUIR RAPs
      AND NOT (a)-[:Completado|Perfecto]->(siguiente_cualquier)
    
    WITH 
        siguiente_misma_unidad AS candidato1,
        siguiente_cualquier AS candidato2,
        CASE 
            WHEN siguiente_misma_unidad IS NOT NULL THEN 1
            ELSE 2
        END AS prioridad
    
    // Elegir el mejor candidato
    WITH coalesce(candidato1, candidato2) AS siguiente, prioridad
    
    WHERE siguiente IS NOT NULL
    
    RETURN 
        labels(siguiente) AS labels, 
        siguiente.nombre AS nombre,
        prioridad
    ORDER BY prioridad
    LIMIT 1
    """
    
    try:
        with driver.session() as session:
            record = session.run(cypher, correo=correo).single()
            if not record:
                return None

            labels: List[str] = list(record.get("labels") or [])
            tipo: str = labels[0] if labels else "Desconocido"

            return {
                "tipo": tipo, 
                "nombre": record.get("nombre"),
                "prioridad": record.get("prioridad")
            }
    finally:
        driver.close()


def fetch_siguiente_actividad_simple(correo: str) -> Optional[Dict[str, Any]]:
    """
    Versión simple para encontrar siguiente actividad no completada.
    Excluye actividades RAP y busca cualquier actividad disponible.
    
    Args:
        correo: Correo del alumno
        
    Returns:
        Optional[Dict[str, Any]]: Siguiente actividad disponible
    """
    driver: Driver = obtener_driver()
    
    cypher = """
    // QUERY SIMPLE: EXCLUIR RAPs completamente
    MATCH (a:Alumno {correo: $correo})
    MATCH (siguiente:Cuestionario|Ayudantia)  // Solo Cuestionarios y Ayudantias
    WHERE NOT (a)-[:Completado|Perfecto]->(siguiente)
    RETURN labels(siguiente) AS labels, siguiente.nombre AS nombre
    ORDER BY siguiente.nombre
    LIMIT 1
    """
    
    try:
        with driver.session() as session:
            record = session.run(cypher, correo=correo).single()
            if not record:
                return None

            labels: List[str] = list(record.get("labels") or [])
            tipo: str = labels[0] if labels else "Desconocido"

            return {"tipo": tipo, "nombre": record.get("nombre")}
    finally:
        driver.close()


def fetch_siguiente_actividad(correo: str) -> Optional[Dict[str, Any]]:
    """
    Alias principal para mantener compatibilidad con el sistema.
    Utiliza la versión simple por defecto.
    
    Args:
        correo: Correo del alumno
        
    Returns:
        Optional[Dict[str, Any]]: Siguiente actividad recomendada
    """
    return fetch_siguiente_actividad_simple(correo)


# ============================================================================
# FUNCIONES DE ESTADÍSTICAS Y ANÁLISIS (EXCLUYENDO RAPs)
# ============================================================================

def fetch_estadisticas_globales() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Obtiene métricas globales de todas las actividades excluyendo RAPs.
    
    Returns:
        Dict: Estadísticas organizadas por tipo y nombre de actividad
    """
    driver: Driver = obtener_driver()
    
    cypher = """
    // Estadísticas de duración por actividad (EXCLUYENDO RAPs)
    MATCH (a:Alumno)-[r:Intento|Completado|Perfecto]->(act)
    WHERE r.duration_seconds IS NOT NULL AND r.duration_seconds > 0
    AND NOT 'RAP' IN labels(act)  // EXCLUIMOS RAPs
    WITH labels(act)[0] as tipo_actividad, act.nombre as nombre_actividad,
         r.duration_seconds as duracion
    RETURN 
        tipo_actividad,
        nombre_actividad,
        COUNT(duracion) as total_intentos,
        AVG(duracion) as duracion_promedio_segundos,
        MIN(duracion) as duracion_minima_segundos,
        MAX(duracion) as duracion_maxima_segundos
    ORDER BY tipo_actividad, nombre_actividad
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher)
            estadisticas: Dict[str, Dict[str, Dict[str, Any]]] = {}
            for record in result:
                tipo: str = record["tipo_actividad"]
                nombre: str = record["nombre_actividad"]
                
                if tipo not in estadisticas:
                    estadisticas[tipo] = {}
                
                estadisticas[tipo][nombre] = {
                    "total_intentos": record["total_intentos"],
                    "duracion_promedio": record["duracion_promedio_segundos"],
                    "duracion_minima": record["duracion_minima_segundos"],
                    "duracion_maxima": record["duracion_maxima_segundos"]
                }
            return estadisticas
    finally:
        driver.close()


def fetch_estadisticas_alumno(correo: str) -> Dict[str, Any]:
    """
    Obtiene análisis detallado del progreso de un alumno excluyendo RAPs.
    
    Args:
        correo: Correo del alumno
        
    Returns:
        Dict: Estadísticas detalladas con resumen y datos por actividad
    """
    driver: Driver = obtener_driver()
    
    cypher = """
    // Estadísticas avanzadas EXCLUYENDO RAPs
    MATCH (a:Alumno {correo: $correo})-[r:Intento|Completado|Perfecto]->(act)
    WHERE r.duration_seconds IS NOT NULL AND r.duration_seconds > 0
    AND NOT 'RAP' IN labels(act)  // EXCLUIMOS RAPs
    WITH labels(act)[0] as tipo_actividad, act.nombre as nombre_actividad,
         r.duration_seconds as duracion, type(r) as estado,
         r.score as puntaje
    RETURN 
        tipo_actividad,
        nombre_actividad,
        estado,
        duracion,
        puntaje
    ORDER BY tipo_actividad, nombre_actividad, duracion
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher, correo=correo)
            
            actividades_dict: Dict[str, Dict[str, Any]] = {}
            resumen_dict: Dict[str, Any] = {
                "total_actividades": 0,
                "total_tiempo_segundos": 0,
                "actividades_con_tiempo": 0
            }
            
            for record in result:
                tipo: str = record["tipo_actividad"]
                nombre: str = record["nombre_actividad"]
                clave: str = f"{tipo}_{nombre}"
                
                if clave not in actividades_dict:
                    actividades_dict[clave] = {
                        "tipo": tipo,
                        "nombre": nombre,
                        "intentos": [],
                        "mejor_puntaje": 0,
                        "estado_final": ""
                    }
                
                actividad: Dict[str, Any] = actividades_dict[clave]
                intento_data: Dict[str, Any] = {
                    "estado": record["estado"],
                    "duracion_segundos": record["duracion"],
                    "puntaje": record["puntaje"] or 0
                }
                actividad["intentos"].append(intento_data)
                
                puntaje_actual: Any = record["puntaje"]
                if puntaje_actual and puntaje_actual > actividad["mejor_puntaje"]:
                    actividad["mejor_puntaje"] = puntaje_actual
                    actividad["estado_final"] = record["estado"]
                
                duracion_actual: Any = record["duracion"]
                if duracion_actual:
                    resumen_dict["total_tiempo_segundos"] += duracion_actual
                    resumen_dict["actividades_con_tiempo"] += 1
            
            resumen_dict["total_actividades"] = len(actividades_dict)
            
            return {
                "actividades": actividades_dict,
                "resumen": resumen_dict
            }
    finally:
        driver.close()


def fetch_verificar_alumno_perfecto(correo: str) -> bool:
    """
    Verifica si un alumno tiene todas sus actividades en estado Perfecto.
    Excluye actividades RAP del análisis.
    
    Args:
        correo: Correo del alumno
        
    Returns:
        bool: True si todas las actividades están en estado Perfecto
    """
    driver: Driver = obtener_driver()
    
    cypher = """
    // Verificación EXCLUYENDO RAPs
    MATCH (a:Alumno {correo: $correo})-[r]->(act)
    WHERE type(r) IN ["Intento","Completado","Perfecto"]
    AND NOT 'RAP' IN labels(act)  // EXCLUIMOS RAPs
    WITH 
        COUNT(r) as total_actividades,
        COUNT(CASE WHEN type(r) = "Perfecto" THEN 1 END) as total_perfectos
    RETURN total_actividades = total_perfectos as todo_perfecto
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher, correo=correo)
            record = result.single()
            return record["todo_perfecto"] if record else False
    finally:
        driver.close()


def fetch_actividades_lentas_alumno(correo: str) -> List[Dict[str, Any]]:
    """
    Identifica actividades donde el alumno es significativamente más lento
    que el promedio global, excluyendo RAPs.
    
    Args:
        correo: Correo del alumno
        
    Returns:
        List[Dict[str, Any]]: Top 10 actividades más lentas con métricas comparativas
    """
    driver: Driver = obtener_driver()
    
    cypher = """
    // Obtener estadísticas del alumno vs globales (EXCLUYENDO RAPs)
    MATCH (a:Alumno {correo: $correo})-[r:Intento|Completado|Perfecto]->(act)
    WHERE r.duration_seconds IS NOT NULL 
    AND r.duration_seconds > 0
    AND NOT 'RAP' IN labels(act)
    
    WITH labels(act)[0] as tipo, act.nombre as nombre_actividad,
         AVG(r.duration_seconds) as tiempo_promedio_alumno,
         COUNT(r) as intentos_alumno
    
    // Obtener estadísticas globales para comparar
    MATCH (global_alumno:Alumno)-[r_global:Intento|Completado|Perfecto]->(act_global)
    WHERE labels(act_global)[0] = tipo 
    AND act_global.nombre = nombre_actividad
    AND r_global.duration_seconds IS NOT NULL
    AND r_global.duration_seconds > 0
    AND NOT 'RAP' IN labels(act_global)
    
    WITH tipo, nombre_actividad, tiempo_promedio_alumno, intentos_alumno,
         AVG(r_global.duration_seconds) as tiempo_promedio_global,
         COUNT(r_global) as intentos_global
    
    WHERE intentos_global >= 3  // Solo considerar actividades con suficiente data global
    AND tiempo_promedio_alumno > tiempo_promedio_global  // Solo donde el alumno es más lento
    
    RETURN 
        tipo,
        nombre_actividad,
        tiempo_promedio_alumno,
        tiempo_promedio_global,
        intentos_alumno,
        intentos_global,
        ((tiempo_promedio_alumno - tiempo_promedio_global) / tiempo_promedio_global) * 100 as diferencia_porcentual
    
    ORDER BY diferencia_porcentual DESC  // Más lentas primero
    LIMIT 10  // Top 10 actividades más lentas
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher, correo=correo)
            actividades_lentas: List[Dict[str, Any]] = []
            
            for record in result:
                actividad: Dict[str, Any] = {
                    "tipo": record["tipo"],
                    "nombre": record["nombre_actividad"],
                    "tiempo_promedio_alumno": record["tiempo_promedio_alumno"],
                    "tiempo_promedio_global": record["tiempo_promedio_global"],
                    "intentos_alumno": record["intentos_alumno"],
                    "intentos_global": record["intentos_global"],
                    "diferencia_porcentual": record["diferencia_porcentual"],
                    "eficiencia": "MUY_LENTO" if record["diferencia_porcentual"] > 30 else "LENTO"
                }
                actividades_lentas.append(actividad)
            
            return actividades_lentas
    finally:
        driver.close()

# ============================================================================
# FUNCIONES DE ESTADÍSTICAS DE PARALELO
# ============================================================================

def fetch_paralelos_disponibles() -> List[Dict[str, str]]:
    """
    Obtiene la lista de todos los paralelos disponibles en la base de datos.
    
    Consulta todos los valores únicos de la propiedad 'paralelo' en los nodos Alumno,
    excluyendo valores nulos o vacíos y ordenando alfabéticamente.
    
    Returns:
        List[Dict[str, str]]: Lista de paralelos con su nombre
        Ejemplo: [{"paralelo": "Paralelo_1"}, {"paralelo": "Paralelo_2"}, ...]
        
    Example:
        >>> paralelos = fetch_paralelos_disponibles()
        >>> print([p["paralelo"] for p in paralelos])
        ['Paralelo_1', 'Paralelo_2', 'Paralelo_3']
    """
    driver: Driver = obtener_driver()
    cypher = """
    MATCH (a:Alumno)
    WHERE a.paralelo IS NOT NULL AND a.paralelo <> ""
    RETURN DISTINCT a.paralelo AS paralelo
    ORDER BY a.paralelo
    """
    try:
        with driver.session() as session:
            result = session.run(cypher)
            paralelos: List[Dict[str, str]] = [
                {"paralelo": str(record["paralelo"])}
                for record in result
                if record.get("paralelo")
            ]
            return paralelos
    finally:
        driver.close()


def fetch_estadisticas_completitud_paralelo(paralelo: str) -> Dict[str, Any]:
    """
    Obtiene métricas de completitud global para un paralelo específico.
    
    Calcula:
    - Total de actividades disponibles (excluyendo RAPs)
    - Número de actividades completadas por todos los alumnos del paralelo
    - Promedio de actividades completadas por alumno
    - Porcentaje de completitud global
    
    Args:
        paralelo: Nombre del paralelo a analizar
        
    Returns:
        Dict[str, Any]: Estadísticas de completitud con:
            - total_actividades: int
            - actividades_completadas_todos: int  
            - promedio_completadas_por_alumno: float
            - porcentaje_completitud_global: float
            - total_alumnos: int
            
    Example:
        >>> stats = fetch_estadisticas_completitud_paralelo("Paralelo_1")
        >>> print(f"Completitud: {stats['porcentaje_completitud_global']:.1f}%")
        Completitud: 75.5%
    """
    driver: Driver = obtener_driver()
    cypher = """
    // Contar total de alumnos en el paralelo
    MATCH (a:Alumno {paralelo: $paralelo})
    WITH count(a) as total_alumnos
    
    // Obtener todas las actividades disponibles (excluyendo RAPs)
    MATCH (act:Cuestionario|Ayudantia)
    WHERE NOT 'RAP' IN labels(act)
    WITH total_alumnos, collect(act) as todas_actividades, count(act) as total_actividades  // ✅ CORREGIDO

    // Para cada actividad, contar cuántos alumnos la han completado
    UNWIND todas_actividades as actividad
    OPTIONAL MATCH (a:Alumno {paralelo: $paralelo})-[:Completado|Perfecto]->(actividad)
    WITH 
        total_alumnos,
        total_actividades,  // ✅ USAMOS EL COUNT CORRECTO
        actividad,
        count(a) as alumnos_completados
    
    // Calcular métricas
    RETURN 
        total_actividades,
        total_alumnos,
        count(CASE WHEN alumnos_completados = total_alumnos THEN 1 END) as actividades_completadas_todos,
        avg(alumnos_completados) as promedio_completadas_por_alumno,
        (sum(alumnos_completados) * 100.0) / (total_actividades * total_alumnos) as porcentaje_completitud_global
    """
    try:
        with driver.session() as session:
            result = session.run(cypher, paralelo=paralelo)
            record = result.single()
            
            if not record:
                return {
                    "total_actividades": 0,
                    "actividades_completadas_todos": 0,
                    "promedio_completadas_por_alumno": 0.0,
                    "porcentaje_completitud_global": 0.0,
                    "total_alumnos": 0
                }
            
            return {
                "total_actividades": record["total_actividades"] or 0,
                "actividades_completadas_todos": record["actividades_completadas_todos"] or 0,
                "promedio_completadas_por_alumno": float(record["promedio_completadas_por_alumno"] or 0),
                "porcentaje_completitud_global": float(record["porcentaje_completitud_global"] or 0),
                "total_alumnos": record["total_alumnos"] or 0
            }
    finally:
        driver.close()


def fetch_actividades_baja_participacion(paralelo: str, umbral_participacion: float = 0.5) -> List[Dict[str, Any]]:
    """
    Identifica actividades con baja participación en un paralelo.
    
    Una actividad tiene baja participación si menos del umbral especificado
    de alumnos tiene al menos un estado 'Completado' o 'Perfecto'.
    
    Args:
        paralelo: Nombre del paralelo a analizar
        umbral_participacion: Umbral de participación (0.5 = 50% por defecto)
        
    Returns:
        List[Dict[str, Any]]: Lista de actividades con baja participación con:
            - tipo: str (Cuestionario/Ayudantia)
            - nombre: str
            - alumnos_completados: int
            - total_alumnos: int
            - porcentaje_participacion: float
            - critico: bool (si participación < 25%)
            
    Example:
        >>> bajas = fetch_actividades_baja_participacion("Paralelo_1", 0.5)
        >>> print(f"Actividades críticas: {len([a for a in bajas if a['critico']])}")
        Actividades críticas: 3
    """
    driver: Driver = obtener_driver()
    cypher = """
    // Contar total de alumnos en el paralelo
    MATCH (a:Alumno {paralelo: $paralelo})
    WITH count(a) as total_alumnos
    
    // Para cada actividad, contar alumnos que la han completado
    MATCH (act:Cuestionario|Ayudantia)
    WHERE NOT 'RAP' IN labels(act)
    OPTIONAL MATCH (alumno:Alumno {paralelo: $paralelo})-[:Completado|Perfecto]->(act)
    
    WITH 
        total_alumnos,
        labels(act)[0] as tipo,
        act.nombre as nombre,
        count(alumno) as alumnos_completados,
        (count(alumno) * 100.0 / total_alumnos) as porcentaje_participacion
    
    WHERE porcentaje_participacion < $umbral_porcentaje
    
    RETURN 
        tipo,
        nombre,
        alumnos_completados,
        total_alumnos,
        porcentaje_participacion
    
    ORDER BY porcentaje_participacion ASC
    """
    try:
        with driver.session() as session:
            # Convertir umbral a porcentaje
            umbral_porcentaje = umbral_participacion * 100
            result = session.run(cypher, paralelo=paralelo, umbral_porcentaje=umbral_porcentaje)
            
            actividades: List[Dict[str, Any]] = []
            for record in result:
                porcentaje = float(record["porcentaje_participacion"] or 0)
                actividad: Dict[str, Any] = {
                    "tipo": str(record["tipo"]),
                    "nombre": str(record["nombre"]),
                    "alumnos_completados": int(record["alumnos_completados"] or 0),
                    "total_alumnos": int(record["total_alumnos"] or 0),
                    "porcentaje_participacion": porcentaje,
                    "critico": porcentaje < 25.0  # Menos del 25% es crítico
                }
                actividades.append(actividad)
            
            return actividades
    finally:
        driver.close()


def fetch_actividades_eficiencia_paralelo(paralelo: str, top_n: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    """
    Analiza la eficiencia de actividades en un paralelo y retorna las mejores y peores.
    
    La eficiencia se calcula como:
    (Perfectos + Completados) / Total Alumnos * 100
    
    Args:
        paralelo: Nombre del paralelo a analizar
        top_n: Número de actividades a retornar en cada categoría (default: 3)
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Diccionario con:
            - "mejores": Lista de actividades con mayor eficiencia
            - "peores": Lista de actividades con menor eficiencia
        Cada actividad incluye:
            - tipo: str
            - nombre: str  
            - eficiencia: float
            - total_perfectos: int
            - total_completados: int
            - total_alumnos: int
            
    Example:
        >>> eficiencia = fetch_actividades_eficiencia_paralelo("Paralelo_1")
        >>> print(f"Mejor actividad: {eficiencia['mejores'][0]['nombre']}")
        Mejor actividad: Cuestionario_1
    """
    driver: Driver = obtener_driver()
    cypher = """
    // Contar total de alumnos en el paralelo
    MATCH (a:Alumno {paralelo: $paralelo})
    WITH count(a) as total_alumnos
    
    // Para cada actividad, contar estados
    MATCH (act:Cuestionario|Ayudantia)
    WHERE NOT 'RAP' IN labels(act)
    OPTIONAL MATCH (alumno:Alumno {paralelo: $paralelo})-[r]->(act)
    WHERE type(r) IN ["Completado", "Perfecto"]
    
    WITH 
        total_alumnos,
        labels(act)[0] as tipo,
        act.nombre as nombre,
        count(CASE WHEN type(r) = "Perfecto" THEN 1 END) as total_perfectos,
        count(CASE WHEN type(r) = "Completado" THEN 1 END) as total_completados,
        count(r) as total_relaciones_exitosas
    
    // Calcular eficiencia
    WITH 
        tipo, nombre, total_perfectos, total_completados, total_alumnos,
        (total_relaciones_exitosas * 100.0 / total_alumnos) as eficiencia
    
    RETURN 
        tipo,
        nombre,
        eficiencia,
        total_perfectos,
        total_completados,
        total_alumnos
    
    ORDER BY eficiencia DESC
    """
    try:
        with driver.session() as session:
            result = session.run(cypher, paralelo=paralelo)
            todas_actividades: List[Dict[str, Any]] = []
            
            for record in result:
                actividad: Dict[str, Any] = {
                    "tipo": str(record["tipo"]),
                    "nombre": str(record["nombre"]),
                    "eficiencia": float(record["eficiencia"] or 0),
                    "total_perfectos": int(record["total_perfectos"] or 0),
                    "total_completados": int(record["total_completados"] or 0),
                    "total_alumnos": int(record["total_alumnos"] or 0)
                }
                todas_actividades.append(actividad)
            
            # Separar en mejores y peores
            mejores = todas_actividades[:top_n]
            peores = todas_actividades[-top_n:] if len(todas_actividades) > top_n else todas_actividades
            peores.reverse()  # Para que las peores aparezcan de mayor a menor problema
            
            return {
                "mejores": mejores,
                "peores": peores
            }
    finally:
        driver.close()


def fetch_detalle_paralelo(paralelo: str) -> Dict[str, Any]:
    """
    Obtiene un análisis completo y consolidado de un paralelo.
    
    Combina todas las métricas en un solo diccionario para facilitar
    la presentación en el sistema.
    
    Args:
        paralelo: Nombre del paralelo a analizar
        
    Returns:
        Dict[str, Any]: Diccionario consolidado con todas las estadísticas:
            - info_general: Dict con totales
            - completitud: Dict con métricas de completitud
            - baja_participacion: List de actividades problemáticas
            - eficiencia: Dict con mejores y peores actividades
            
    Example:
        >>> detalle = fetch_detalle_paralelo("Paralelo_1")
        >>> print(f"Alumnos: {detalle['info_general']['total_alumnos']}")
        Alumnos: 45
    """
    # Obtener todas las métricas
    completitud = fetch_estadisticas_completitud_paralelo(paralelo)
    baja_participacion = fetch_actividades_baja_participacion(paralelo)
    eficiencia = fetch_actividades_eficiencia_paralelo(paralelo)
    
    # Consolidar información general
    info_general: Dict[str, Any] = {
        "total_alumnos": completitud["total_alumnos"],
        "total_actividades": completitud["total_actividades"],
        "actividades_criticas": len([a for a in baja_participacion if a["critico"]]),
        "actividades_baja_participacion": len(baja_participacion)
    }
    
    return {
        "info_general": info_general,
        "completitud": completitud,
        "baja_participacion": baja_participacion,
        "eficiencia": eficiencia
    }

__all__ = [
    'fetch_alumnos',
    'fetch_progreso_alumno', 
    'fetch_siguiente_actividad',
    'fetch_siguiente_actividad_mejorada',
    'fetch_siguiente_actividad_simple',
    'fetch_estadisticas_globales',
    'fetch_estadisticas_alumno',
    'fetch_verificar_alumno_perfecto',
    'fetch_actividades_lentas_alumno',
    # Nuevas funciones de estadísticas de paralelo
    'fetch_paralelos_disponibles',
    'fetch_estadisticas_completitud_paralelo', 
    'fetch_actividades_baja_participacion',
    'fetch_actividades_eficiencia_paralelo',
    'fetch_detalle_paralelo'
]