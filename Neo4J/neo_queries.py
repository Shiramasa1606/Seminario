# Neo4J/neo_queries.py
from typing import List, Dict, Any, Optional
from neo4j import Driver
from Neo4J.conn import obtener_driver

def fetch_alumnos() -> List[Dict[str, str]]:
    driver: Driver = obtener_driver()
    cypher: str = """
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
    driver: Driver = obtener_driver()
    cypher: str = """
    MATCH (a:Alumno {correo: $correo})-[r]->(act)
    WHERE type(r) IN ["Intento","Completado","Perfecto"]
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

def fetch_siguiente_por_avance_mejorada(correo: str) -> Optional[Dict[str, Any]]:
    """
    VERSIÓN MEJORADA - Encuentra siguiente actividad de forma más flexible
    Los RAPs siempre están disponibles para recomendación
    """
    driver: Driver = obtener_driver()
    
    cypher: str = """
    // ESTRATEGIA MEJORADA - Los RAPs siempre disponibles para roadmap
    
    MATCH (a:Alumno {correo: $correo})
    
    // 1. Buscar última actividad del alumno (excluyendo RAPs para determinar unidad)
    OPTIONAL MATCH (a)-[r:Intento|Completado|Perfecto]->(ultima_act)
    WHERE NOT 'RAP' IN labels(ultima_act)
    WITH a, ultima_act
    ORDER BY r.end DESC
    LIMIT 1
    
    // 2. Si hay última actividad, buscar su unidad
    OPTIONAL MATCH (unidad_ultima:Unidad)-[:TIENE_RAP|TIENE_CUESTIONARIO|TIENE_AYUDANTIA]->(ultima_act)
    
    // 3. Buscar actividades en esta unidad (RAPs siempre disponibles)
    OPTIONAL MATCH (unidad_ultima)-[:TIENE_RAP|TIENE_CUESTIONARIO|TIENE_AYUDANTIA]->(siguiente_misma_unidad)
    WHERE siguiente_misma_unidad IS NOT NULL 
      AND (
        // RAPs: siempre disponibles para el roadmap
        'RAP' IN labels(siguiente_misma_unidad)
        OR 
        // Cuestionarios/Ayudantias: solo si no están completados/perfectos
        (NOT 'RAP' IN labels(siguiente_misma_unidad) 
         AND NOT (a)-[:Completado|Perfecto]->(siguiente_misma_unidad))
      )
    
    // 4. Si no hay en la misma unidad, buscar en cualquier unidad
    OPTIONAL MATCH (cualquier_unidad:Unidad)-[:TIENE_RAP|TIENE_CUESTIONARIO|TIENE_AYUDANTIA]->(siguiente_cualquier)
    WHERE siguiente_cualquier IS NOT NULL 
      AND (
        // RAPs: siempre disponibles
        'RAP' IN labels(siguiente_cualquier)
        OR 
        // Cuestionarios/Ayudantias: solo si no están completados/perfectos
        (NOT 'RAP' IN labels(siguiente_cualquier) 
         AND NOT (a)-[:Completado|Perfecto]->(siguiente_cualquier))
      )
    
    WITH 
        siguiente_misma_unidad AS candidato1,
        siguiente_cualquier AS candidato2,
        CASE 
            WHEN siguiente_misma_unidad IS NOT NULL THEN 1
            ELSE 2
        END AS prioridad
    
    // 5. Elegir el mejor candidato y priorizar por tipo (RAPs primero)
    WITH coalesce(candidato1, candidato2) AS siguiente, prioridad
    
    WHERE siguiente IS NOT NULL
    
    RETURN 
        labels(siguiente) AS labels, 
        siguiente.nombre AS nombre,
        prioridad,
        CASE 
            WHEN 'RAP' IN labels(siguiente) THEN 1
            WHEN 'Cuestionario' IN labels(siguiente) THEN 2
            WHEN 'Ayudantia' IN labels(siguiente) THEN 3
            ELSE 4
        END AS prioridad_tipo
    ORDER BY prioridad, prioridad_tipo
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
                "prioridad": record.get("prioridad"),
                "prioridad_tipo": record.get("prioridad_tipo")
            }
    finally:
        driver.close()

def fetch_siguiente_actividad_simple(correo: str) -> Optional[Dict[str, Any]]:
    """
    VERSIÓN SIMPLE - Para cuando falla la query compleja
    Los RAPs siempre disponibles para roadmap
    """
    driver: Driver = obtener_driver()
    
    cypher: str = """
    // QUERY SIMPLE: RAPs siempre disponibles, otros solo si no completados
    MATCH (a:Alumno {correo: $correo})
    MATCH (siguiente:RAP|Cuestionario|Ayudantia)
    WHERE 
        'RAP' IN labels(siguiente)
        OR 
        (NOT 'RAP' IN labels(siguiente) 
         AND NOT (a)-[:Completado|Perfecto]->(siguiente))
    
    WITH siguiente,
         CASE 
           WHEN 'RAP' IN labels(siguiente) THEN 1
           WHEN 'Cuestionario' IN labels(siguiente) THEN 2
           WHEN 'Ayudantia' IN labels(siguiente) THEN 3
           ELSE 4
         END AS prioridad
    
    RETURN labels(siguiente) AS labels, siguiente.nombre AS nombre
    ORDER BY prioridad, siguiente.nombre
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

# Mantener la original por compatibilidad
def fetch_siguiente_por_avance(correo: str) -> Optional[Dict[str, Any]]:
    """Versión original (puede ser eliminada luego)"""
    return fetch_siguiente_actividad_simple(correo)

# Neo4J/consultar.py
from typing import List, Any, Optional, Dict, Callable, cast


# Define type aliases for better clarity
ActivityDict = Dict[str, Any]
ProgressItem = Dict[str, Any]
RecommendationResult = Optional[Dict[str, Any]]
FetchNextFunction = Callable[[], Optional[ActivityDict]]


def recomendar_siguiente_from_progress(progreso: List[ProgressItem]) -> RecommendationResult:
    """
    Recibe la lista de progreso (cada item con 'tipo','nombre','estado',...) y devuelve:
      {"estrategia": "refuerzo|mejora|avance", "actividad": {...}}
    Reglas:
      - Si hay 'Intento' -> refuerzo: sugerir repetir ese recurso (primero encontrado)
      - Else si hay 'Completado' -> mejora: sugerir mejorar (primero encontrado)
      - Else si hay 'Perfecto' -> avance: devuelve None (quedan que consultar con Neo4J para next)
    """
    if not progreso:
        return None

    # Excluir RAPs de la lógica de refuerzo/mejora
    progreso_filtrado = [p for p in progreso if p.get("tipo") != "RAP"]
    
    if not progreso_filtrado:
        return None

    intentos = [p for p in progreso_filtrado if p.get("estado") == "Intento"]
    if intentos:
        return {"estrategia": "refuerzo", "actividad": intentos[0]}

    completados = [p for p in progreso_filtrado if p.get("estado") == "Completado"]
    if completados:
        return {"estrategia": "mejora", "actividad": completados[0]}

    perfectos = [p for p in progreso_filtrado if p.get("estado") == "Perfecto"]
    if perfectos:
        # Para 'avance' devolvemos la señal; la resolución del siguiente recurso
        # (buscar en el grafo) la hace el módulo de consultas Neo4J.
        return {"estrategia": "avance", "actividad": perfectos[0]}

    return None


def generar_roadmap_from_progress_and_fetcher(
    progreso: List[ProgressItem],
    fetch_next_for_avance: FetchNextFunction
) -> List[Dict[str, Any]]:
    """
    Genera un roadmap en memoria a partir del progreso inicial.
    `fetch_next_for_avance` es una función que devuelve next_activity_dict o None
    que se invoca cuando la estrategia es 'avance' y necesitamos consultar el grafo
    para obtener la siguiente actividad disponible.
    NOTA: No modifica la DB.
    """
    roadmap: List[Dict[str, Any]] = []
    seen: set[tuple[Optional[str], Optional[str], str]] = set()

    # copia en memoria para simular progresos que vamos cambiando
    # Incluimos RAPs en el roadmap pero con lógica diferente
    prog_map: Dict[tuple[Optional[str], Optional[str]], ActivityDict] = {}
    for p in progreso:
        key = (p.get("tipo"), p.get("nombre"))
        prog_map[key] = p

    while True:
        rec = recomendar_siguiente_from_progress(list(prog_map.values()))
        if not rec:
            break

        estrategia = rec["estrategia"]
        # Use cast to help Pylance understand the type
        actividad = cast(ActivityDict, rec["actividad"])

        # Si avance, necesitarás resolver la siguiente actividad en el grafo
        if estrategia == "avance":
            # fetch_next_for_avance debe devolver {"tipo":.., "nombre":..} o None
            siguiente = fetch_next_for_avance()
            if not siguiente:
                break
            # usar siguiente como la actividad a añadir
            actividad = siguiente

        # Extract variables to help with type inference
        act_tipo = actividad.get("tipo")
        act_nombre = actividad.get("nombre")
        act_key = (act_tipo, act_nombre, estrategia)
        
        if act_key in seen:
            break
        seen.add(act_key)
        roadmap.append({"estrategia": estrategia, "actividad": actividad})

        # simular avance en prog_map (para RAPs no cambiamos el estado)
        prog_key = (act_tipo, act_nombre)
        if prog_key in prog_map:
            if act_tipo != "RAP":  # Solo actualizamos estado para no-RAPs
                if estrategia == "refuerzo":
                    prog_map[prog_key]["estado"] = "Completado"
                elif estrategia == "mejora":
                    prog_map[prog_key]["estado"] = "Perfecto"
                else:
                    prog_map[prog_key]["estado"] = "Perfecto"
        else:
            # si no existía, añadir (para RAPs mantenemos estado neutro)
            nuevo_estado = "Completado" if act_tipo != "RAP" else "Visto"
            prog_map[prog_key] = {
                "tipo": act_tipo, 
                "nombre": act_nombre, 
                "estado": nuevo_estado
            }

    return roadmap

# NUEVAS FUNCIONES CON TIPOS CORREGIDOS - Agregar al final de neo_queries.py

def fetch_estadisticas_globales_actividades() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Obtiene estadísticas globales de todas las actividades con tiempos
    EXCLUYENDO RAPs de las estadísticas
    """
    driver: Driver = obtener_driver()
    
    cypher: str = """
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

def fetch_estadisticas_alumno_avanzadas(correo: str) -> Dict[str, Any]:
    """
    Obtiene estadísticas avanzadas de un alumno específico con tiempos
    EXCLUYENDO RAPs de las estadísticas
    """
    driver: Driver = obtener_driver()
    
    cypher: str = """
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
            
            # Definir tipos explícitos
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
                
                # Actualizar mejor puntaje y estado final
                puntaje_actual: Any = record["puntaje"]
                if puntaje_actual and puntaje_actual > actividad["mejor_puntaje"]:
                    actividad["mejor_puntaje"] = puntaje_actual
                    actividad["estado_final"] = record["estado"]
                
                # Estadísticas generales
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

def verificar_alumno_todo_perfecto(correo: str) -> bool:
    """
    Verifica si un alumno tiene todas sus actividades en estado Perfecto
    EXCLUYENDO RAPs de la verificación
    """
    driver: Driver = obtener_driver()
    
    cypher: str = """
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