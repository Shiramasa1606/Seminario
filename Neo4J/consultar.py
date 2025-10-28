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
    EXCLUYE RAPs de la lógica de refuerzo/mejora
    """
    if not progreso:
        return None

    # EXCLUIR RAPs de la lógica de refuerzo/mejora - solo Cuestionarios y Ayudantías
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
    LOS RAPs PUEDEN APARECER EN EL ROADMAP PERO NO AFECTAN EL ESTADO DE PROGRESO
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


# CORRECCIÓN: Versión simplificada sin problemas de tipos
def generar_roadmap_para_alumno(
    correo: str,
    fetch_next_func: FetchNextFunction
) -> List[Dict[str, Any]]:
    """
    Función conveniente para generar roadmap para un alumno específico
    LOS RAPs PUEDEN APARECER EN EL ROADMAP PERO NO AFECTAN EL ESTADO DE PROGRESO
    """
    from Neo4J.neo_queries import fetch_progreso_alumno
    
    # Obtener progreso del alumno - esto devuelve List[Dict[str, Any]]
    progreso = fetch_progreso_alumno(correo)
    
    # Asegurarnos de que tenemos una lista válida
    # NOTA: fetch_progreso_alumno() siempre devuelve lista, nunca None
    if not progreso:
        progreso = []
    
    return generar_roadmap_from_progress_and_fetcher(progreso, fetch_next_func)

# NUEVAS FUNCIONES PARA ANÁLISIS COMPARATIVO - Agregar al final de consultar.py

def analizar_rendimiento_comparativo(correo: str) -> Dict[str, Any]:
    """
    Analiza el rendimiento del alumno comparado con las estadísticas globales
    EXCLUYE RAPs del análisis comparativo
    """
    from Neo4J.neo_queries import (
        fetch_estadisticas_globales_actividades, 
        fetch_estadisticas_alumno_avanzadas,
        verificar_alumno_todo_perfecto
    )
    
    # Verificar si el alumno tiene todo perfecto (EXCLUYENDO RAPs)
    if not verificar_alumno_todo_perfecto(correo):
        return {"error": "El alumno no tiene todas las actividades en estado Perfecto"}
    
    print("📊 Obteniendo datos para análisis comparativo...")
    stats_globales = fetch_estadisticas_globales_actividades()
    stats_alumno = fetch_estadisticas_alumno_avanzadas(correo)
    
    # Filtrar actividades del alumno para excluir RAPs
    actividades_alumno_sin_raps = {
        clave: actividad for clave, actividad in stats_alumno["actividades"].items() 
        if actividad.get("tipo") != "RAP"
    }
    
    analisis: Dict[str, Any] = {
        "resumen_general": {
            "total_actividades": len(actividades_alumno_sin_raps),
            "tiempo_total_alumno": stats_alumno["resumen"]["total_tiempo_segundos"],
            "actividades_analizadas": stats_alumno["resumen"]["actividades_con_tiempo"]
        },
        "comparativas": [],
        "insights": {
            "fortalezas": [],
            "areas_mejora": [],
            "recomendaciones": []
        },
        "nota": "⚠️ Análisis excluye RAPs - solo considera Cuestionarios y Ayudantías"
    }
    
    # Analizar cada actividad del alumno (EXCLUYENDO RAPs)
    actividades_analizadas = 0
    
    for actividad_alumno in actividades_alumno_sin_raps.values():
        tipo: str = actividad_alumno["tipo"]
        nombre: str = actividad_alumno["nombre"]
        
        # Solo analizar actividades con tiempo registrado
        duraciones_alumno = [i["duracion_segundos"] for i in actividad_alumno["intentos"] if i["duracion_segundos"]]
        if not duraciones_alumno:
            continue
            
        actividades_analizadas += 1
        duracion_promedio_alumno: float = sum(duraciones_alumno) / len(duraciones_alumno)
        duracion_mejor_alumno: float = min(duraciones_alumno)  # Mejor tiempo = más eficiente
        
        comparativa: Dict[str, Any] = {
            "actividad": nombre,
            "tipo": tipo,
            "duracion_promedio_alumno": duracion_promedio_alumno,
            "duracion_mejor_alumno": duracion_mejor_alumno,
            "total_intentos": len(actividad_alumno["intentos"]),
            "puntaje_final": actividad_alumno["mejor_puntaje"]
        }
        
        # Comparar con estadísticas globales si están disponibles
        if tipo in stats_globales and nombre in stats_globales[tipo]:
            stats_global = stats_globales[tipo][nombre]
            duracion_promedio_global: float = stats_global["duracion_promedio"]
            
            comparativa["duracion_promedio_global"] = duracion_promedio_global
            comparativa["diferencia_promedio"] = duracion_promedio_alumno - duracion_promedio_global
            comparativa["diferencia_porcentual"] = ((duracion_promedio_alumno - duracion_promedio_global) / duracion_promedio_global) * 100 if duracion_promedio_global > 0 else 0
            
            # Categorizar eficiencia
            if comparativa["diferencia_porcentual"] < -25:
                comparativa["eficiencia"] = "MUY_EFICIENTE"
            elif comparativa["diferencia_porcentual"] < -10:
                comparativa["eficiencia"] = "EFICIENTE"
            elif comparativa["diferencia_porcentual"] < 10:
                comparativa["eficiencia"] = "PROMEDIO"
            elif comparativa["diferencia_porcentual"] < 30:
                comparativa["eficiencia"] = "LENTO"
            else:
                comparativa["eficiencia"] = "MUY_LENTO"
        
        analisis["comparativas"].append(comparativa)
    
    # Actualizar contador real de actividades analizadas
    analisis["resumen_general"]["actividades_analizadas"] = actividades_analizadas
    
    # Generar insights basados en el análisis
    if analisis["comparativas"]:
        _generar_insights_comparativos(analisis)
    
    return analisis

def _generar_insights_comparativos(analisis: Dict[str, Any]) -> None:
    """
    Genera insights basados en el análisis comparativo de tiempos
    EXCLUYE RAPs del análisis
    """
    comparativas: List[Dict[str, Any]] = analisis["comparativas"]
    insights: Dict[str, List[str]] = analisis["insights"]
    
    # Identificar fortalezas (actividades muy eficientes)
    actividades_muy_eficientes: List[Dict[str, Any]] = [c for c in comparativas if c.get("eficiencia") == "MUY_EFICIENTE"]
    actividades_eficientes: List[Dict[str, Any]] = [c for c in comparativas if c.get("eficiencia") == "EFICIENTE"]
    actividades_muy_lentas: List[Dict[str, Any]] = [c for c in comparativas if c.get("eficiencia") == "MUY_LENTO"]
    
    # Generar fortalezas
    if actividades_muy_eficientes:
        insights["fortalezas"].append(f"🎯 Eres excepcionalmente rápido en {len(actividades_muy_eficientes)} actividades")
        for act in actividades_muy_eficientes[:2]:  # Mostrar hasta 2 ejemplos
            tiempo_ahorrado: float = -act["diferencia_porcentual"]
            insights["fortalezas"].append(f"   • {act['actividad']}: {tiempo_ahorrado:.1f}% más rápido que el promedio")
    
    if actividades_eficientes:
        insights["fortalezas"].append(f"⚡ Eres eficiente en {len(actividades_eficientes)} actividades")
    
    # Generar áreas de mejora
    if actividades_muy_lentas:
        insights["areas_mejora"].append(f"⏰ Puedes mejorar tu ritmo en {len(actividades_muy_lentas)} actividades")
        for act in actividades_muy_lentas[:2]:
            tiempo_extra: float = act["diferencia_porcentual"]
            insights["areas_mejora"].append(f"   • {act['actividad']}: {tiempo_extra:.1f}% más lento que el promedio")
    
    # Recomendaciones generales
    total_actividades: int = len(comparativas)
    if actividades_muy_lentas:
        porcentaje_lento: float = (len(actividades_muy_lentas) / total_actividades) * 100
        if porcentaje_lento > 50:
            insights["recomendaciones"].append("📚 Enfócate en mejorar tu velocidad general mediante práctica constante")
        else:
            insights["recomendaciones"].append("🎯 Trabaja en las actividades específicas donde puedes ser más eficiente")
    
    if actividades_muy_eficientes:
        insights["recomendaciones"].append("💪 Aprovecha tu velocidad en ciertas áreas para ayudar a compañeros")
    
    # Mensaje de felicitación general
    if len(actividades_muy_eficientes) + len(actividades_eficientes) > len(actividades_muy_lentas):
        insights["recomendaciones"].append("🏆 ¡Excelente rendimiento! Mantén este nivel de excelencia")
    else:
        insights["recomendaciones"].append("🌟 Buen trabajo en alcanzar todos los Perfectos, ahora enfócate en la eficiencia")

def formatear_tiempo_analisis(segundos: float) -> str:
    """Formatea segundos a formato legible para el análisis"""
    if segundos < 60:
        return f"{segundos:.0f} segundos"
    elif segundos < 3600:
        minutos: float = segundos / 60
        return f"{minutos:.1f} minutos"
    else:
        horas: float = segundos / 3600
        return f"{horas:.1f} horas"