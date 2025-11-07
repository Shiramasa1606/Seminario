# Neo4J/consultar.py
from typing import List, Any, Optional, Dict, Callable, cast


# Define type aliases for better clarity
ActivityDict = Dict[str, Any]
ProgressItem = Dict[str, Any]
RecommendationResult = Optional[Dict[str, Any]]
FetchNextFunction = Callable[[], Optional[ActivityDict]]


def recomendar_siguiente_from_progress(progreso: List[ProgressItem]) -> RecommendationResult:
    """
    NUEVA JERARQUÍA DE PRIORIDADES por ORDEN DE ANTIGÜEDAD:
    1. 🚀 NUEVAS ACTIVIDADES (no en progreso) - señal para buscar en Neo4J
    2. 🔄 ACTIVIDADES NO TERMINADAS (Intento) - MÁS ANTIGUA primero
    3. 📈 ACTIVIDADES NO PERFECTAS (Completado) - MÁS ANTIGUA primero
    4. ⏰ REFUERZO DE TIEMPO - MENOS EFICIENTE primero
    
    EXCLUYE RAPs completamente del roadmap
    """
    if not progreso:
        return {"estrategia": "nuevas", "actividad": None}

    # EXCLUIR RAPs completamente - solo Cuestionarios y Ayudantías
    progreso_filtrado = [p for p in progreso if p.get("tipo") != "RAP"]
    
    if not progreso_filtrado:
        return {"estrategia": "nuevas", "actividad": None}

    # 1. Buscar actividades en Intento (no terminadas) - MÁS ANTIGUA primero
    intentos = [p for p in progreso_filtrado if p.get("estado") == "Intento"]
    if intentos:
        # Ordenar por fecha de inicio (más antigua primero)
        intentos_ordenados = sorted(
            intentos, 
            key=lambda x: x.get("start") or "9999-12-31"  # Si no tiene fecha, va al final
        )
        return {"estrategia": "refuerzo", "actividad": intentos_ordenados[0]}

    # 2. Buscar actividades en Completado (no perfectas) - MÁS ANTIGUA primero
    completados = [p for p in progreso_filtrado if p.get("estado") == "Completado"]
    if completados:
        # Ordenar por fecha de inicio (más antigua primero)
        completados_ordenados = sorted(
            completados,
            key=lambda x: x.get("start") or "9999-12-31"  # Si no tiene fecha, va al final
        )
        return {"estrategia": "mejora", "actividad": completados_ordenados[0]}

    # 3. Si todo está Perfecto, buscar nuevas actividades
    return {"estrategia": "nuevas", "actividad": None}

def generar_roadmap_from_progress_and_fetcher(
    progreso: List[ProgressItem],
    fetch_next_for_avance: FetchNextFunction,
    actividades_lentas: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    Genera un roadmap en memoria donde:
    - Cada actividad aparece UNA SOLA VEZ con su categoría más prioritaria
    - Los RAPs están EXCLUIDOS completamente del roadmap
    - Orden de prioridad: NUEVAS → REFUERZO → MEJORA → REFUERZO_TIEMPO
    """
    roadmap: List[Dict[str, Any]] = []
    actividades_vistas: set[tuple[Optional[str], Optional[str]]] = set()
    
    # Copia en memoria para simular progresos (excluyendo RAPs)
    prog_map: Dict[tuple[Optional[str], Optional[str]], ActivityDict] = {}
    for p in progreso:
        if p.get("tipo") != "RAP":  # Excluir RAPs del progreso simulado
            key = (p.get("tipo"), p.get("nombre"))
            prog_map[key] = p

    # Función auxiliar para obtener siguiente actividad no-RAP
    def obtener_siguiente_no_rap() -> Optional[Dict[str, Any]]:
        siguiente = fetch_next_for_avance()
        while siguiente and siguiente.get("tipo") == "RAP":
            siguiente = fetch_next_for_avance()
        return siguiente

    # Función auxiliar para obtener siguiente actividad lenta (menos eficiente primero)
    def obtener_siguiente_lenta() -> Optional[Dict[str, Any]]:
        if not actividades_lentas:
            return None
        # Ordenar por diferencia porcentual (menos eficiente primero)
        actividades_lentas_ordenadas = sorted(
            actividades_lentas,
            key=lambda x: x.get('diferencia_porcentual', 0),
            reverse=True  # Mayor diferencia primero (menos eficiente)
        )
        for act_lenta in actividades_lentas_ordenadas:
            if act_lenta.get("tipo") != "RAP":
                act_key = (act_lenta.get("tipo"), act_lenta.get("nombre"))
                if act_key not in actividades_vistas:
                    return act_lenta
        return None

    # CASO 1: Alumno sin progreso - empezar con primera actividad no-RAP
    if not prog_map:
        siguiente = obtener_siguiente_no_rap()
        if siguiente:
            act_key = (siguiente.get("tipo"), siguiente.get("nombre"))
            actividades_vistas.add(act_key)
            roadmap.append({
                "estrategia": "nuevas", 
                "actividad": siguiente,
                "motivo": "Primera actividad recomendada"
            })
        return roadmap

    # CASO 2: Alumno con progreso - generar roadmap según prioridades
    max_iteraciones = 30
    iteracion = 0
    
    while iteracion < max_iteraciones:
        iteracion += 1
        
        # Obtener recomendación basada en progreso simulado
        rec = recomendar_siguiente_from_progress(list(prog_map.values()))
        if not rec:
            # Si no hay más recomendaciones del progreso, intentar refuerzo de tiempo
            actividad_lenta = obtener_siguiente_lenta()
            if actividad_lenta:
                act_key = (actividad_lenta.get("tipo"), actividad_lenta.get("nombre"))
                actividades_vistas.add(act_key)
                diferencia = actividad_lenta.get('diferencia_porcentual', 0)
                roadmap.append({
                    "estrategia": "refuerzo_tiempo",
                    "actividad": actividad_lenta,
                    "motivo": f"Refuerzo recomendado - Tiempo +{diferencia:.1f}% vs promedio"
                })
                # Continuar para ver si hay más actividades lentas
                continue
            else:
                break  # No hay más actividades de ningún tipo

        estrategia = rec["estrategia"]
        actividad = cast(ActivityDict, rec["actividad"])

        # Manejar estrategia "nuevas" - buscar actividades no realizadas
        if estrategia == "nuevas":
            siguiente = obtener_siguiente_no_rap()
            if not siguiente:
                # Intentar refuerzo de tiempo si no hay actividades nuevas
                actividad_lenta = obtener_siguiente_lenta()
                if actividad_lenta:
                    act_key = (actividad_lenta.get("tipo"), actividad_lenta.get("nombre"))
                    actividades_vistas.add(act_key)
                    diferencia = actividad_lenta.get('diferencia_porcentual', 0)
                    roadmap.append({
                        "estrategia": "refuerzo_tiempo",
                        "actividad": actividad_lenta,
                        "motivo": f"Refuerzo recomendado - Tiempo +{diferencia:.1f}% vs promedio"
                    })
                    # Continuar para ver si hay más actividades lentas
                    continue
                else:
                    break  # No hay más actividades de ningún tipo
            actividad = siguiente

        # Verificar si ya procesamos esta actividad
        act_key = (actividad.get("tipo"), actividad.get("nombre"))
        if act_key in actividades_vistas:
            continue
            
        actividades_vistas.add(act_key)
        
        # Añadir al roadmap con la estrategia correspondiente
        item_roadmap: Dict[str, Any] = {
            "estrategia": estrategia, 
            "actividad": actividad
        }
            
        roadmap.append(item_roadmap)

        # Simular avance en el progreso (solo para actividades no-RAP)
        if actividad.get("tipo") != "RAP":
            if act_key in prog_map:
                # Actualizar estado existente
                if estrategia == "refuerzo":
                    prog_map[act_key]["estado"] = "Completado"
                elif estrategia == "mejora":
                    prog_map[act_key]["estado"] = "Perfecto"
                elif estrategia == "nuevas":
                    prog_map[act_key]["estado"] = "Completado"
            else:
                # Añadir nueva actividad al progreso simulado
                prog_map[act_key] = {
                    "tipo": actividad.get("tipo"), 
                    "nombre": actividad.get("nombre"), 
                    "estado": "Completado"
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