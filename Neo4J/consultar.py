# Neo4J/consultar.py
from typing import List, Any, Optional, Dict, Callable, Tuple


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
    actividades_lentas: Optional[List[ActivityDict]] = None
) -> List[Dict[str, Any]]:
    """
    Genera un roadmap en memoria con JERARQUÍA DE PRIORIDADES SIN LÍMITES:
    1. 🔄 ACTIVIDADES EN INTENTO (no terminadas)
    2. ⏰ ACTIVIDADES PARA MEJORAR TIEMPO (TODAS las identificadas como lentas)
    3. 📈 ACTIVIDADES PARA MEJORAR (Completado → Perfecto) 
    4. 🚀 NUEVAS ACTIVIDADES (no en progreso)
    """
    roadmap: List[Dict[str, Any]] = []
    actividades_vistas: set[Tuple[Optional[str], Optional[str]]] = set()
    
    # Copia en memoria para simular progresos (excluyendo RAPs)
    prog_map: Dict[Tuple[Optional[str], Optional[str]], ActivityDict] = {}
    for p in progreso:
        if p.get("tipo") != "RAP":  # Excluir RAPs del progreso simulado
            key = (p.get("tipo"), p.get("nombre"))
            prog_map[key] = p

    # Preparar actividades por categorías según la jerarquía
    actividades_intento: List[ActivityDict] = []
    actividades_mejora: List[ActivityDict] = []
    actividades_lentas_activas: List[ActivityDict] = []
    
    # Clasificar actividades existentes
    for actividad in prog_map.values():
        estado = actividad.get("estado")
        act_key = (actividad.get("tipo"), actividad.get("nombre"))
        
        if estado == "Intento" and act_key not in actividades_vistas:
            actividades_intento.append(actividad)
        elif estado == "Completado" and act_key not in actividades_vistas:
            actividades_mejora.append(actividad)
    
    # PREPARAR ACTIVIDADES LENTAS - INCLUIR TODAS LAS IDENTIFICADAS
    if actividades_lentas:
        print(f"🔍 Procesando {len(actividades_lentas)} actividades lentas identificadas...")
        procesadas = 0
        for act_lenta in actividades_lentas:
            act_tipo: Optional[str] = act_lenta.get("tipo")
            act_nombre: Optional[str] = act_lenta.get("nombre")
            act_key = (act_tipo, act_nombre)
            
            # INCLUIR actividades lentas que existen en el progreso y no están en el roadmap
            if act_key in prog_map and act_key not in actividades_vistas:
                # Combinar datos del progreso con análisis de tiempo
                actividad_combinada = {**prog_map[act_key], **act_lenta}
                actividades_lentas_activas.append(actividad_combinada)
                procesadas += 1
        print(f"🔍 Se agregaron {procesadas} actividades lentas al roadmap.")
        
        # Ordenar por diferencia porcentual (más lentas primero)
        actividades_lentas_activas.sort(
            key=lambda x: x.get('diferencia_porcentual', 0), 
            reverse=True
        )
        print(f"   📊 Actividades lentas válidas para roadmap: {len(actividades_lentas_activas)}")

    # Función auxiliar para obtener siguiente actividad no-RAP
    def obtener_siguiente_no_rap() -> Optional[ActivityDict]:
        siguiente = fetch_next_for_avance()
        while siguiente and siguiente.get("tipo") == "RAP":
            siguiente = fetch_next_for_avance()
        return siguiente

    # ========== JERARQUÍA DE PRIORIDADES SIN LÍMITES ==========
    
    # 1. 🔄 ACTIVIDADES EN INTENTO (prioridad máxima) - TODAS
    for actividad in actividades_intento:
        act_tipo = actividad.get("tipo")
        act_nombre = actividad.get("nombre")
        act_key = (act_tipo, act_nombre)
        
        if act_key not in actividades_vistas:
            actividades_vistas.add(act_key)
            roadmap.append({
                "estrategia": "refuerzo",
                "actividad": actividad,
                "motivo": "Terminar actividad pendiente"
            })

    # 2. ⏰ ACTIVIDADES PARA MEJORAR TIEMPO - TODAS LAS IDENTIFICADAS
    for actividad_lenta in actividades_lentas_activas:
        act_tipo = actividad_lenta.get("tipo")
        act_nombre = actividad_lenta.get("nombre")
        act_key = (act_tipo, act_nombre)
        
        if act_key not in actividades_vistas:
            actividades_vistas.add(act_key)
            diferencia = actividad_lenta.get('diferencia_porcentual', 0)
            roadmap.append({
                "estrategia": "refuerzo_tiempo",
                "actividad": actividad_lenta,
                "motivo": f"Mejorar eficiencia (+{diferencia:.1f}% vs promedio)"
            })

    # 3. 📈 ACTIVIDADES PARA MEJORAR (Completado → Perfecto) - TODAS
    for actividad in actividades_mejora:
        act_tipo = actividad.get("tipo")
        act_nombre = actividad.get("nombre")
        act_key = (act_tipo, act_nombre)
        
        if act_key not in actividades_vistas:
            actividades_vistas.add(act_key)
            roadmap.append({
                "estrategia": "mejora", 
                "actividad": actividad,
                "motivo": "Buscar calificación perfecta"
            })

    # 4. 🚀 ACTIVIDADES NUEVAS - BUSCAR HASTA 10 COMO MÁXIMO RAZONABLE
    # (Para evitar roadmap infinito si hay muchas actividades disponibles)
    actividades_nuevas_agregadas = 0
    max_nuevas_razonable = 10
    
    while actividades_nuevas_agregadas < max_nuevas_razonable:
        siguiente = obtener_siguiente_no_rap()
        if not siguiente:
            break
            
        act_tipo: Optional[str] = siguiente.get("tipo")
        act_nombre: Optional[str] = siguiente.get("nombre")
        act_key = (act_tipo, act_nombre)
        
        if act_key not in actividades_vistas:
            actividades_vistas.add(act_key)
            roadmap.append({
                "estrategia": "nuevas", 
                "actividad": siguiente,
                "motivo": "Nuevo desafío de aprendizaje"
            })
            actividades_nuevas_agregadas += 1
        else:
            # Si encontramos una actividad que ya está en el roadmap, salir
            break

    print(f"\n")
    print(f"   📋 Roadmap generado con {len(roadmap)} actividades totales:")
    print(f"   🔄 Actividades en intento: {len(actividades_intento)}")
    print(f"   ⏰ Actividades para mejorar tiempo: {len(actividades_lentas_activas)}")
    print(f"   📈 Actividades para mejorar: {len(actividades_mejora)}")
    print(f"   🚀 Actividades nuevas: {actividades_nuevas_agregadas}")

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

# NUEVAS FUNCIONES PARA ANÁLISIS COMPARATIVO - CORREGIDAS

def analizar_rendimiento_comparativo(correo: str) -> Dict[str, Any]:
    """
    Analiza el rendimiento del alumno comparado con las estadísticas globales
    EXCLUYE RAPs del análisis comparativo
    """
    from Neo4J.neo_queries import (
        fetch_estadisticas_globales,  # CORREGIDO: nombre correcto
        fetch_estadisticas_alumno,    # CORREGIDO: nombre correcto
        fetch_verificar_alumno_perfecto  # CORREGIDO: nombre correcto
    )
    
    # Verificar si el alumno tiene todo perfecto (EXCLUYENDO RAPs)
    if not fetch_verificar_alumno_perfecto(correo):
        return {"error": "El alumno no tiene todas las actividades en estado Perfecto"}
    
    print("📊 Obteniendo datos para análisis comparativo...")
    stats_globales = fetch_estadisticas_globales()
    stats_alumno = fetch_estadisticas_alumno(correo)
    
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