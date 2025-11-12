"""
Módulo de Consultas Inteligentes - Cerebro del Sistema

Procesa datos de Neo4J y aplica algoritmos inteligentes para generar
recomendaciones personalizadas, roadmaps de aprendizaje y análisis comparativos.
Actúa como puente entre la capa de datos (neo_queries.py) y la interfaz (main.py).

Funciones principales:
    - Motor de recomendaciones con jerarquía de prioridades
    - Generación de roadmaps de aprendizaje adaptativos
    - Análisis comparativo de rendimiento vs promedios globales
    - Sistema de insights automáticos basados en métricas

Estrategias implementadas:
    - 🔄 Refuerzo: Para actividades pendientes (Intento)
    - 📈 Mejora: Para actividades completadas pero no perfectas
    - ⏰ Refuerzo_tiempo: Para mejorar eficiencia temporal
    - 🚀 Nuevas: Para expandir conocimiento con nuevas actividades

Características:
    - Exclusión consistente de actividades RAP
    - Ordenamiento por antigüedad (más antiguas primero)
    - Análisis de eficiencia comparativa
    - Generación automática de insights
"""

from typing import List, Any, Optional, Dict, Callable, Tuple

# ============================================================================
# CONSTANTES
# ============================================================================

MAX_ACTIVIDADES_NUEVAS: int = 10
FECHA_MAXIMA: str = "9999-12-31"
UMBRAL_MUY_LENTO: float = 30.0
UMBRAL_LENTO: float = 10.0
UMBRAL_EFICIENTE: float = -10.0
UMBRAL_MUY_EFICIENTE: float = -25.0

# Define type aliases for better clarity
ActivityDict = Dict[str, Any]
ProgressItem = Dict[str, Any]
RecommendationResult = Optional[Dict[str, Any]]
FetchNextFunction = Callable[[], Optional[ActivityDict]]


# ============================================================================
# FUNCIONES DE RECOMENDACIONES
# ============================================================================

def recomendar_siguiente_from_progress(progreso: List[ProgressItem]) -> RecommendationResult:
    """
    Analiza el progreso y recomienda siguiente actividad usando jerarquía de prioridades.
    
    Jerarquía de prioridades (por orden de antigüedad):
        1. 🚀 NUEVAS ACTIVIDADES (no en progreso) - señal para buscar en Neo4J
        2. 🔄 ACTIVIDADES NO TERMINADAS (Intento) - MÁS ANTIGUA primero
        3. 📈 ACTIVIDADES NO PERFECTAS (Completado) - MÁS ANTIGUA primero
        4. ⏰ REFUERZO DE TIEMPO - MENOS EFICIENTE primero

    Args:
        progreso: Lista del progreso actual del alumno

    Returns:
        RecommendationResult: Diccionario con estrategia y actividad recomendada,
                             o señal para buscar nuevas actividades
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
        intentos_ordenados = _ordenar_por_antiguedad(intentos)
        return {"estrategia": "refuerzo", "actividad": intentos_ordenados[0]}

    # 2. Buscar actividades en Completado (no perfectas) - MÁS ANTIGUA primero
    completados = [p for p in progreso_filtrado if p.get("estado") == "Completado"]
    if completados:
        completados_ordenados = _ordenar_por_antiguedad(completados)
        return {"estrategia": "mejora", "actividad": completados_ordenados[0]}

    # 3. Si todo está Perfecto, buscar nuevas actividades
    return {"estrategia": "nuevas", "actividad": None}


def _ordenar_por_antiguedad(actividades: List[ProgressItem]) -> List[ProgressItem]:
    """
    Ordena actividades por fecha de inicio (más antigua primero).
    
    Args:
        actividades: Lista de actividades a ordenar
        
    Returns:
        Lista ordenada por antigüedad
    """
    return sorted(
        actividades, 
        key=lambda x: x.get("start") or FECHA_MAXIMA
    )


# ============================================================================
# FUNCIONES DE GESTIÓN DE ROADMAP - REFACTORIZADAS
# ============================================================================

def _crear_mapa_progreso(progreso: List[ProgressItem]) -> Dict[Tuple[Optional[str], Optional[str]], ActivityDict]:
    """
    Crea un mapa de progreso en memoria excluyendo actividades RAP.
    
    Args:
        progreso: Progreso actual del alumno
        
    Returns:
        Mapa de actividades por (tipo, nombre)
    """
    return {
        (p.get("tipo"), p.get("nombre")): p 
        for p in progreso 
        if p.get("tipo") != "RAP"
    }


def _clasificar_actividades_por_estrategia(
    prog_map: Dict[Tuple[Optional[str], Optional[str]], ActivityDict],
    actividades_vistas: set[Tuple[Optional[str], Optional[str]]],
    actividades_lentas: Optional[List[ActivityDict]] = None
) -> Tuple[List[ActivityDict], List[ActivityDict], List[ActivityDict]]:
    """
    Clasifica actividades en categorías según estrategias.
    
    Args:
        prog_map: Mapa de progreso
        actividades_vistas: Conjunto de actividades ya procesadas
        actividades_lentas: Lista de actividades con baja eficiencia
        
    Returns:
        Tupla con (actividades_intento, actividades_mejora, actividades_lentas_activas)
    """
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
    
    # Procesar actividades lentas si están disponibles
    if actividades_lentas:
        actividades_lentas_activas = _procesar_actividades_lentas(
            actividades_lentas, prog_map, actividades_vistas
        )
    
    return actividades_intento, actividades_mejora, actividades_lentas_activas


def _procesar_actividades_lentas(
    actividades_lentas: List[ActivityDict],
    prog_map: Dict[Tuple[Optional[str], Optional[str]], ActivityDict],
    actividades_vistas: set[Tuple[Optional[str], Optional[str]]]
) -> List[ActivityDict]:
    """
    Procesa y filtra actividades lentas para incluirlas en el roadmap.
    
    Args:
        actividades_lentas: Lista de actividades identificadas como lentas
        prog_map: Mapa de progreso actual
        actividades_vistas: Actividades ya procesadas
        
    Returns:
        Lista de actividades lentas válidas para el roadmap
    """
    actividades_lentas_activas: List[ActivityDict] = []
    procesadas = 0
    
    print(f"🔍 Procesando {len(actividades_lentas)} actividades lentas identificadas...")
    
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
    
    return actividades_lentas_activas


def _agregar_actividades_por_estrategia(
    roadmap: List[Dict[str, Any]],
    actividades_vistas: set[Tuple[Optional[str], Optional[str]]],
    actividades: List[ActivityDict],
    estrategia: str,
    motivo_base: str,
    formato_motivo: Optional[Callable[[ActivityDict], str]] = None
) -> None:
    """
    Agrega actividades al roadmap según la estrategia especificada.
    
    Args:
        roadmap: Lista actual del roadmap
        actividades_vistas: Conjunto de actividades ya procesadas
        actividades: Lista de actividades a agregar
        estrategia: Estrategia a aplicar
        motivo_base: Motivo base para la estrategia
        formato_motivo: Función opcional para formatear el motivo
    """
    for actividad in actividades:
        act_tipo = actividad.get("tipo")
        act_nombre = actividad.get("nombre")
        act_key = (act_tipo, act_nombre)
        
        if act_key not in actividades_vistas:
            actividades_vistas.add(act_key)
            
            # Construir motivo
            motivo = motivo_base
            if formato_motivo:
                motivo = formato_motivo(actividad)
            
            roadmap.append({
                "estrategia": estrategia,
                "actividad": actividad,
                "motivo": motivo
            })


def _agregar_actividades_nuevas(
    roadmap: List[Dict[str, Any]],
    actividades_vistas: set[Tuple[Optional[str], Optional[str]]],
    fetch_next_for_avance: FetchNextFunction
) -> int:
    """
    Agrega nuevas actividades al roadmap hasta alcanzar el máximo razonable.
    
    Args:
        roadmap: Lista actual del roadmap
        actividades_vistas: Conjunto de actividades ya procesadas
        fetch_next_for_avance: Función para obtener siguiente actividad
        
    Returns:
        Número de nuevas actividades agregadas
    """
    def obtener_siguiente_no_rap() -> Optional[ActivityDict]:
        siguiente = fetch_next_for_avance()
        while siguiente and siguiente.get("tipo") == "RAP":
            siguiente = fetch_next_for_avance()
        return siguiente

    actividades_nuevas_agregadas = 0
    
    while actividades_nuevas_agregadas < MAX_ACTIVIDADES_NUEVAS:
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
    
    return actividades_nuevas_agregadas


def generar_roadmap_from_progress_and_fetcher(
    progreso: List[ProgressItem],
    fetch_next_for_avance: FetchNextFunction,
    actividades_lentas: Optional[List[ActivityDict]] = None
) -> List[Dict[str, Any]]:
    """
    Genera secuencia completa de aprendizaje (roadmap) con jerarquía de prioridades.
    
    Estrategias aplicadas en orden:
        1. 🔄 ACTIVIDADES EN INTENTO (no terminadas) - TODAS
        2. ⏰ ACTIVIDADES PARA MEJORAR TIEMPO (TODAS las identificadas como lentas)
        3. 📈 ACTIVIDADES PARA MEJORAR (Completado → Perfecto) - TODAS
        4. 🚀 NUEVAS ACTIVIDADES (no en progreso) - hasta 10 como máximo razonable

    Args:
        progreso: Progreso actual del alumno
        fetch_next_for_avance: Función para obtener siguiente actividad
        actividades_lentas: Lista de actividades con baja eficiencia

    Returns:
        List[Dict[str, Any]]: Roadmap ordenado con actividades y estrategias
    """
    roadmap: List[Dict[str, Any]] = []
    actividades_vistas: set[Tuple[Optional[str], Optional[str]]] = set()
    
    # 1. Preparar datos
    prog_map = _crear_mapa_progreso(progreso)
    
    # 2. Clasificar actividades por estrategia
    actividades_intento, actividades_mejora, actividades_lentas_activas = _clasificar_actividades_por_estrategia(
        prog_map, actividades_vistas, actividades_lentas
    )
    
    # 3. Aplicar jerarquía de prioridades
    
    # 3.1. ACTIVIDADES EN INTENTO (prioridad máxima)
    _agregar_actividades_por_estrategia(
        roadmap, actividades_vistas, actividades_intento,
        "refuerzo", "Terminar actividad pendiente"
    )
    
    # 3.2. ACTIVIDADES PARA MEJORAR TIEMPO
    def formatear_motivo_tiempo(actividad: ActivityDict) -> str:
        diferencia = actividad.get('diferencia_porcentual', 0)
        return f"Mejorar eficiencia (+{diferencia:.1f}% vs promedio)"
    
    _agregar_actividades_por_estrategia(
        roadmap, actividades_vistas, actividades_lentas_activas,
        "refuerzo_tiempo", "", formatear_motivo_tiempo
    )
    
    # 3.3. ACTIVIDADES PARA MEJORAR (Completado → Perfecto)
    _agregar_actividades_por_estrategia(
        roadmap, actividades_vistas, actividades_mejora,
        "mejora", "Buscar calificación perfecta"
    )
    
    # 3.4. ACTIVIDADES NUEVAS
    actividades_nuevas_agregadas = _agregar_actividades_nuevas(
        roadmap, actividades_vistas, fetch_next_for_avance
    )
    
    # 4. Reporte final
    _mostrar_resumen_roadmap(roadmap, actividades_intento, actividades_lentas_activas, actividades_mejora, actividades_nuevas_agregadas)
    
    return roadmap


def _mostrar_resumen_roadmap(
    roadmap: List[Dict[str, Any]],
    actividades_intento: List[ActivityDict],
    actividades_lentas_activas: List[ActivityDict],
    actividades_mejora: List[ActivityDict],
    actividades_nuevas_agregadas: int
) -> None:
    """
    Muestra resumen del roadmap generado.
    
    Args:
        roadmap: Roadmap completo
        actividades_intento: Actividades en intento procesadas
        actividades_lentas_activas: Actividades lentas procesadas
        actividades_mejora: Actividades para mejorar procesadas
        actividades_nuevas_agregadas: Nuevas actividades agregadas
    """
    print(f"\n")
    print(f"   📋 Roadmap generado con {len(roadmap)} actividades totales:")
    print(f"   🔄 Actividades en intento: {len(actividades_intento)}")
    print(f"   ⏰ Actividades para mejorar tiempo: {len(actividades_lentas_activas)}")
    print(f"   📈 Actividades para mejorar: {len(actividades_mejora)}")
    print(f"   🚀 Actividades nuevas: {actividades_nuevas_agregadas}")


def generar_roadmap_para_alumno(
    correo: str,
    fetch_progreso_func: Callable[[str], List[Dict[str, Any]]],
    fetch_next_func: FetchNextFunction
) -> List[Dict[str, Any]]:
    """
    Función conveniente para generar roadmap completo para un alumno específico.
    
    Combina obtención de progreso y generación de roadmap en una sola operación.
    Excluye actividades RAP del análisis.

    Args:
        correo: Correo del alumno
        fetch_progreso_func: Función para obtener progreso del alumno
        fetch_next_func: Función para obtener siguiente actividad

    Returns:
        List[Dict[str, Any]]: Roadmap personalizado para el alumno
    """
    # Obtener progreso del alumno
    progreso = fetch_progreso_func(correo)
    
    # Asegurarnos de que tenemos una lista válida
    if not progreso:
        progreso = []
    
    return generar_roadmap_from_progress_and_fetcher(progreso, fetch_next_func)


# ============================================================================
# FUNCIONES DE ANÁLISIS COMPARATIVO
# ============================================================================

def analizar_rendimiento_comparativo(
    correo: str,
    fetch_verificar_perfecto_func: Callable[[str], bool],
    fetch_estadisticas_globales_func: Callable[[], Dict[str, Dict[str, Dict[str, Any]]]],
    fetch_estadisticas_alumno_func: Callable[[str], Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Analiza el rendimiento del alumno comparado con estadísticas globales.
    
    Requiere que el alumno tenga todas las actividades en estado 'Perfecto'
    para realizar un análisis comparativo completo. Excluye RAPs del análisis.

    Args:
        correo: Correo del alumno a analizar
        fetch_verificar_perfecto_func: Función para verificar si tiene todo perfecto
        fetch_estadisticas_globales_func: Función para obtener estadísticas globales
        fetch_estadisticas_alumno_func: Función para obtener estadísticas del alumno

    Returns:
        Dict[str, Any]: Análisis completo con comparativas, insights y recomendaciones
    """
    # Verificar si el alumno tiene todo perfecto (EXCLUYENDO RAPs)
    if not fetch_verificar_perfecto_func(correo):
        return {"error": "El alumno no tiene todas las actividades en estado Perfecto"}
    
    print("📊 Obteniendo datos para análisis comparativo...")
    stats_globales = fetch_estadisticas_globales_func()
    stats_alumno = fetch_estadisticas_alumno_func(correo)
    
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
        
        # Crear comparativa
        comparativa = _crear_comparativa_actividad(actividad_alumno, tipo, nombre, duraciones_alumno, stats_globales)
        analisis["comparativas"].append(comparativa)
    
    # Actualizar contador real de actividades analizadas
    analisis["resumen_general"]["actividades_analizadas"] = actividades_analizadas
    
    # Generar insights basados en el análisis
    if analisis["comparativas"]:
        _generar_insights_comparativos(analisis)
    
    return analisis


def _crear_comparativa_actividad(
    actividad_alumno: Dict[str, Any],
    tipo: str,
    nombre: str,
    duraciones_alumno: List[float],
    stats_globales: Dict[str, Dict[str, Dict[str, Any]]]
) -> Dict[str, Any]:
    """
    Crea una comparativa individual para una actividad.
    
    Args:
        actividad_alumno: Datos de la actividad del alumno
        tipo: Tipo de actividad
        nombre: Nombre de la actividad
        duraciones_alumno: Lista de duraciones del alumno
        stats_globales: Estadísticas globales
        
    Returns:
        Dict con datos comparativos de la actividad
    """
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
        comparativa["eficiencia"] = _categorizar_eficiencia(comparativa["diferencia_porcentual"])
    
    return comparativa


def _categorizar_eficiencia(diferencia_porcentual: float) -> str:
    """
    Categoriza la eficiencia basándose en la diferencia porcentual.
    
    Args:
        diferencia_porcentual: Diferencia porcentual vs promedio
        
    Returns:
        str: Categoría de eficiencia
    """
    if diferencia_porcentual < UMBRAL_MUY_EFICIENTE:
        return "MUY_EFICIENTE"
    elif diferencia_porcentual < UMBRAL_EFICIENTE:
        return "EFICIENTE"
    elif diferencia_porcentual < UMBRAL_LENTO:
        return "PROMEDIO"
    elif diferencia_porcentual < UMBRAL_MUY_LENTO:
        return "LENTO"
    else:
        return "MUY_LENTO"


def _generar_insights_comparativos(analisis: Dict[str, Any]) -> None:
    """
    Genera insights automáticos basados en el análisis comparativo de tiempos.
    
    Categoriza actividades y genera recomendaciones específicas según
    los patrones de eficiencia identificados. Excluye RAPs del análisis.

    Args:
        analisis: Análisis con comparativas de rendimiento
    """
    comparativas: List[Dict[str, Any]] = analisis.get("comparativas", [])
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
    """
    Convierte segundos a formato legible para análisis y reportes.
    
    Args:
        segundos: Tiempo en segundos
        
    Returns:
        str: Tiempo formateado en segundos, minutos u horas según corresponda
    """
    if segundos < 60:
        return f"{segundos:.0f} segundos"
    elif segundos < 3600:
        minutos: float = segundos / 60
        return f"{minutos:.1f} minutos"
    else:
        horas: float = segundos / 3600
        return f"{horas:.1f} horas"