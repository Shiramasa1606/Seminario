import os
from typing import Any, Dict, Optional, List
from Neo4J.conn import obtener_driver
from Neo4J.neo_queries import fetch_alumnos, fetch_progreso_alumno, fetch_siguiente_por_avance, verificar_alumno_todo_perfecto
from Neo4J.consultar import recomendar_siguiente_from_progress, generar_roadmap_from_progress_and_fetcher, analizar_rendimiento_comparativo, formatear_tiempo_analisis
from Neo4J.Inserts.insertMain import rellenarGrafo, mostrar_estadisticas_rapidas

driver = obtener_driver()

# ============================================================
# Funciones de consola
# ============================================================

def limpiar_consola() -> None:
    os.system('cls' if os.name == 'nt' else 'clear')

def mostrar_menu_principal() -> str:
    print("\n" + "="*40)
    print("🎯 SISTEMA DE RECOMENDACIÓN DE APRENDIZAJE")
    print("="*40)
    print("1. Ejecutar inserción de datos (rellenar grafo)")
    print("2. Consultar alumnos y progreso")
    print("3. Ver estadísticas del sistema")
    print("0. Salir")
    return input("\nSeleccione una opción: ").strip()

def mostrar_menu_alumno(nombre: str) -> str:
    print(f"\n=== Alumno: {nombre} ===")
    print("1. 📊 Ver progreso")
    print("2. 🎯 Siguiente actividad recomendada")
    print("3. 🗺️ Roadmap completo")
    print("4. 📈 Análisis avanzado (disponible para todos)")
    print("0. ↩️ Volver al menú principal")
    return input("\nSeleccione una opción: ").strip()

# ============================================================
# Funciones de análisis detallado
# ============================================================

def _mostrar_analisis_detallado(analisis: Dict[str, Any]) -> None:
    """Muestra el análisis detallado actividad por actividad"""
    print(f"\n" + "📈 ANÁLISIS DETALLADO POR ACTIVIDAD")
    print("=" * 70)
    
    comparativas: List[Dict[str, Any]] = analisis.get("comparativas", [])
    
    for comparativa in comparativas:
        print(f"\n📚 {comparativa.get('actividad', 'Desconocida')} ({comparativa.get('tipo', 'Desconocido')})")
        print(f"   Puntaje: {comparativa.get('puntaje_final', 0)}% - Intentos: {comparativa.get('total_intentos', 0)}")
        print(f"   Tu tiempo promedio: {formatear_tiempo_analisis(comparativa.get('duracion_promedio_alumno', 0))}")
        
        if "duracion_promedio_global" in comparativa:
            print(f"   Tiempo promedio del grupo: {formatear_tiempo_analisis(comparativa['duracion_promedio_global'])}")
            diferencia: float = comparativa.get('diferencia_porcentual', 0)
            
            if diferencia < -10:
                print(f"   🚀 Eres {abs(diferencia):.1f}% más rápido que el promedio")
            elif diferencia > 10:
                print(f"   ⏰ Estás {diferencia:.1f}% más lento que el promedio")
            else:
                print(f"   📊 Tu tiempo está en el promedio")
            
            if "eficiencia" in comparativa:
                emoji_eficiencia = {
                    "MUY_EFICIENTE": "🚀",
                    "EFICIENTE": "⚡", 
                    "PROMEDIO": "📊",
                    "LENTO": "🐢",
                    "MUY_LENTO": "⏰"
                }.get(comparativa["eficiencia"], "📌")
                print(f"   {emoji_eficiencia} Categoría: {comparativa['eficiencia'].replace('_', ' ').title()}")

# ============================================================
# Funciones de opciones MEJORADAS
# ============================================================

def ver_progreso_alumno(correo: str) -> None:
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("⚠️ No hay progreso registrado para este alumno")
        return
    
    print("\n" + "📊 PROGRESO DEL ALUMNO")
    print("=" * 60)
    
    # Estadísticas rápidas
    total_actividades = len(progreso)
    intentos = len([p for p in progreso if p.get("estado") == "Intento"])
    completados = len([p for p in progreso if p.get("estado") == "Completado"])
    perfectos = len([p for p in progreso if p.get("estado") == "Perfecto"])
    
    print(f"📈 Resumen General:")
    print(f"   • 🔄 Intentos: {intentos}")
    print(f"   • ✅ Completados: {completados}")
    print(f"   • 🏆 Perfectos: {perfectos}")
    print(f"   • 📊 Total actividades: {total_actividades}")
    
    if total_actividades > 0:
        porcentaje_completado = ((completados + perfectos) / total_actividades) * 100
        print(f"   • 🎯 Progreso general: {porcentaje_completado:.1f}%")
    
    # Agrupar actividades por estado
    actividades_por_estado: Dict[str, List[Dict[str, Any]]] = {
        "🔄 EN PROGRESO": [p for p in progreso if p.get("estado") == "Intento"],
        "✅ COMPLETADAS": [p for p in progreso if p.get("estado") == "Completado"],
        "🏆 PERFECTAS": [p for p in progreso if p.get("estado") == "Perfecto"]
    }
    
    print("\n" + "📋 DETALLE AGRUPADO POR ESTADO")
    print("=" * 60)
    
    for estado, actividades in actividades_por_estado.items():
        if actividades:
            print(f"\n{estado} ({len(actividades)} actividades):")
            print("-" * 40)
            
            # Agrupar por tipo de actividad (usando nombre diferente)
            actividades_agrupadas_por_tipo: Dict[str, List[Dict[str, Any]]] = {}
            for actividad in actividades:
                tipo_act: str = str(actividad.get('tipo', 'Desconocido'))
                if tipo_act not in actividades_agrupadas_por_tipo:
                    actividades_agrupadas_por_tipo[tipo_act] = []
                actividades_agrupadas_por_tipo[tipo_act].append(actividad)
            
            for tipo_act, lista_act in actividades_agrupadas_por_tipo.items():
                print(f"  📚 {tipo_act} ({len(lista_act)}):")
                for act in lista_act:
                    nombre_act: str = str(act.get('nombre', 'Sin nombre'))
                    # Mostrar información adicional si está disponible
                    info_extra = ""
                    if act.get('score'):
                        score_val = act.get('score')
                        info_extra = f" - Puntaje: {score_val}%"
                    elif act.get('duration_seconds'):
                        duration_val = act.get('duration_seconds', 0)
                        minutos: int = int(duration_val) // 60
                        info_extra = f" - Tiempo: {minutos}min"
                    
                    print(f"     • {nombre_act}{info_extra}")
    
    # Resumen por tipo de actividad (usando nombre diferente)
    print("\n" + "🎯 RESUMEN POR TIPO DE ACTIVIDAD")
    print("-" * 40)
    
    resumen_por_tipo: Dict[str, Dict[str, int]] = {}
    for actividad in progreso:
        tipo_act: str = str(actividad.get('tipo', 'Desconocido'))
        if tipo_act not in resumen_por_tipo:
            resumen_por_tipo[tipo_act] = {'total': 0, 'intentos': 0, 'completados': 0, 'perfectos': 0}
        
        resumen_por_tipo[tipo_act]['total'] += 1
        estado_act = actividad.get('estado', '')
        if estado_act == "Intento":
            resumen_por_tipo[tipo_act]['intentos'] += 1
        elif estado_act == "Completado":
            resumen_por_tipo[tipo_act]['completados'] += 1
        elif estado_act == "Perfecto":
            resumen_por_tipo[tipo_act]['perfectos'] += 1
    
    for tipo_act, estadisticas in resumen_por_tipo.items():
        print(f"  📖 {tipo_act}:")
        print(f"     • Total: {estadisticas['total']}")
        if estadisticas['intentos'] > 0:
            print(f"     • 🔄 En progreso: {estadisticas['intentos']}")
        if estadisticas['completados'] > 0:
            print(f"     • ✅ Completadas: {estadisticas['completados']}")
        if estadisticas['perfectos'] > 0:
            print(f"     • 🏆 Perfectas: {estadisticas['perfectos']}")

def ver_siguiente_actividad_alumno(correo: str) -> None:
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("⚠️ No hay progreso registrado para este alumno")
        siguiente = fetch_siguiente_por_avance(correo)
        if siguiente:
            print(f"\n🎯 **RECOMENDACIÓN PARA COMENZAR:**")
            print(f"   • 📚 Comienza con: '{siguiente.get('nombre')}'")
            print(f"   • 💡 Es tu primera actividad en el sistema")
            print(f"   • 🎯 Objetivo: Familiarizarte con la plataforma")
        return
    
    sugerencia = recomendar_siguiente_from_progress(progreso)
    if not sugerencia:
        print("🎉 **¡FELICITACIONES!**")
        print("=" * 50)
        print("🏆 **Has alcanzado un logro importante:**")
        print("   • ✅ Todas las actividades están completadas o en estado perfecto")
        print("   • 📚 Has dominado el material disponible")
        print("   • 🎯 Objetivo cumplido: Aprendizaje completo")
        print(f"\n💡 **Próximos pasos sugeridos:**")
        print("   • 🔄 Repasar temas que necesites reforzar")
        print("   • ⏳ Esperar nuevas actividades del profesor")
        print("   • 📊 Revisar tu progreso para mantener el nivel")
        return
    
    act = sugerencia['actividad']
    estrategia = sugerencia['estrategia']
    
    print("\n" + "🎯 SIGUIENTE ACTIVIDAD RECOMENDADA")
    print("=" * 60)
    
    # Emojis y títulos según estrategia
    estrategia_config = {
        "refuerzo": ("🔄", "REFUERZO - TERMINAR ACTIVIDAD PENDIENTE"),
        "mejora": ("📈", "MEJORA - BUSCAR LA PERFECCIÓN"), 
        "avance": ("🚀", "AVANCE - NUEVO DESAFÍO"),
        "inicio": ("🎯", "INICIO - COMENZAR EL APRENDIZAJE")
    }.get(estrategia, ("📌", estrategia.upper()))
    
    emoji, titulo = estrategia_config
    
    print(f"{emoji} {titulo}")
    print("-" * 50)
    print(f"📚 Tipo: {act.get('tipo', 'Desconocido')}")
    print(f"📖 Actividad: {act.get('nombre', 'Sin nombre')}")
    
    # MENSAJES ESPECÍFICOS SEGÚN ESTRATEGIA
    if estrategia == "refuerzo":
        print(f"\n🔍 **ANÁLISIS DE TU SITUACIÓN:**")
        print(f"   • 📅 Esta actividad está en estado 'Intento'")
        print(f"   • ⏰ Es tu actividad más antigua sin completar")
        print(f"   • 🎯 Necesita tu atención prioritaria")
        
        print(f"\n💡 **PLAN DE ACCIÓN RECOMENDADO:**")
        print(f"   • 1️⃣ **Revisa el material**: Consulta los RAPs relacionados")
        print(f"   • 2️⃣ **Identifica dificultades**: ¿Qué conceptos te causan problemas?")
        print(f"   • 3️⃣ **Practica**: Resuelve ejercicios similares")
        print(f"   • 4️⃣ **Reintenta**: Completa la actividad al 100%")
        
        print(f"\n⏱️  **GESTIÓN DEL TIEMPO:**")
        print(f"   • 🕒 Dedica al menos 30-45 minutos seguidos")
        print(f"   • ⏸️  Toma descansos cortos cada 25 minutos")
        print(f"   • 📝 Anota tus dudas para consultar después")
        
    elif estrategia == "mejora":
        print(f"\n🔍 **ANÁLISIS DE TU SITUACIÓN:**")
        print(f"   • ✅ Esta actividad está 'Completada' pero no 'Perfecta'")
        print(f"   • 📊 Tienes oportunidad de mejorar tu calificación")
        print(f"   • 🎯 Estás cerca del dominio total del tema")
        
        print(f"\n💡 **PLAN DE ACCIÓN RECOMENDADO:**")
        print(f"   • 1️⃣ **Revisa errores**: ¿Dónde perdiste puntos?")
        print(f"   • 2️⃣ **Profundiza**: Estudia los conceptos específicos")
        print(f"   • 3️⃣ **Practica selectiva**: Enfócate en tus áreas débiles")
        print(f"   • 4️⃣ **Perfecciona**: Busca el 100% de calificación")
        
        print(f"\n🎯 **OBJETIVO DE CALIDAD:**")
        print(f"   • ⭐ No se trata solo de terminar, sino de dominar")
        print(f"   • 📈 La práctica deliberada lleva a la excelencia")
        print(f"   • 🏆 El 'Perfecto' demuestra comprensión completa")
        
    elif estrategia == "avance":
        print(f"\n🔍 **ANÁLISIS DE TU SITUACIÓN:**")
        print(f"   • 🏆 Tienes actividades en estado 'Perfecto'")
        print(f"   • 📚 Demuestras dominio de los temas anteriores")
        print(f"   • 🚀 Estás listo para nuevos desafíos")
        
        print(f"\n💡 **PLAN DE ACCIÓN RECOMENDADO:**")
        print(f"   • 1️⃣ **Mantén el ritmo**: Sigue con la misma dedicación")
        print(f"   • 2️⃣ **Aplica conocimiento**: Usa lo aprendido en lo nuevo")
        print(f"   • 3️⃣ **Conecta conceptos**: Relaciona con temas anteriores")
        print(f"   • 4️⃣ **Profundiza**: Ve más allá de lo básico")
        
        print(f"\n🌟 **MANTENIENDO EL ÉXITO:**")
        print(f"   • 📚 El aprendizaje continuo es clave")
        print(f"   • 💪 Tu consistencia te ha traído hasta aquí")
        print(f"   • 🎯 Sigue desafiándote a ti mismo")
        
    elif estrategia == "inicio":
        print(f"\n🔍 **ANÁLISIS DE TU SITUACIÓN:**")
        print(f"   • 🆕 Estás comenzando tu journey de aprendizaje")
        print(f"   • 📖 Esta es tu primera actividad recomendada")
        print(f"   • 🎯 Es el punto de partida ideal")
        
        print(f"\n💡 **PLAN DE ACCIÓN RECOMENDADO:**")
        print(f"   • 1️⃣ **Familiarízate**: Conoce la plataforma")
        print(f"   • 2️⃣ **Establece ritmo**: Encuentra tu horario ideal")
        print(f"   • 3️⃣ **Consulta material**: Usa los RAPs como guía")
        print(f"   • 4️⃣ **Avanza progresivamente**: Paso a paso")
    
    # RESUMEN FINAL MOTIVACIONAL
    print(f"\n" + "🌟 RESUMEN FINAL" + " 🌟")
    print("-" * 30)
    
    total = len(progreso)
    completados = len([p for p in progreso if p.get("estado") in ["Completado", "Perfecto"]])
    
    if total > 0:
        progreso_porcentaje = (completados / total) * 100
        print(f"📊 **Tu progreso general**: {progreso_porcentaje:.1f}%")
        
        if progreso_porcentaje == 0:
            print("💪 **¡Comienza hoy mismo! Cada viaje empieza con un primer paso**")
        elif progreso_porcentaje < 50:
            print("🔥 **¡Vas por buen camino! Sigue construyendo tu base**")
        elif progreso_porcentaje < 80:
            print("⭐ **¡Excelente progreso! La consistencia es tu aliada**")
        else:
            print("🏆 **¡Impresionante! Estás cerca de dominar todo el material**")

def ver_roadmap_alumno(correo: str) -> None:
    from Neo4J.consultar import analizar_rendimiento_comparativo
    
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("⚠️ No hay progreso registrado para este alumno")
        progreso = []
    
    def fetch_next_activity() -> Optional[Dict[str, Any]]:
        return fetch_siguiente_por_avance(correo)
    
    # Obtener actividades lentas del análisis comparativo
    actividades_lentas: List[Dict[str, Any]] = []
    try:
        analisis: Dict[str, Any] = analizar_rendimiento_comparativo(correo)
        if "error" not in analisis and "comparativas" in analisis:
            comparativas: List[Dict[str, Any]] = analisis["comparativas"]
            # Filtrar actividades donde el alumno fue más lento que el promedio
            actividades_lentas = [
                comparativa for comparativa in comparativas
                if comparativa.get("diferencia_porcentual", 0) > 10  # +10% más lento
            ]
            # Ordenar por menor eficiencia (mayor diferencia primero)
            actividades_lentas.sort(
                key=lambda x: x.get("diferencia_porcentual", 0), 
                reverse=True
            )
            print(f"📊 Identificadas {len(actividades_lentas)} actividades para refuerzo de tiempo")
    except Exception as e:
        print(f"⚠️ No se pudieron obtener actividades para refuerzo de tiempo: {e}")
        actividades_lentas = []
    
    roadmap: List[Dict[str, Any]] = generar_roadmap_from_progress_and_fetcher(
        progreso, 
        fetch_next_activity, 
        actividades_lentas
    )
    
    if not roadmap:
        print("🎉 ¡Felicidades! Has completado todas las actividades disponibles")
        return
    
    print("\n" + "🗺️ ROADMAP COMPLETO DE APRENDIZAJE")
    print("-" * 50)
    print(f"📋 Total de recomendaciones: {len(roadmap)}")
    
    # Mostrar estadísticas de tipos de recomendaciones
    estrategias_count: Dict[str, int] = {}
    for r in roadmap:
        estrategia: str = r["estrategia"]
        estrategias_count[estrategia] = estrategias_count.get(estrategia, 0) + 1
    
    print("📊 Resumen: ", end="")
    for estrategia, count in estrategias_count.items():
        emoji: str = {
            "nuevas": "🆕",
            "refuerzo": "🔄", 
            "mejora": "📈",
            "refuerzo_tiempo": "⏰"
        }.get(estrategia, "📌")
        print(f"{emoji} {count} ", end="")
    print()
    
    print("\n" + "=" * 60)
    
    for i, r in enumerate(roadmap, 1):
        act: Dict[str, Any] = r['actividad']
        estrategia: str = r['estrategia']
        
        # Actualizar emojis según nuevas estrategias
        estrategia_config: Dict[str, tuple[str, str]] = {
            "nuevas": ("🆕", "NUEVA ACTIVIDAD"),
            "refuerzo": ("🔄", "TERMINAR ACTIVIDAD"),
            "mejora": ("📈", "MEJORAR RESULTADO"), 
            "refuerzo_tiempo": ("⏰", "REFUERZO DE TIEMPO"),
            "avance": ("🚀", "AVANCE")
        }
        
        emoji, texto_estrategia = estrategia_config.get(
            estrategia, 
            ("📌", estrategia.upper())
        )
        
        print(f"\n{i}. {emoji} [{texto_estrategia}]")
        print(f"   📚 {act.get('tipo', 'Actividad')}")
        print(f"   📖 {act.get('nombre', 'Sin nombre')}")
        
        # Mostrar motivo si existe (especialmente para refuerzo_tiempo)
        if r.get('motivo'):
            print(f"   💡 {r['motivo']}")
        # Mostrar diferencia de tiempo para actividades de refuerzo
        elif estrategia == "refuerzo_tiempo" and act.get('diferencia_porcentual'):
            print(f"   ⏱️  Tiempo: +{act['diferencia_porcentual']:.1f}% vs promedio")
        
        # Línea separadora cada 3 pasos
        if i % 3 == 0 and i < len(roadmap):
            print("   " + "─" * 40)

def ver_analisis_avanzado_alumno(correo: str) -> None:
    """
    Muestra análisis avanzado de rendimiento - DISPONIBLE PARA TODOS LOS ALUMNOS
    """
    print("\n" + "📊 ANÁLISIS AVANZADO DE RENDIMIENTO")
    print("=" * 60)
    print("⏳ Analizando tu desempeño comparado con el grupo...")
    
    # Obtener progreso del alumno para mostrar estado actual
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("⚠️ No hay progreso registrado para este alumno")
        return
    
    # Mostrar estado actual del alumno
    total_actividades = len(progreso)
    intentos = len([p for p in progreso if p.get("estado") == "Intento"])
    completados = len([p for p in progreso if p.get("estado") == "Completado"])
    perfectos = len([p for p in progreso if p.get("estado") == "Perfecto"])
    
    print(f"\n📈 TU ESTADO ACTUAL:")
    print("-" * 25)
    print(f"• 📊 Actividades totales: {total_actividades}")
    print(f"• 🔄 En progreso: {intentos}")
    print(f"• ✅ Completadas: {completados}")
    print(f"• 🏆 Perfectas: {perfectos}")
    
    if total_actividades > 0:
        progreso_porcentaje = ((completados + perfectos) / total_actividades) * 100
        print(f"• 📈 Progreso general: {progreso_porcentaje:.1f}%")
    
    # Verificar si tiene todo perfecto para análisis completo
    tiene_todo_perfecto = verificar_alumno_todo_perfecto(correo)
    
    if tiene_todo_perfecto:
        print(f"\n🎉 ¡FELICITACIONES! Tienes todas las actividades en estado 'Perfecto'")
        print("📊 Procediendo con análisis comparativo completo...")
        analisis = analizar_rendimiento_comparativo(correo)
    else:
        print(f"\nℹ️  Análisis básico disponible (análisis completo requiere todas las actividades en 'Perfecto')")
        # Llamar a la función de análisis pero manejar el caso de no-todo-perfecto
        from Neo4J.neo_queries import fetch_estadisticas_globales_actividades, fetch_estadisticas_alumno_avanzadas
        
        stats_globales = fetch_estadisticas_globales_actividades()
        stats_alumno = fetch_estadisticas_alumno_avanzadas(correo)
        
        # Crear un análisis básico con la información disponible
        analisis: Dict[str, Any] = {
            "resumen_general": {
                "total_actividades": stats_alumno["resumen"]["total_actividades"],
                "tiempo_total_alumno": stats_alumno["resumen"]["total_tiempo_segundos"],
                "actividades_analizadas": stats_alumno["resumen"]["actividades_con_tiempo"]
            },
            "comparativas": [],
            "insights": {
                "fortalezas": [],
                "areas_mejora": [],
                "recomendaciones": []
            },
            "nota": "⚠️ Análisis básico - Para análisis completo completa todas las actividades"
        }
        
        # Analizar actividades completadas del alumno
        actividades_completadas = 0
        for actividad_alumno in stats_alumno["actividades"].values():
            tipo: str = actividad_alumno["tipo"]
            nombre: str = actividad_alumno["nombre"]
            
            # Solo analizar actividades con tiempo registrado
            duraciones_alumno = [i["duracion_segundos"] for i in actividad_alumno["intentos"] if i["duracion_segundos"]]
            if not duraciones_alumno:
                continue
                
            actividades_completadas += 1
            duracion_promedio_alumno: float = sum(duraciones_alumno) / len(duraciones_alumno)
            
            comparativa: Dict[str, Any] = {
                "actividad": nombre,
                "tipo": tipo,
                "duracion_promedio_alumno": duracion_promedio_alumno,
                "total_intentos": len(actividad_alumno["intentos"]),
                "puntaje_final": actividad_alumno["mejor_puntaje"],
                "estado_final": actividad_alumno["estado_final"]
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
        
        # Generar insights básicos
        if analisis["comparativas"]:
            _generar_insights_basicos(analisis, tiene_todo_perfecto)
    
    # Mostrar resultados del análisis
    if "error" in analisis:
        print(f"\nℹ️  {analisis['error']}")
        return
    
    if not analisis.get("comparativas"):
        print("⚠️ No hay suficientes datos de tiempo para realizar el análisis")
        print("💡 Las actividades necesitan tener registro de duración")
        return
    
    # Resumen general
    resumen: Dict[str, Any] = analisis["resumen_general"]
    print(f"\n🎯 RESUMEN DE TU DESEMPEÑO")
    print("-" * 40)
    print(f"• 📈 Actividades analizadas: {resumen.get('actividades_analizadas', 0)}")
    tiempo_total: float = resumen.get('tiempo_total_alumno', 0)
    if tiempo_total > 0:
        print(f"• ⏱️  Tiempo total invertido: {formatear_tiempo_analisis(tiempo_total)}")
    
    # Mostrar insights
    insights: Dict[str, List[str]] = analisis["insights"]
    
    if insights.get("fortalezas"):
        print(f"\n💪 TUS FORTALEZAS")
        print("-" * 25)
        for fortaleza in insights["fortalezas"]:
            print(f"  {fortaleza}")
    
    if insights.get("areas_mejora"):
        print(f"\n🎯 ÁREAS DE MEJORA")
        print("-" * 25)
        for mejora in insights["areas_mejora"]:
            print(f"  {mejora}")
    
    if insights.get("recomendaciones"):
        print(f"\n💡 RECOMENDACIONES")
        print("-" * 20)
        for recomendacion in insights["recomendaciones"]:
            print(f"  {recomendacion}")
    
    # Análisis detallado (opcional)
    if analisis.get("comparativas"):
        print(f"\n📋 ¿Ver análisis detallado por actividad? (s/n): ", end="")
        if input().strip().lower() == 's':
            _mostrar_analisis_detallado(analisis)

def _generar_insights_basicos(analisis: Dict[str, Any], tiene_todo_perfecto: bool) -> None:
    """
    Genera insights básicos para alumnos que no tienen todo perfecto
    """
    comparativas: List[Dict[str, Any]] = analisis.get("comparativas", [])
    insights: Dict[str, List[str]] = analisis["insights"]
    
    # Identificar actividades eficientes
    actividades_eficientes = [c for c in comparativas if c.get("eficiencia") in ["MUY_EFICIENTE", "EFICIENTE"]]
    actividades_lentas = [c for c in comparativas if c.get("eficiencia") in ["LENTO", "MUY_LENTO"]]
    
    # Generar fortalezas
    if actividades_eficientes:
        insights["fortalezas"].append(f"⚡ Eres eficiente en {len(actividades_eficientes)} actividades")
        for act in actividades_eficientes[:2]:
            tiempo_ahorrado: float = -act.get("diferencia_porcentual", 0)
            insights["fortalezas"].append(f"   • {act['actividad']}: {tiempo_ahorrado:.1f}% más rápido que el promedio")
    
    # Generar áreas de mejora
    if actividades_lentas:
        insights["areas_mejora"].append(f"⏰ Puedes mejorar tu ritmo en {len(actividades_lentas)} actividades")
        for act in actividades_lentas[:2]:
            tiempo_extra: float = act.get("diferencia_porcentual", 0)
            insights["areas_mejora"].append(f"   • {act['actividad']}: {tiempo_extra:.1f}% más lento que el promedio")
    
    # Recomendaciones según el progreso
    if not tiene_todo_perfecto:
        actividades_perfectas = [c for c in comparativas if c.get("estado_final") == "Perfecto"]
        actividades_completadas = [c for c in comparativas if c.get("estado_final") == "Completado"]
        actividades_intento = [c for c in comparativas if c.get("estado_final") == "Intento"]
        
        if actividades_intento:
            insights["recomendaciones"].append("🎯 Enfócate en completar las actividades en estado 'Intento'")
        if actividades_completadas:
            insights["recomendaciones"].append("📈 Busca alcanzar 'Perfecto' en las actividades completadas")
        if actividades_perfectas:
            insights["recomendaciones"].append("💪 Mantén tu excelencia en las actividades perfectas")
        
        insights["recomendaciones"].append("🏆 Completa todas las actividades para obtener un análisis completo")

def ver_estadisticas_sistema() -> None:
    """
    Muestra estadísticas generales del sistema
    """
    print("\n" + "📊 ESTADÍSTICAS DEL SISTEMA")
    print("=" * 50)
    mostrar_estadisticas_rapidas()
    input("\n📝 Presione Enter para continuar...")

# ============================================================
# Bucle principal MEJORADO
# ============================================================

def main() -> None:
    while True:
        limpiar_consola()
        opcion = mostrar_menu_principal()

        if opcion == "1":
            print("\n🔹 Ejecutando inserción de datos...")
            print("⏳ Esto puede tomar unos momentos...")
            rellenarGrafo()
            input("\n✅ Inserción completada. Presione Enter para continuar...")

        elif opcion == "2":
            alumnos_data = fetch_alumnos()
            if not alumnos_data:
                print("⚠️ No hay alumnos registrados en el sistema")
                print("💡 Ejecute primero la opción 1 para insertar datos")
                input("\nPresione Enter para continuar...")
                continue

            # Extract just the names for display
            alumnos_nombres = [alumno["nombre"] for alumno in alumnos_data]
            # Store the mapping from name to email for later queries
            alumno_map = {alumno["nombre"]: alumno["correo"] for alumno in alumnos_data}

            while True:
                limpiar_consola()
                print("\n" + "👥 LISTA DE ALUMNOS")
                print("=" * 40)
                print("0. ↩️ Volver al menú principal")
                
                for idx, nombre in enumerate(alumnos_nombres, start=1):
                    print(f"{idx}. {nombre}")

                try:
                    seleccion = int(input(f"\nSeleccione un alumno (0-{len(alumnos_nombres)}): ").strip())
                except ValueError:
                    print("❌ Ingrese un número válido")
                    input("\nPresione Enter para continuar...")
                    continue

                if seleccion == 0:
                    break  # volver al menú principal
                elif 1 <= seleccion <= len(alumnos_nombres):
                    alumno_nombre = alumnos_nombres[seleccion - 1]
                    alumno_correo = alumno_map[alumno_nombre]

                    while True:
                        limpiar_consola()
                        print(f"\n🎓 ALUMNO: {alumno_nombre}")
                        print("=" * 40)
                        opcion_alumno = mostrar_menu_alumno(alumno_nombre)

                        if opcion_alumno == "1":
                            ver_progreso_alumno(alumno_correo)
                        elif opcion_alumno == "2":
                            ver_siguiente_actividad_alumno(alumno_correo)
                        elif opcion_alumno == "3":
                            ver_roadmap_alumno(alumno_correo)
                        elif opcion_alumno == "4":
                            ver_analisis_avanzado_alumno(alumno_correo)
                        elif opcion_alumno == "0":
                            break  # volver al listado de alumnos
                        else:
                            print("❌ Opción no válida")

                        input("\n📝 Presione Enter para continuar...")

                else:
                    print(f"❌ Ingrese un número entre 0 y {len(alumnos_nombres)}")
                    input("\nPresione Enter para continuar...")

        elif opcion == "3":
            ver_estadisticas_sistema()

        elif opcion == "0":
            print("\n👋 ¡Hasta pronto!")
            print("💾 Cerrando conexiones...")
            driver.close()
            break

        else:
            print("❌ Opción no válida")
            input("\nPresione Enter para continuar...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Programa interrumpido por el usuario")
        driver.close()
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        driver.close()