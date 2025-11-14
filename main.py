"""
Sistema de Recomendación de Aprendizaje - Interfaz Principal

Interfaz de consola que proporciona acceso a todas las funcionalidades del sistema:
- Inicialización de datos en Neo4J
- Consulta de progreso de alumnos
- Recomendaciones personalizadas
- Análisis avanzado de rendimiento
- Estadísticas del sistema

Módulos integrados:
- Neo4J.conn: Gestión de conexiones a la base de datos
- Neo4J.neo_queries: Consultas básicas a Neo4J
- Neo4J.consultar: Lógica de recomendaciones y análisis
- Neo4J.Inserts.insertMain: Inicialización de datos
"""

import os
from typing import Any, Dict, List

# Importaciones organizadas por módulo
from Neo4J.Inserts.insertMain import mostrar_estadisticas_rapidas, rellenarGrafo
from Neo4J.conn import obtener_driver
from Neo4J.consultar import (
    analizar_rendimiento_comparativo,
    formatear_tiempo_analisis,
    generar_roadmap_from_progress_and_fetcher,
    recomendar_siguiente_from_progress,
)
from Neo4J.neo_queries import (
    fetch_actividades_lentas_alumno,
    fetch_estadisticas_alumno,
    fetch_estadisticas_globales,
    fetch_progreso_alumno,
    fetch_siguiente_actividad,
    fetch_verificar_alumno_perfecto,
)

# Inicializar driver de Neo4J
driver = obtener_driver()


# ============================================================
# Funciones de Utilidad de Consola
# ============================================================

def limpiar_consola() -> None:
    """Limpia la pantalla de la consola según el sistema operativo."""
    os.system('cls' if os.name == 'nt' else 'clear')


def mostrar_menu_principal() -> str:
    """Muestra el menú principal y retorna la opción seleccionada."""
    print("\n" + "="*50)
    print("🎓 SISTEMA DE RECOMENDACIÓN DE APRENDIZAJE 🎓")
    print("="*50)
    print("📊 1. Ejecutar inserción de datos (rellenar grafo)")
    print("👨‍🎓 2. Consultar alumnos y progreso")
    print("🏫 3. Estadísticas de Paralelo")
    print("📈 4. Estadísticas del Sistema")
    print("🚪 0. Salir")
    return input("\n🎯 Seleccione una opción: ").strip()


def mostrar_menu_alumno(nombre: str) -> str:
    """Muestra el menú específico para un alumno."""
    print(f"\n⭐ ALUMNO: {nombre} ⭐")
    print("="*40)
    print("📊 1. Ver progreso")
    print("🎯 2. Siguiente actividad recomendada")
    print("🗺️  3. Roadmap completo")
    print("🔍 4. Análisis avanzado")
    print("↩️  0. Volver al menú principal")
    return input("\n🎯 Seleccione una opción: ").strip()

def _manejar_seleccion_alumno_por_paralelo(paralelo: str) -> None:
    """
    Maneja la selección de alumnos filtrados por paralelo específico.
    
    Args:
        paralelo: Nombre del paralelo seleccionado
    """
    from Neo4J.neo_queries import fetch_alumnos_por_paralelo
    
    # Obtener alumnos del paralelo específico
    alumnos_data = fetch_alumnos_por_paralelo(paralelo)
    if not alumnos_data:
        print(f"❌ No hay alumnos registrados en el paralelo {paralelo}")
        input("\n⏎ Presione Enter para continuar...")
        return

    # Preparar datos para selección
    alumnos_nombres = [alumno["nombre"] for alumno in alumnos_data]
    alumno_map = {alumno["nombre"]: alumno["correo"] for alumno in alumnos_data}

    while True:
        limpiar_consola()
        print(f"\n👥 ALUMNOS DEL PARALELO")
        print("=" * 50)
        print(f"🏫 Paralelo: {paralelo}")
        print(f"📊 Total alumnos: {len(alumnos_nombres)}")
        print("\n↩️  0. Volver a selección de paralelo")
        
        for idx, nombre in enumerate(alumnos_nombres, start=1):
            print(f"{idx}. 👤 {nombre}")

        try:
            seleccion = int(input(f"\n🎯 Seleccione un alumno (0-{len(alumnos_nombres)}): ").strip())
        except ValueError:
            print("❌ Ingrese un número válido")
            input("\n⏎ Presione Enter para continuar...")
            continue

        if seleccion == 0:
            break
        elif 1 <= seleccion <= len(alumnos_nombres):
            alumno_nombre = alumnos_nombres[seleccion - 1]
            alumno_correo = alumno_map[alumno_nombre]

            while True:
                limpiar_consola()
                print(f"\n⭐ ALUMNO: {alumno_nombre}")
                print(f"🏫 Paralelo: {paralelo}")
                print("=" * 50)
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
                    break
                else:
                    print("❌ Opción no válida")

                input("\n⏎ Presione Enter para continuar...")

        else:
            print(f"❌ Ingrese un número entre 0 y {len(alumnos_nombres)}")
            input("\n⏎ Presione Enter para continuar...")

# ============================================================
# Funciones de Análisis y Presentación de Datos
# ============================================================

def _obtener_datos_analisis_detallado(analisis: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Procesa el análisis y estructura los datos para presentación.
    
    Args:
        analisis: Diccionario con datos de análisis comparativo
        
    Returns:
        Lista de actividades con datos estructurados para visualización
    """
    comparativas: List[Dict[str, Any]] = analisis.get("comparativas", [])
    datos_actividades: List[Dict[str, Any]] = []
    
    for comparativa in comparativas:
        actividad_data: Dict[str, Any] = {
            "nombre": comparativa.get('actividad', 'Desconocida'),
            "tipo": comparativa.get('tipo', 'Desconocido'),
            "puntaje": comparativa.get('puntaje_final', 0),
            "intentos": comparativa.get('total_intentos', 0),
            "tiempo_promedio": comparativa.get('duracion_promedio_alumno', 0),
            "tiempo_grupo": comparativa.get('duracion_promedio_global'),
            "diferencia_porcentual": comparativa.get('diferencia_porcentual', 0),
            "categoria_eficiencia": comparativa.get('eficiencia'),
            "mensaje_eficiencia": "",
            "emoji_eficiencia": ""
        }
        
        # Calcular mensaje de eficiencia basado en diferencia porcentual
        diferencia = actividad_data["diferencia_porcentual"]
        if diferencia < -10:
            actividad_data["mensaje_eficiencia"] = f"🚀 Eres {abs(diferencia):.1f}% más rápido que el promedio"
        elif diferencia > 10:
            actividad_data["mensaje_eficiencia"] = f"🐢 Estás {diferencia:.1f}% más lento que el promedio"
        else:
            actividad_data["mensaje_eficiencia"] = "📊 Tu tiempo está en el promedio"
        
        # Asignar emoji según categoría de eficiencia
        if actividad_data["categoria_eficiencia"]:
            emoji_eficiencia = {
                "MUY_EFICIENTE": "🚀",
                "EFICIENTE": "⚡", 
                "PROMEDIO": "📊",
                "LENTO": "🐢",
                "MUY_LENTO": "⏰"
            }.get(actividad_data["categoria_eficiencia"], "📌")
            actividad_data["emoji_eficiencia"] = emoji_eficiencia
        
        datos_actividades.append(actividad_data)
    
    return datos_actividades


def _mostrar_analisis_detallado(analisis: Dict[str, Any]) -> None:
    """Muestra el análisis detallado actividad por actividad con formato visual."""
    print(f"\n🔍 ANÁLISIS DETALLADO POR ACTIVIDAD")
    print("=" * 70)
    
    actividades_data = _obtener_datos_analisis_detallado(analisis)
    
    for actividad in actividades_data:
        # Emoji según tipo de actividad
        tipo_emoji = "❓"
        if "cuestionario" in actividad['tipo'].lower():
            tipo_emoji = "📝"
        elif "ayudantía" in actividad['tipo'].lower() or "ayudantia" in actividad['tipo'].lower():
            tipo_emoji = "👨‍🏫"
        
        print(f"\n{tipo_emoji} {actividad['nombre']} ({actividad['tipo']})")
        print(f"   🎯 Puntaje: {actividad['puntaje']}% - 🔄 Intentos: {actividad['intentos']}")
        print(f"   ⏱️  Tu tiempo promedio: {formatear_tiempo_analisis(actividad['tiempo_promedio'])}")
        
        if actividad['tiempo_grupo'] is not None:
            print(f"   📊 Tiempo promedio del grupo: {formatear_tiempo_analisis(actividad['tiempo_grupo'])}")
            print(f"   {actividad['mensaje_eficiencia']}")
            
            if actividad['emoji_eficiencia'] and actividad['categoria_eficiencia']:
                print(f"   🏷️  Categoría: {actividad['categoria_eficiencia'].replace('_', ' ').title()} {actividad['emoji_eficiencia']}")


def _obtener_resumen_analisis(analisis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Genera un resumen estadístico del análisis comparativo.
    
    Args:
        analisis: Datos de análisis completos
        
    Returns:
        Resumen con métricas agregadas y tops de actividades
    """
    actividades_data = _obtener_datos_analisis_detallado(analisis)
    
    if not actividades_data:
        return {}
    
    # Calcular estadísticas generales
    total_actividades = len(actividades_data)
    actividades_eficientes = len([a for a in actividades_data if a['diferencia_porcentual'] < -10])
    actividades_promedio = len([a for a in actividades_data if -10 <= a['diferencia_porcentual'] <= 10])
    actividades_lentas = len([a for a in actividades_data if a['diferencia_porcentual'] > 10])
    
    # Identificar actividades más problemáticas
    actividades_mas_lentas = sorted(
        [a for a in actividades_data if a['diferencia_porcentual'] > 0],
        key=lambda x: x['diferencia_porcentual'],
        reverse=True
    )[:3]
    
    # Identificar actividades más eficientes
    actividades_mas_eficientes = sorted(
        [a for a in actividades_data if a['diferencia_porcentual'] < 0],
        key=lambda x: x['diferencia_porcentual']
    )[:3]
    
    return {
        "total_actividades": total_actividades,
        "actividades_eficientes": actividades_eficientes,
        "actividades_promedio": actividades_promedio,
        "actividades_lentas": actividades_lentas,
        "actividades_mas_lentas": actividades_mas_lentas,
        "actividades_mas_eficientes": actividades_mas_eficientes,
        "porcentaje_lentas": (actividades_lentas / total_actividades * 100) if total_actividades > 0 else 0
    }


def _mostrar_resumen_analisis(analisis: Dict[str, Any]) -> None:
    """Muestra un resumen ejecutivo del análisis con métricas clave."""
    resumen = _obtener_resumen_analisis(analisis)
    
    if not resumen:
        print("❌ No hay datos suficientes para generar resumen")
        return
    
    print(f"\n📊 RESUMEN EJECUTIVO DEL ANÁLISIS")
    print("=" * 50)
    print(f"📦 Total de actividades analizadas: {resumen['total_actividades']}")
    print(f"🚀 Eficientes: {resumen['actividades_eficientes']} actividades")
    print(f"📊 En promedio: {resumen['actividades_promedio']} actividades") 
    print(f"🐢 Necesitan mejora: {resumen['actividades_lentas']} actividades")
    
    if resumen['actividades_mas_lentas']:
        print(f"\n🔴 TOP 3 ACTIVIDADES QUE NECESITAN MÁS ATENCIÓN:")
        for i, actividad in enumerate(resumen['actividades_mas_lentas'], 1):
            print(f"   {i}. {actividad['nombre']} (+{actividad['diferencia_porcentual']:.1f}% tiempo)")
    
    if resumen['actividades_mas_eficientes']:
        print(f"\n🟢 TOP 3 ACTIVIDADES MÁS EFICIENTES:")
        for i, actividad in enumerate(resumen['actividades_mas_eficientes'], 1):
            print(f"   {i}. {actividad['nombre']} ({abs(actividad['diferencia_porcentual']):.1f}% más rápido)")


# ============================================================
# Funciones Principales de Opciones del Menú
# ============================================================

def ver_progreso_alumno(correo: str) -> None:
    """
    Muestra el progreso detallado de un alumno con estadísticas agrupadas.
    
    Args:
        correo: Correo electrónico del alumno a consultar
    """
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("❌ No hay progreso registrado para este alumno")
        return
    
    print("\n📊 PROGRESO DEL ALUMNO")
    print("=" * 60)
    
    # Estadísticas rápidas
    total_actividades = len(progreso)
    intentos = len([p for p in progreso if p.get("estado") == "Intento"])
    completados = len([p for p in progreso if p.get("estado") == "Completado"])
    perfectos = len([p for p in progreso if p.get("estado") == "Perfecto"])
    
    print(f"📈 Resumen General:")
    print(f"   🔄 Intentos: {intentos}")
    print(f"   ✅ Completados: {completados}")
    print(f"   🏆 Perfectos: {perfectos}")
    print(f"   📦 Total actividades: {total_actividades}")
    
    if total_actividades > 0:
        porcentaje_completado = ((completados + perfectos) / total_actividades) * 100
        print(f"   📊 Progreso general: {porcentaje_completado:.1f}%")
    
    # Agrupar actividades por estado
    actividades_por_estado: Dict[str, List[Dict[str, Any]]] = {
        "🟡 EN PROGRESO": [p for p in progreso if p.get("estado") == "Intento"],
        "🟢 COMPLETADAS": [p for p in progreso if p.get("estado") == "Completado"],
        "🏆 PERFECTAS": [p for p in progreso if p.get("estado") == "Perfecto"]
    }
    
    print("\n📋 DETALLE AGRUPADO POR ESTADO")
    print("=" * 60)
    
    for estado, actividades in actividades_por_estado.items():
        if actividades:
            print(f"\n{estado} ({len(actividades)} actividades):")
            print("-" * 40)
            
            # Agrupar por tipo de actividad
            actividades_agrupadas_por_tipo: Dict[str, List[Dict[str, Any]]] = {}
            for actividad in actividades:
                tipo_act: str = str(actividad.get('tipo', 'Desconocido'))
                if tipo_act not in actividades_agrupadas_por_tipo:
                    actividades_agrupadas_por_tipo[tipo_act] = []
                actividades_agrupadas_por_tipo[tipo_act].append(actividad)
            
            for tipo_act, lista_act in actividades_agrupadas_por_tipo.items():
                # Emoji según tipo de actividad
                tipo_emoji = "📝" if "cuestionario" in tipo_act.lower() else "👨‍🏫" if "ayudantía" in tipo_act.lower() or "ayudantia" in tipo_act.lower() else "📚"
                
                print(f"  {tipo_emoji} {tipo_act} ({len(lista_act)}):")
                for act in lista_act:
                    nombre_act: str = str(act.get('nombre', 'Sin nombre'))
                    # Mostrar información adicional si está disponible
                    info_extra = ""
                    if act.get('score'):
                        score_val = act.get('score')
                        info_extra = f" - 🎯 Puntaje: {score_val}%"
                    elif act.get('duration_seconds'):
                        duration_val = act.get('duration_seconds', 0)
                        minutos: int = int(duration_val) // 60
                        info_extra = f" - ⏱️ Tiempo: {minutos}min"
                    
                    print(f"     • {nombre_act}{info_extra}")
    
    # Resumen por tipo de actividad
    print("\n📊 RESUMEN POR TIPO DE ACTIVIDAD")
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
        tipo_emoji = "📝" if "cuestionario" in tipo_act.lower() else "👨‍🏫" if "ayudantía" in tipo_act.lower() or "ayudantia" in tipo_act.lower() else "📚"
        
        print(f"  {tipo_emoji} {tipo_act}:")
        print(f"     • 📦 Total: {estadisticas['total']}")
        if estadisticas['intentos'] > 0:
            print(f"     • 🔄 En progreso: {estadisticas['intentos']}")
        if estadisticas['completados'] > 0:
            print(f"     • ✅ Completadas: {estadisticas['completados']}")
        if estadisticas['perfectos'] > 0:
            print(f"     • 🏆 Perfectas: {estadisticas['perfectos']}")


def ver_siguiente_actividad_alumno(correo: str) -> None:
    """
    Muestra la siguiente actividad recomendada para un alumno con análisis contextual.
    
    Args:
        correo: Correo electrónico del alumno
    """
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("❌ No hay progreso registrado para este alumno")
        siguiente = fetch_siguiente_actividad(correo)
        if siguiente:
            print(f"\n🎯 RECOMENDACIÓN PARA COMENZAR:")
            print(f"   🚀 Comienza con: '{siguiente.get('nombre')}'")
            print(f"   📍 Es tu primera actividad en el sistema")
            print(f"   🎯 Objetivo: Familiarizarte con la plataforma")
        return
    
    sugerencia = recomendar_siguiente_from_progress(progreso)
    if not sugerencia:
        print("🎉 ¡FELICITACIONES!")
        print("=" * 50)
        print("🏆 Has alcanzado un logro importante:")
        print("   • ✅ Todas las actividades están completadas o en estado perfecto")
        print("   • 📚 Has dominado el material disponible")
        print("   • 🎯 Objetivo cumplido: Aprendizaje completo")
        print(f"\n📋 Próximos pasos sugeridos:")
        print("   • 🔄 Repasar temas que necesites reforzar")
        print("   • ⏳ Esperar nuevas actividades del profesor")
        print("   • 📊 Revisar tu progreso para mantener el nivel")
        return
    
    act = sugerencia['actividad']
    estrategia = sugerencia['estrategia']
    
    print("\n🎯 SIGUIENTE ACTIVIDAD RECOMENDADA")
    print("=" * 60)
    
    # Configuración visual según estrategia
    estrategia_config = {
        "refuerzo": ("🔄 REFUERZO - TERMINAR ACTIVIDAD PENDIENTE", "🟡"),
        "mejora": ("⭐ MEJORA - BUSCAR LA PERFECCIÓN", "🟠"), 
        "nuevas": ("🚀 NUEVO DESAFÍO", "🔵"),
        "inicio": ("🎯 INICIO - COMENZAR EL APRENDIZAJE", "🟢")
    }.get(estrategia, (estrategia.upper(), "📌"))
    
    titulo, emoji_estrategia = estrategia_config
    
    print(f"{emoji_estrategia} {titulo}")
    print("-" * 50)
    
    # Emoji según tipo de actividad
    tipo_emoji = "📝" if "cuestionario" in str(act.get('tipo', '')).lower() else "👨‍🏫" if "ayudantía" in str(act.get('tipo', '')).lower() or "ayudantia" in str(act.get('tipo', '')).lower() else "📚"
    
    print(f"{tipo_emoji} Tipo: {act.get('tipo', 'Desconocido')}")
    print(f"📖 Actividad: {act.get('nombre', 'Sin nombre')}")
    
    # Mensajes específicos según estrategia
    if estrategia == "refuerzo":
        print(f"\n🔍 ANÁLISIS DE TU SITUACIÓN:")
        print(f"   🟡 Esta actividad está en estado 'Intento'")
        print(f"   📅 Es tu actividad más antigua sin completar")
        print(f"   ⚠️  Necesita tu atención prioritaria")
        
        print(f"\n📋 PLAN DE ACCIÓN RECOMENDADO:")
        print(f"   📚 1. Revisa el material: Consulta los RAPs relacionados")
        print(f"   🔍 2. Identifica dificultades: ¿Qué conceptos te causan problemas?")
        print(f"   💪 3. Practica: Resuelve ejercicios similares")
        print(f"   🎯 4. Reintenta: Completa la actividad al 100%")
        
    elif estrategia == "mejora":
        print(f"\n🔍 ANÁLISIS DE TU SITUACIÓN:")
        print(f"   🟢 Esta actividad está 'Completada' pero no 'Perfecta'")
        print(f"   📈 Tienes oportunidad de mejorar tu calificación")
        print(f"   🎯 Estás cerca del dominio total del tema")
        
        print(f"\n📋 PLAN DE ACCIÓN RECOMENDADO:")
        print(f"   📊 1. Revisa errores: ¿Dónde perdiste puntos?")
        print(f"   📚 2. Profundiza: Estudia los conceptos específicos")
        print(f"   🎯 3. Practica selectiva: Enfócate en tus áreas débiles")
        print(f"   ⭐ 4. Perfecciona: Busca el 100% de calificación")
        
    elif estrategia == "nuevas":
        print(f"\n🔍 ANÁLISIS DE TU SITUACIÓN:")
        print(f"   🏆 Tienes actividades en estado 'Perfecto'")
        print(f"   📚 Demuestras dominio de los temas anteriores")
        print(f"   🚀 Estás listo para nuevos desafíos")
        
        print(f"\n📋 PLAN DE ACCIÓN RECOMENDADO:")
        print(f"   🔄 1. Mantén el ritmo: Sigue con la misma dedicación")
        print(f"   🔗 2. Aplica conocimiento: Usa lo aprendido en lo nuevo")
        print(f"   🧠 3. Conecta conceptos: Relaciona con temas anteriores")
        print(f"   📈 4. Profundiza: Ve más allá de lo básico")
        
    elif estrategia == "inicio":
        print(f"\n🔍 ANÁLISIS DE TU SITUACIÓN:")
        print(f"   🎯 Estás comenzando tu journey de aprendizaje")
        print(f"   🚀 Esta es tu primera actividad recomendada")
        print(f"   📍 Es el punto de partida ideal")
        
        print(f"\n📋 PLAN DE ACCIÓN RECOMENDADO:")
        print(f"   🏠 1. Familiarízate: Conoce la plataforma")
        print(f"   ⏰ 2. Establece ritmo: Encuentra tu horario ideal")
        print(f"   📚 3. Consulta material: Usa los RAPs como guía")
        print(f"   🎯 4. Avanza progresivamente: Paso a paso")
    
    # Resumen final motivacional
    print(f"\n📊 RESUMEN FINAL")
    print("-" * 30)
    
    total = len(progreso)
    completados = len([p for p in progreso if p.get("estado") in ["Completado", "Perfecto"]])
    
    if total > 0:
        progreso_porcentaje = (completados / total) * 100
        
        # Emoji según nivel de progreso
        progreso_emoji = "🎯" if progreso_porcentaje == 0 else "🚀" if progreso_porcentaje < 50 else "⭐" if progreso_porcentaje < 80 else "🏆"
        
        print(f"{progreso_emoji} Tu progreso general: {progreso_porcentaje:.1f}%")
        
        if progreso_porcentaje == 0:
            print("💪 ¡Comienza hoy mismo! Cada viaje empieza con un primer paso")
        elif progreso_porcentaje < 50:
            print("🚀 ¡Vas por buen camino! Sigue construyendo tu base")
        elif progreso_porcentaje < 80:
            print("⭐ ¡Excelente progreso! La consistencia es tu aliada")
        else:
            print("🏆 ¡Impresionante! Estás cerca de dominar todo el material")


def ver_roadmap_alumno(correo: str) -> None:
    """
    Muestra el roadmap completo de aprendizaje para un alumno.
    
    Args:
        correo: Correo electrónico del alumno
    """
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("❌ No hay progreso registrado para este alumno")
        return
    
    # Obtener actividades lentas para análisis de eficiencia
    actividades_lentas = []
    
    try:
        print("⏱️ Analizando eficiencia en tiempo...")
        actividades_lentas = fetch_actividades_lentas_alumno(correo)
        
        if actividades_lentas:
            print(f"📊 Se encontraron {len(actividades_lentas)} actividades donde puedes mejorar tu eficiencia")
        else:
            print("✅ Tu ritmo de trabajo está dentro del promedio")
            
    except Exception as e:
        print(f"❌ No se pudieron analizar actividades lentas: {e}")
        actividades_lentas = []
    
    def fetch_next_activity():
        return fetch_siguiente_actividad(correo)
    
    # Generar roadmap con actividades lentas incluidas
    roadmap = generar_roadmap_from_progress_and_fetcher(progreso, fetch_next_activity, actividades_lentas)
    
    if not roadmap:
        print("🎉 ¡Felicidades! Has completado todas las actividades disponibles")
        return
    
    # Mostrar estadísticas del roadmap
    estrategias_count: Dict[str, int] = {}
    for r in roadmap:
        estrategia: str = r["estrategia"]
        estrategias_count[estrategia] = estrategias_count.get(estrategia, 0) + 1
    
    print("\n🗺️ ROADMAP DE APRENDIZAJE")
    print("=" * 60)
    print(f"📦 Total de recomendaciones: {len(roadmap)}")
    
    print(f"\n🎯 JERARQUÍA DE PRIORIDADES:")
    print("-" * 30)
    
    estrategias_info = {
        "refuerzo": ("🔄 TERMINAR ACTIVIDADES PENDIENTES", estrategias_count.get("refuerzo", 0)),
        "mejora": ("⭐ MEJORAR CALIFICACIONES", estrategias_count.get("mejora", 0)),
        "refuerzo_tiempo": ("⏱️ MEJORAR EFICIENCIA", estrategias_count.get("refuerzo_tiempo", 0)),
        "nuevas": ("🚀 NUEVOS DESAFÍOS", estrategias_count.get("nuevas", 0))
    }
    
    for estrategia, (descripcion, count) in estrategias_info.items():
        if count > 0:
            print(f"   {descripcion}: {count} actividades")
    
    print("\n" + "=" * 70)
    
    # Mostrar actividades en orden
    for i, r in enumerate(roadmap, 1):
        act: Dict[str, Any] = r['actividad']
        estrategia: str = r['estrategia']
        
        # Configuración visual según estrategia
        estrategia_config = {
            "nuevas": ("🚀 NUEVO DESAFÍO", "Comienza un nuevo tema", "🔵"),
            "refuerzo": ("🔄 TERMINAR PENDIENTE", "Completa esta actividad", "🟡"), 
            "mejora": ("⭐ BUSCAR PERFECTO", "Mejora tu calificación", "🟠"),
            "refuerzo_tiempo": ("⏱️ MEJORAR EFICIENCIA", "Optimiza tu tiempo", "🟣")
        }.get(estrategia, (estrategia.upper(), "", "📌"))
        
        texto_estrategia, descripcion, color_emoji = estrategia_config
        
        print(f"\n{color_emoji} {i}. {texto_estrategia}")
        
        # Emoji según tipo de actividad
        tipo_emoji = "📝" if "cuestionario" in str(act.get('tipo', '')).lower() else "👨‍🏫" if "ayudantía" in str(act.get('tipo', '')).lower() or "ayudantia" in str(act.get('tipo', '')).lower() else "📚"
        
        print(f"   {tipo_emoji} {act.get('tipo', 'Actividad')}")
        print(f"   📖 {act.get('nombre', 'Sin nombre')}")
        print(f"   💡 {descripcion}")
        
        # Información específica para actividades de mejora de tiempo
        if estrategia == "refuerzo_tiempo" and act.get('diferencia_porcentual'):
            print(f"   📊 Eficiencia: +{act['diferencia_porcentual']:.1f}% vs promedio")
            if act.get('tiempo_promedio_alumno') and act.get('tiempo_promedio_global'):
                tiempo_alumno = formatear_tiempo_analisis(act['tiempo_promedio_alumno'])
                tiempo_promedio = formatear_tiempo_analisis(act['tiempo_promedio_global'])
                print(f"   ⏱️ Tiempos: Tú: {tiempo_alumno} | Promedio: {tiempo_promedio}")
        
        # Mostrar motivo específico si está disponible
        if r.get('motivo'):
            print(f"   📌 {r['motivo']}")
        
        # Línea separadora cada 3 actividades para mejor legibilidad
        if i % 3 == 0 and i < len(roadmap):
            print("   " + "─" * 50)


def ver_analisis_avanzado_alumno(correo: str) -> None:
    """
    Muestra análisis avanzado de rendimiento comparado con el grupo.
    
    Args:
        correo: Correo electrónico del alumno
    """
    print("\n🔍 ANÁLISIS AVANZADO DE RENDIMIENTO")
    print("=" * 60)
    print("📊 Analizando tu desempeño comparado con el grupo...")
    
    # Obtener progreso del alumno para mostrar estado actual
    progreso = fetch_progreso_alumno(correo)
    if not progreso:
        print("❌ No hay progreso registrado para este alumno")
        return
    
    # Mostrar estado actual del alumno
    total_actividades = len(progreso)
    intentos = len([p for p in progreso if p.get("estado") == "Intento"])
    completados = len([p for p in progreso if p.get("estado") == "Completado"])
    perfectos = len([p for p in progreso if p.get("estado") == "Perfecto"])
    
    print(f"\n📈 TU ESTADO ACTUAL:")
    print("-" * 25)
    print(f"📦 Actividades totales: {total_actividades}")
    print(f"🟡 En progreso: {intentos}")
    print(f"🟢 Completadas: {completados}")
    print(f"🏆 Perfectas: {perfectos}")
    
    if total_actividades > 0:
        progreso_porcentaje = ((completados + perfectos) / total_actividades) * 100
        print(f"📊 Progreso general: {progreso_porcentaje:.1f}%")
    
    # Realizar análisis según el estado del alumno
    analisis: Dict[str, Any] = {}
    tiene_todo_perfecto: bool = fetch_verificar_alumno_perfecto(correo)
    
    if tiene_todo_perfecto:
        print(f"\n🎉 ¡FELICITACIONES! Tienes todas las actividades en estado 'Perfecto'")
        print("📈 Procediendo con análisis comparativo completo...")
        analisis = analizar_rendimiento_comparativo(
            correo,
            fetch_verificar_alumno_perfecto,
            fetch_estadisticas_globales,
            fetch_estadisticas_alumno
        )
    else:
        print(f"\n📊 Análisis básico disponible (análisis completo requiere todas las actividades en 'Perfecto')")
        # Análisis básico con información disponible
        stats_globales = fetch_estadisticas_globales()
        stats_alumno = fetch_estadisticas_alumno(correo)
        
        analisis = {
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
            "nota": "📝 Análisis básico - Para análisis completo completa todas las actividades"
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
        print(f"\n❌ {analisis['error']}")
        return
    
    if not analisis.get("comparativas"):
        print("❌ No hay suficientes datos de tiempo para realizar el análisis")
        print("📝 Las actividades necesitan tener registro de duración")
        return
    
    # Mostrar resumen ejecutivo primero
    _mostrar_resumen_analisis(analisis)
    
    # Resumen general
    resumen: Dict[str, Any] = analisis["resumen_general"]
    print(f"\n📊 RESUMEN DE TU DESEMPEÑO")
    print("-" * 40)
    print(f"📦 Actividades analizadas: {resumen.get('actividades_analizadas', 0)}")
    tiempo_total: float = resumen.get('tiempo_total_alumno', 0)
    if tiempo_total > 0:
        print(f"⏱️ Tiempo total invertido: {formatear_tiempo_analisis(tiempo_total)}")
    
    # Mostrar insights generados
    insights: Dict[str, List[str]] = analisis["insights"]
    
    if insights.get("fortalezas"):
        print(f"\n🟢 TUS FORTALEZAS")
        print("-" * 25)
        for fortaleza in insights["fortalezas"]:
            print(f"  💪 {fortaleza}")
    
    if insights.get("areas_mejora"):
        print(f"\n🔴 ÁREAS DE MEJORA")
        print("-" * 25)
        for mejora in insights["areas_mejora"]:
            print(f"  📌 {mejora}")
    
    if insights.get("recomendaciones"):
        print(f"\n🎯 RECOMENDACIONES")
        print("-" * 20)
        for recomendacion in insights["recomendaciones"]:
            print(f"  💡 {recomendacion}")
    
    # Opción para ver análisis detallado
    if analisis.get("comparativas"):
        print(f"\n🔍 ¿Ver análisis detallado por actividad? (s/n): ", end="")
        if input().strip().lower() == 's':
            _mostrar_analisis_detallado(analisis)


def _generar_insights_basicos(analisis: Dict[str, Any], tiene_todo_perfecto: bool) -> None:
    """
    Genera insights básicos para alumnos que no tienen todas las actividades en perfecto.
    
    Args:
        analisis: Diccionario con datos de análisis
        tiene_todo_perfecto: Indica si el alumno tiene todo perfecto
    """
    comparativas: List[Dict[str, Any]] = analisis.get("comparativas", [])
    insights: Dict[str, List[str]] = analisis["insights"]
    
    # Identificar actividades eficientes y lentas
    actividades_eficientes = [c for c in comparativas if c.get("eficiencia") in ["MUY_EFICIENTE", "EFICIENTE"]]
    actividades_lentas = [c for c in comparativas if c.get("eficiencia") in ["LENTO", "MUY_LENTO"]]
    
    # Generar fortalezas
    if actividades_eficientes:
        insights["fortalezas"].append(f"🚀 Eres eficiente en {len(actividades_eficientes)} actividades")
        for act in actividades_eficientes[:2]:
            tiempo_ahorrado: float = -act.get("diferencia_porcentual", 0)
            insights["fortalezas"].append(f"   • {act['actividad']}: {tiempo_ahorrado:.1f}% más rápido que el promedio")
    
    # Generar áreas de mejora
    if actividades_lentas:
        insights["areas_mejora"].append(f"🐢 Puedes mejorar tu ritmo en {len(actividades_lentas)} actividades")
        for act in actividades_lentas[:2]:
            tiempo_extra: float = act.get("diferencia_porcentual", 0)
            insights["areas_mejora"].append(f"   • {act['actividad']}: {tiempo_extra:.1f}% más lento que el promedio")
    
    # Recomendaciones según el progreso
    if not tiene_todo_perfecto:
        actividades_perfectas = [c for c in comparativas if c.get("estado_final") == "Perfecto"]
        actividades_completadas = [c for c in comparativas if c.get("estado_final") == "Completado"]
        actividades_intento = [c for c in comparativas if c.get("estado_final") == "Intento"]
        
        if actividades_intento:
            insights["recomendaciones"].append("🔄 Enfócate en completar las actividades en estado 'Intento'")
        if actividades_completadas:
            insights["recomendaciones"].append("⭐ Busca alcanzar 'Perfecto' en las actividades completadas")
        if actividades_perfectas:
            insights["recomendaciones"].append("🏆 Mantén tu excelencia en las actividades perfectas")
        
        insights["recomendaciones"].append("🎯 Completa todas las actividades para obtener un análisis completo")


def ver_estadisticas_sistema() -> None:
    """Muestra estadísticas generales del sistema."""
    print("\n📈 ESTADÍSTICAS DEL SISTEMA")
    print("=" * 50)
    mostrar_estadisticas_rapidas()
    input("\n⏎ Presione Enter para continuar...")

# ============================================================
# FUNCIONES DE ESTADÍSTICAS DE PARALELO
# ============================================================

def mostrar_menu_paralelos() -> str:
    """Muestra el menú de estadísticas de paralelo."""
    print("\n" + "="*40)
    print("🏫 ESTADÍSTICAS DE PARALELO")
    print("="*40)
    print("📋 1. Ver lista de paralelos disponibles")
    print("🔍 2. Analizar paralelo específico")
    print("↩️  0. Volver al menú principal")
    return input("\n🎯 Seleccione una opción: ").strip()


def ver_lista_paralelos() -> None:
    """Muestra la lista de todos los paralelos disponibles."""
    from Neo4J.neo_queries import fetch_paralelos_disponibles
    from Neo4J.consultar import obtener_lista_paralelos_procesada
    
    print("\n🏫 PARALELOS DISPONIBLES")
    print("=" * 40)
    
    try:
        paralelos = obtener_lista_paralelos_procesada(fetch_paralelos_disponibles)
        
        if not paralelos:
            print("❌ No se encontraron paralelos en la base de datos")
            return
        
        print(f"📊 Total de paralelos encontrados: {len(paralelos)}\n")
        
        for i, paralelo in enumerate(paralelos, 1):
            print(f"{i}. 🏫 {paralelo}")
            
    except Exception as e:
        print(f"❌ Error obteniendo lista de paralelos: {e}")


def analizar_paralelo_especifico() -> None:
    """Permite seleccionar y analizar un paralelo específico."""
    from Neo4J.neo_queries import fetch_paralelos_disponibles, fetch_detalle_paralelo
    from Neo4J.consultar import obtener_lista_paralelos_procesada, generar_reporte_paralelo_completo
    
    print("\n🔍 ANALIZAR PARALELO ESPECÍFICO")
    print("=" * 40)
    
    try:
        # Obtener lista de paralelos
        paralelos = obtener_lista_paralelos_procesada(fetch_paralelos_disponibles)
        
        if not paralelos:
            print("❌ No se encontraron paralelos en la base de datos")
            return
        
        # Mostrar lista numerada
        print("🏫 Paralelos disponibles:\n")
        for i, paralelo in enumerate(paralelos, 1):
            print(f"{i}. 🏫 {paralelo}")
        
        # Seleccionar paralelo
        try:
            seleccion = int(input(f"\n🎯 Seleccione un paralelo (1-{len(paralelos)}): ").strip())
        except ValueError:
            print("❌ Ingrese un número válido")
            return
        
        if not (1 <= seleccion <= len(paralelos)):
            print(f"❌ Ingrese un número entre 1 y {len(paralelos)}")
            return
        
        paralelo_seleccionado = paralelos[seleccion - 1]
        
        print(f"\n📊 Analizando paralelo: {paralelo_seleccionado}...")
        
        # Generar reporte completo
        reporte = generar_reporte_paralelo_completo(paralelo_seleccionado, fetch_detalle_paralelo)
        
        if "error" in reporte:
            print(f"❌ {reporte['error']}")
            return
        
        # Mostrar reporte
        _mostrar_reporte_paralelo(reporte)
        
    except Exception as e:
        print(f"❌ Error analizando paralelo: {e}")


def _mostrar_reporte_paralelo(reporte: Dict[str, Any]) -> None:
    """Muestra el reporte completo de un paralelo con formato visual."""
    paralelo = reporte.get("paralelo", "Desconocido")
    resumen_ejecutivo = reporte.get("resumen_ejecutivo", {})
    completitud = reporte.get("completitud", {})
    actividades_problematicas = reporte.get("actividades_problematicas", {})
    eficiencia = reporte.get("eficiencia", {})
    
    print("\n📊 REPORTE COMPLETO DEL PARALELO")
    print("=" * 60)
    print(f"🏫 Paralelo: {paralelo}")
    
    # Resumen ejecutivo
    print(f"\n📈 RESUMEN EJECUTIVO")
    print("-" * 30)
    print(f"👥 Alumnos: {resumen_ejecutivo.get('total_alumnos', 0)}")
    print(f"📦 Actividades: {resumen_ejecutivo.get('total_actividades', 0)}")
    print(f"📊 Completitud: {resumen_ejecutivo.get('porcentaje_completitud', 0):.1f}%")
    print(f"⚠️  Actividades críticas: {resumen_ejecutivo.get('actividades_criticas', 0)}")
    print(f"🚀 Mejor eficiencia: {resumen_ejecutivo.get('mejor_eficiencia', 0):.1f}%")
    print(f"🐢 Peor eficiencia: {resumen_ejecutivo.get('peor_eficiencia', 0):.1f}%")
    
    # Puntos fuertes y áreas de mejora
    puntos_fuertes = resumen_ejecutivo.get('puntos_fuertes', [])
    areas_mejora = resumen_ejecutivo.get('areas_mejora', [])
    
    if puntos_fuertes:
        print(f"\n🟢 PUNTOS FUERTES")
        print("-" * 20)
        for punto in puntos_fuertes:
            print(f"  ✅ {punto}")
    
    if areas_mejora:
        print(f"\n🔴 ÁREAS DE MEJORA")
        print("-" * 20)
        for area in areas_mejora:
            print(f"  📌 {area}")
    
    # Completitud detallada
    print(f"\n📊 COMPLETITUD DETALLADA")
    print("-" * 25)
    print(f"📦 Actividades totales: {completitud.get('total_actividades', 0)}")
    print(f"✅ Completadas por todos: {completitud.get('actividades_completadas_todos', 0)}")
    print(f"📈 Promedio por alumno: {completitud.get('promedio_completadas_por_alumno', 0):.1f}")
    print(f"📊 Completitud global: {completitud.get('porcentaje_completitud_global', 0):.1f}%")
    print(f"🏷️  Nivel: {completitud.get('nivel_completitud', 'Desconocido')}")
    
    # Actividades problemáticas
    actividades_criticas = actividades_problematicas.get('criticas', [])
    actividades_no_criticas = actividades_problematicas.get('no_criticas', [])
    total_problematicas = actividades_problematicas.get('total', 0)
    
    if total_problematicas > 0:
        print(f"\n⚠️  ACTIVIDADES CON BAJA PARTICIPACIÓN")
        print("-" * 35)
        print(f"📦 Total: {total_problematicas} actividades")
        print(f"🔴 Críticas (<25%): {len(actividades_criticas)}")
        print(f"🟡 No críticas: {len(actividades_no_criticas)}")
        
        if actividades_criticas:
            print(f"\n🔴 ACTIVIDADES CRÍTICAS (prioridad alta):")
            for i, act in enumerate(actividades_criticas[:5], 1):
                tipo_emoji = "📝" if "cuestionario" in str(act.get('tipo', '')).lower() else "👨‍🏫" if "ayudantía" in str(act.get('tipo', '')).lower() or "ayudantia" in str(act.get('tipo', '')).lower() else "📚"
                print(f"  {i}. {tipo_emoji} {act.get('nombre', 'Desconocida')} ({act.get('tipo', 'Desconocido')})")
                print(f"     📊 Participación: {act.get('porcentaje_participacion', 0):.1f}%")
        
        if actividades_no_criticas:
            print(f"\n🟡 ACTIVIDADES CON BAJA PARTICIPACIÓN:")
            for i, act in enumerate(actividades_no_criticas[:3], 1):
                tipo_emoji = "📝" if "cuestionario" in str(act.get('tipo', '')).lower() else "👨‍🏫" if "ayudantía" in str(act.get('tipo', '')).lower() or "ayudantia" in str(act.get('tipo', '')).lower() else "📚"
                print(f"  {i}. {tipo_emoji} {act.get('nombre', 'Desconocida')} ({act.get('tipo', 'Desconocido')})")
                print(f"     📊 Participación: {act.get('porcentaje_participacion', 0):.1f}%")
    
    # Eficiencia
    metricas_eficiencia = eficiencia.get('metricas_agregadas', {})
    mejores = eficiencia.get('mejores', [])
    peores = eficiencia.get('peores', [])
    insights = eficiencia.get('insights', [])
    
    print(f"\n⏱️  EFICIENCIA DE ACTIVIDADES")
    print("-" * 30)
    print(f"🚀 Mejor eficiencia: {metricas_eficiencia.get('mejor_eficiencia', 0):.1f}%")
    print(f"🐢 Peor eficiencia: {metricas_eficiencia.get('peor_eficiencia', 0):.1f}%")
    print(f"📊 Brecha de eficiencia: {metricas_eficiencia.get('brecha_eficiencia', 0):.1f}%")
    
    if mejores:
        print(f"\n🟢 TOP 3 ACTIVIDADES MÁS EFICIENTES:")
        for i, act in enumerate(mejores[:3], 1):
            tipo_emoji = "📝" if "cuestionario" in str(act.get('tipo', '')).lower() else "👨‍🏫" if "ayudantía" in str(act.get('tipo', '')).lower() or "ayudantia" in str(act.get('tipo', '')).lower() else "📚"
            print(f"  {i}. {tipo_emoji} {act.get('nombre', 'Desconocida')} ({act.get('tipo', 'Desconocido')})")
            print(f"     📊 Eficiencia: {act.get('eficiencia', 0):.1f}%")
            print(f"     🏆 Perfectos: {act.get('total_perfectos', 0)} | ✅ Completados: {act.get('total_completados', 0)}")
    
    if peores:
        print(f"\n🔴 TOP 3 ACTIVIDADES MENOS EFICIENTES:")
        for i, act in enumerate(peores[:3], 1):
            tipo_emoji = "📝" if "cuestionario" in str(act.get('tipo', '')).lower() else "👨‍🏫" if "ayudantía" in str(act.get('tipo', '')).lower() or "ayudantia" in str(act.get('tipo', '')).lower() else "📚"
            print(f"  {i}. {tipo_emoji} {act.get('nombre', 'Desconocida')} ({act.get('tipo', 'Desconocido')})")
            print(f"     📊 Eficiencia: {act.get('eficiencia', 0):.1f}%")
            print(f"     🏆 Perfectos: {act.get('total_perfectos', 0)} | ✅ Completados: {act.get('total_completados', 0)}")
    
    if insights:
        print(f"\n💡 INSIGHTS AUTOMÁTICOS:")
        for insight in insights:
            print(f"  • {insight}")


def manejar_estadisticas_paralelo() -> None:
    """Maneja el flujo completo de estadísticas de paralelo."""
    while True:
        limpiar_consola()
        opcion = mostrar_menu_paralelos()

        if opcion == "1":
            ver_lista_paralelos()
        elif opcion == "2":
            analizar_paralelo_especifico()
        elif opcion == "0":
            break
        else:
            print("❌ Opción no válida")
        
        input("\n⏎ Presione Enter para continuar...")

# ============================================================
# Bucle Principal del Sistema
# ============================================================

def main() -> None:
    """Función principal que ejecuta el bucle de la aplicación."""
    while True:
        limpiar_consola()
        opcion = mostrar_menu_principal()

        if opcion == "1":
            print("\n📊 Ejecutando inserción de datos...")
            print("⏳ Esto puede tomar unos momentos...")
            rellenarGrafo()
            input("\n✅ Inserción completada. Presione Enter para continuar...")

        elif opcion == "2":
            # PRIMERO: Seleccionar paralelo
            from Neo4J.neo_queries import fetch_paralelos_disponibles
            from Neo4J.consultar import obtener_lista_paralelos_procesada
            
            paralelos = obtener_lista_paralelos_procesada(fetch_paralelos_disponibles)
            if not paralelos:
                print("❌ No hay paralelos registrados en el sistema")
                print("📝 Ejecute primero la opción 1 para insertar datos")
                input("\n⏎ Presione Enter para continuar...")
                continue

            # Seleccionar paralelo
            while True:
                limpiar_consola()
                print("\n🏫 SELECCIONAR PARALELO")
                print("=" * 40)
                print("↩️  0. Volver al menú principal")
                
                for idx, paralelo in enumerate(paralelos, start=1):
                    print(f"{idx}. 🏫 {paralelo}")

                try:
                    seleccion_paralelo = int(input(f"\n🎯 Seleccione un paralelo (0-{len(paralelos)}): ").strip())
                except ValueError:
                    print("❌ Ingrese un número válido")
                    input("\n⏎ Presione Enter para continuar...")
                    continue

                if seleccion_paralelo == 0:
                    break
                elif 1 <= seleccion_paralelo <= len(paralelos):
                    paralelo_seleccionado = paralelos[seleccion_paralelo - 1]
                    
                    # SEGUNDO: Seleccionar alumno del paralelo elegido
                    _manejar_seleccion_alumno_por_paralelo(paralelo_seleccionado)
                    break
                else:
                    print(f"❌ Ingrese un número entre 0 y {len(paralelos)}")
                    input("\n⏎ Presione Enter para continuar...")

        elif opcion == "3":
            manejar_estadisticas_paralelo()

        elif opcion == "4":
            ver_estadisticas_sistema()

        elif opcion == "0":
            print("\n👋 ¡Hasta pronto!")
            print("🔌 Cerrando conexiones...")
            driver.close()
            break

        else:
            print("❌ Opción no válida")
            input("\n⏎ Presione Enter para continuar...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Programa interrumpido por el usuario")
        driver.close()
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        driver.close()