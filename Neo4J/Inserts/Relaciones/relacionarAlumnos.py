"""
Módulo de Relacionamiento de Alumnos - Gestión de Progreso y Actividades en Neo4J

Este módulo se encarga de establecer las relaciones entre alumnos y actividades educativas
(cuestionarios y ayudantías) basándose en datos de progreso provenientes de archivos CSV.
Incluye el procesamiento de estados, calificaciones, duraciones y fechas para crear
un historial completo del progreso estudiantil.

Funciones principales:
    - relacionar_alumnos: Proceso principal de relacionamiento masivo
    - procesar_csv: Procesamiento individual de archivos CSV
    - crear_relacion: Función genérica para crear relaciones en Neo4J
    - Funciones de parseo: Conversión de formatos españoles a estándares

Características:
    - Procesamiento de fechas en formato español
    - Conversión de duraciones a segundos
    - Determinación automática de estados (Intento/Completado/Perfecto)
    - Validación exhaustiva de datos
    - Manejo robusto de errores por alumno

Estructura de datos manejada:
    - Estados: Intento, Completado, Perfecto
    - Actividades: Cuestionario, Ayudantia
    - Métricas: Duración, Calificación, Fechas
"""

from pathlib import Path
from typing import Literal, Optional, Dict, List, Tuple
import re
from datetime import datetime
import pandas as pd
from neo4j import Driver, ManagedTransaction
import logging

# Configuración de logging para seguimiento de operaciones
logger = logging.getLogger(__name__)

# ----------------------------
# Tipos de relaciones y validaciones
# ----------------------------

# Tipo literal para relaciones válidas
TipoRelacion = Literal["Intento", "Completado", "Perfecto"]

# Conjuntos de validación para relaciones y nodos
VALID_RELACIONES: set[str] = {"Intento", "Completado", "Perfecto"}
VALID_NODOS: set[str] = {"Cuestionario", "Ayudantia"}

# ----------------------------
# Helpers: parseo de campos del CSV
# ----------------------------

# Mapeo de meses en español a números
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

def obtener_actividades_bd(driver: Driver) -> Dict[str, List[str]]:
    """
    Obtiene todas las actividades existentes en la base de datos.
    
    Consulta la base de datos Neo4J para obtener listas completas de 
    cuestionarios y ayudantías disponibles para relacionamiento.
    
    Args:
        driver: Driver de conexión a Neo4J
        
    Returns:
        Dict con listas de nombres de cuestionarios y ayudantías
    """
    actividades: Dict[str, List[str]] = {"cuestionarios": [], "ayudantias": []}
    
    try:
        with driver.session() as session:
            # Obtener cuestionarios
            result_c = session.run("MATCH (c:Cuestionario) RETURN c.nombre as nombre")
            actividades["cuestionarios"] = [record["nombre"] for record in result_c if record["nombre"]]
            
            # Obtener ayudantías
            result_a = session.run("MATCH (a:Ayudantia) RETURN a.nombre as nombre")
            actividades["ayudantias"] = [record["nombre"] for record in result_a if record["nombre"]]
            
        logger.info(f"Actividades en BD: {len(actividades['cuestionarios'])} cuestionarios, {len(actividades['ayudantias'])} ayudantías")
        return actividades
        
    except Exception as e:
        logger.error(f"Error obteniendo actividades de BD: {e}")
        return actividades
    
def limpiar_nombre_archivo_relaciones(nombre_archivo: str) -> Optional[str]:
    """
    Limpia nombres de archivos CSV para extraer el nombre de la actividad.
    
    Remueve prefijos de curso, códigos de paralelo y sufijos de calificaciones
    para obtener el nombre limpio de la actividad.
    
    Args:
        nombre_archivo: Nombre original del archivo CSV
        
    Returns:
        str: Nombre limpio y normalizado o None si no se puede procesar
    """
    try:
        nombre_raw = Path(nombre_archivo).stem
        
        # Patrones para remover información de paralelo y código del curso
        patrones_prefijo = [
            r'^INF1211-1234-\(1S2025\)-P\d+[\s_-]*',
            r'^P\d+[\s_-]*',
        ]
        
        nombre_sin_prefijo = nombre_raw
        for patron in patrones_prefijo:
            nombre_sin_prefijo = re.sub(patron, '', nombre_sin_prefijo, flags=re.IGNORECASE)
        
        # Remover sufijos relacionados con calificaciones
        patrones_sufijo = [
            r'[\s_-]*calificaciones[\s_-]*$',
            r'[\s_-]*calificacion[\s_-]*$',
        ]
        
        nombre_sin_sufijo = nombre_sin_prefijo
        for patron in patrones_sufijo:
            nombre_sin_sufijo = re.sub(patron, '', nombre_sin_sufijo, flags=re.IGNORECASE)
        
        # Limpieza final y normalización
        nombre_limpio = nombre_sin_sufijo.strip()
        nombre_limpio = re.sub(r'\s+', ' ', nombre_limpio)  # Normalizar espacios múltiples
        nombre_limpio = re.sub(r'amp_', '&', nombre_limpio)  # Corregir HTML entities
        nombre_limpio = re.sub(r'amp;', '&', nombre_limpio)  # Corregir HTML entities
        nombre_limpio = nombre_limpio.strip()
        
        return nombre_limpio if nombre_limpio else None
        
    except Exception as e:
        logger.error(f"Error limpiando nombre para relaciones '{nombre_archivo}': {e}")
        return None

def parse_fecha_a_iso(fecha_str: str) -> Optional[str]:
    """
    Convierte una fecha en formato español a formato ISO 8601.
    
    Soporta formatos como "15 de enero de 2024" o "15 de enero de 2024 14:30"
    y también formatos ISO directos.
    
    Args:
        fecha_str: String con la fecha en formato español o ISO
        
    Returns:
        Optional[str]: String en formato ISO 8601 o None si no se puede parsear
    """
    if not fecha_str or str(fecha_str).strip() in ("", "-", "–"):
        return None
    
    try:
        s = str(fecha_str).strip().lower().replace(",", "")
        m = re.search(
            r"(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(\d{4})(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?",
            s
        )
        if m:
            day, month_name, year, time_part = m.groups()
            # Normalizar acentos en nombres de meses
            month_name = month_name.replace("á", "a").replace("é", "e").replace(
                "í", "i").replace("ó", "o").replace("ú", "u"
            )
            month = SPANISH_MONTHS.get(month_name)
            if not month:
                return None
            day_i = int(day)
            year_i = int(year)
            if time_part:
                if len(time_part.split(":")) == 2:
                    time_part += ":00"
            else:
                time_part = "00:00:00"
            try:
                iso = f"{year_i:04d}-{month:02d}-{day_i:02d}T{time_part}"
                datetime.fromisoformat(iso)
                return iso
            except Exception:
                return None
        else:
            # Intentar parsear como formato ISO directo
            try:
                dt = datetime.fromisoformat(s)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                return None
    except Exception as e:
        logger.debug(f"Error parseando fecha '{fecha_str}': {e}")
        return None

def parse_duracion_a_segundos(duracion_str: str) -> Optional[int]:
    """
    Convierte una duración en texto español a segundos.
    
    Extrae horas, minutos y segundos de strings como "2 horas 30 minutos"
    o "1 hora 15 minutos 30 segundos" y calcula el total en segundos.
    
    Args:
        duracion_str: String con la duración en formato descriptivo
        
    Returns:
        Optional[int]: Duración total en segundos o None si no se puede parsear
    """
    if not duracion_str or str(duracion_str).strip() in ("", "-", "–"):
        return None
    
    try:
        s = str(duracion_str).lower()
        horas = minutos = segundos = 0
        m = re.search(r"(\d+)\s*hora", s)
        if m: horas = int(m.group(1))
        m = re.search(r"(\d+)\s*min", s)
        if m: minutos = int(m.group(1))
        m = re.search(r"(\d+)\s*seg", s)
        if m: segundos = int(m.group(1))
        total = horas * 3600 + minutos * 60 + segundos
        return total if total > 0 else None
    except Exception as e:
        logger.debug(f"Error parseando duración '{duracion_str}': {e}")
        return None

def parse_calificacion_a_float(cal_str: str) -> Optional[float]:
    """
    Convierte una calificación en texto a valor float.
    
    Maneja formatos con comas decimales, puntos como separadores de miles,
    y limpia comillas u otros caracteres no numéricos.
    
    Args:
        cal_str: String con la calificación numérica
        
    Returns:
        Optional[float]: Calificación como float o None si no se puede parsear
    """
    if not cal_str or str(cal_str).strip() in ("", "-", "–"):
        return None
    
    try:
        s = str(cal_str).strip().replace('"', "").replace("'", "")
        # Asumir que las comas son decimales y los puntos son separadores de miles
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception as e:
        logger.debug(f"Error parseando calificación '{cal_str}': {e}")
        return None

# ----------------------------
# Función genérica para relaciones
# ----------------------------

def verificar_estado_relaciones(driver: Driver) -> None:
    """
    Verifica el estado actual de relaciones en la BD.
    
    Proporciona un reporte detallado de las relaciones existentes,
    incluyendo conteos por tipo y distribución de relaciones alumno-actividades.
    """
    with driver.session() as session:
        # Contar relaciones totales
        result = session.run("""
            MATCH ()-[r]->() 
            RETURN 
                count(r) as total_relaciones,
                count(DISTINCT type(r)) as tipos_relaciones
        """)
        record = result.single()
        if record:
            logger.info(f"ESTADO RELACIONES:")
            logger.info(f"   Total relaciones: {record['total_relaciones']}")
            logger.info(f"   Tipos de relaciones: {record['tipos_relaciones']}")
            
            # Contar relaciones por tipo
            result_tipos = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as tipo, count(r) as cantidad
                ORDER BY cantidad DESC
            """)
            logger.info(f"   Detalle por tipo:")
            for record_tipo in result_tipos:
                logger.info(f"     {record_tipo['tipo']}: {record_tipo['cantidad']}")
            
            # Relaciones alumno-actividades específicamente
            result_alumno_act = session.run("""
                MATCH (a:Alumno)-[r]->(act)
                WHERE act:Cuestionario OR act:Ayudantia
                RETURN count(r) as relaciones_alumno_actividades
            """)
            record_act = result_alumno_act.single()
            if record_act:
                logger.info(f"   Relaciones alumno-actividades: {record_act['relaciones_alumno_actividades']}")

def crear_relacion(
    tx: ManagedTransaction,
    alumno_correo: str,
    nodo_nombre: str,
    tipo_relacion: TipoRelacion,
    nodo_label: str,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    duration_seconds: Optional[int] = None,
    score: Optional[float] = None,
    estado_raw: Optional[str] = None,
) -> None:
    """
    Crea una relación entre un alumno y un nodo (Cuestionario/Ayudantia) en Neo4J.
    """
    if tipo_relacion not in VALID_RELACIONES:
        raise ValueError(f"Tipo de relación inválido: {tipo_relacion}")
    if nodo_label not in VALID_NODOS:
        raise ValueError(f"Etiqueta de nodo inválida: {nodo_label}")

    try:
        # Usar queries predefinidas para cada combinación
        if nodo_label == "Cuestionario":
            if tipo_relacion == "Intento":
                query_template = """
                MATCH (al:Alumno {{correo: $correo}})
                MATCH (n:Cuestionario {{nombre: $nombre}})
                MERGE (al)-[r:Intento]->(n)
                SET r.estado = $estado
                {additional_sets}
                RETURN r
                """
            elif tipo_relacion == "Completado":
                query_template = """
                MATCH (al:Alumno {{correo: $correo}})
                MATCH (n:Cuestionario {{nombre: $nombre}})
                MERGE (al)-[r:Completado]->(n)
                SET r.estado = $estado
                {additional_sets}
                RETURN r
                """
            else:  # Perfecto
                query_template = """
                MATCH (al:Alumno {{correo: $correo}})
                MATCH (n:Cuestionario {{nombre: $nombre}})
                MERGE (al)-[r:Perfecto]->(n)
                SET r.estado = $estado
                {additional_sets}
                RETURN r
                """
        else:  # Ayudantia
            if tipo_relacion == "Intento":
                query_template = """
                MATCH (al:Alumno {{correo: $correo}})
                MATCH (n:Ayudantia {{nombre: $nombre}})
                MERGE (al)-[r:Intento]->(n)
                SET r.estado = $estado
                {additional_sets}
                RETURN r
                """
            elif tipo_relacion == "Completado":
                query_template = """
                MATCH (al:Alumno {{correo: $correo}})
                MATCH (n:Ayudantia {{nombre: $nombre}})
                MERGE (al)-[r:Completado]->(n)
                SET r.estado = $estado
                {additional_sets}
                RETURN r
                """
            else:  # Perfecto
                query_template = """
                MATCH (al:Alumno {{correo: $correo}})
                MATCH (n:Ayudantia {{nombre: $nombre}})
                MERGE (al)-[r:Perfecto]->(n)
                SET r.estado = $estado
                {additional_sets}
                RETURN r
                """
        
        # Construir las partes SET adicionales con tipado explícito
        additional_sets: List[str] = []
        params: Dict[str, object] = {  # Cambiar a object para aceptar cualquier tipo
            "correo": alumno_correo,
            "nombre": nodo_nombre,
            "estado": estado_raw or ""
        }

        if start_iso is not None:
            additional_sets.append("r.start = datetime($start_iso)")
            params["start_iso"] = start_iso

        if end_iso is not None:
            additional_sets.append("r.end = datetime($end_iso)")
            params["end_iso"] = end_iso

        if duration_seconds is not None:
            additional_sets.append("r.duration_seconds = $duration_seconds")
            params["duration_seconds"] = duration_seconds  # Esto ya funciona con object

        if score is not None:
            additional_sets.append("r.score = $score")
            params["score"] = score  # Esto ya funciona con object

        # Combinar las propiedades adicionales
        if additional_sets:
            additional_sets_clause = "SET " + ", ".join(additional_sets)
        else:
            additional_sets_clause = ""

        # Formatear la query final
        final_query = query_template.format(additional_sets=additional_sets_clause)

        result = tx.run(final_query, parameters=params) # type: ignore
        record = result.single()
        if record:
            logger.info(f"Relación {tipo_relacion} insertada: {alumno_correo} -> {nodo_nombre}")
        else:
            logger.warning(f"No se pudo verificar la inserción de relación: {alumno_correo} -> {nodo_nombre}")
            
    except Exception as e:
        logger.error(f"Error creando relación {tipo_relacion} para {alumno_correo} -> {nodo_nombre}: {e}")
        raise
# ----------------------------
# Funciones específicas por tipo de actividad
# ----------------------------

def relacionar_alumno_cuestionario(
    tx: ManagedTransaction, 
    alumno_correo: str, 
    nombre: str, 
    tipo_relacion: TipoRelacion,
    start_iso: Optional[str] = None, 
    end_iso: Optional[str] = None,
    duration_seconds: Optional[int] = None, 
    score: Optional[float] = None,
    estado_raw: Optional[str] = None
) -> None:
    """
    Crea relación específica entre alumno y cuestionario.
    
    Wrapper especializado de crear_relacion para cuestionarios.
    """
    crear_relacion(tx, alumno_correo, nombre, tipo_relacion, nodo_label="Cuestionario",
                   start_iso=start_iso, end_iso=end_iso, duration_seconds=duration_seconds,
                   score=score, estado_raw=estado_raw)

def relacionar_alumno_ayudantia(
    tx: ManagedTransaction, 
    alumno_correo: str, 
    nombre: str, 
    tipo_relacion: TipoRelacion,
    start_iso: Optional[str] = None, 
    end_iso: Optional[str] = None,
    duration_seconds: Optional[int] = None, 
    score: Optional[float] = None,
    estado_raw: Optional[str] = None
) -> None:
    """
    Crea relación específica entre alumno y ayudantía.
    
    Wrapper especializado de crear_relacion para ayudantías.
    """
    crear_relacion(tx, alumno_correo, nombre, tipo_relacion, nodo_label="Ayudantia",
                   start_iso=start_iso, end_iso=end_iso, duration_seconds=duration_seconds,
                   score=score, estado_raw=estado_raw)

# ----------------------------
# Listar alumnos existentes
# ----------------------------

def obtener_lista_alumnos(driver: Driver) -> list[str]:
    """
    Obtiene la lista de correos de todos los alumnos en la base de datos.
    
    Consulta Neo4J para obtener todos los correos electrónicos de alumnos
    registrados, normalizados a minúsculas para consistencia en las búsquedas.
    
    Returns:
        list[str]: Lista de correos electrónicos de alumnos existentes
    """
    try:
        with driver.session() as session:
            result = session.run("MATCH (al:Alumno) RETURN al.correo AS correo")
            alumnos = [row["correo"].strip().lower() for row in result if row["correo"]]
            logger.info(f"Encontrados {len(alumnos)} alumnos en la base de datos")
            return alumnos
    except Exception as e:
        logger.error(f"Error obteniendo lista de alumnos: {e}")
        return []

# ----------------------------
# Procesar CSV individual
# ----------------------------

def extraer_numero_actividad(nombre: str) -> Optional[str]:
    """
    Extrae el número de actividad de un nombre para matching más preciso.
    
    Identifica patrones como "# 1", "#1", "N° 1", etc. para diferenciar
    actividades con nombres similares pero números diferentes.
    
    Args:
        nombre: Nombre de la actividad
        
    Returns:
        Número de actividad como string o None si no se encuentra
    """
    patrones = [
        r'#\s*(\d+)',           # # 1, #1
        r'n[°º]\s*(\d+)',       # N° 1, Nº 1
        r'cuestionario\s*(\d+)', # cuestionario 1
        r'(\d+)(?=\s*[-–]|\s*$| M\d)'  # Número al final, antes de guión o M3
    ]
    
    for patron in patrones:
        match = re.search(patron, nombre.lower())
        if match:
            return match.group(1)
    return None

def normalizar_nombre_actividad(nombre: str) -> str:
    """
    Normaliza nombres de actividades para matching consistente.
    
    Reemplaza variaciones de caracteres y remueve palabras comunes
    que no afectan la identificación de la actividad.
    
    Args:
        nombre: Nombre original de la actividad
        
    Returns:
        Nombre normalizado
    """
    # Reemplazar variaciones de caracteres
    nombre = nombre.replace('&', ' y ')
    nombre = nombre.replace('amp;', ' y ')
    nombre = nombre.replace('amp_', ' y ')
    nombre = re.sub(r'\s+', ' ', nombre)  # Normalizar espacios
    nombre = nombre.strip().lower()
    
    # Remover palabras comunes que no afectan la identificación
    palabras_remover = ['cuestionario', 'ayudantia', 'ayudantía', 'semana', 'clase', 'parte']
    palabras = nombre.split()
    palabras_filtradas = [p for p in palabras if p.lower() not in palabras_remover]
    
    return ' '.join(palabras_filtradas)

def encontrar_correspondencia_actividad(nombre_archivo: str, actividades_bd: Dict[str, List[str]]) -> Optional[Tuple[str, str]]:
    """
    Encuentra correspondencia entre archivos CSV y actividades en la base de datos.
    
    Utiliza un algoritmo de matching mejorado que prioriza la coincidencia exacta
    de números de actividad y luego evalúa la similitud semántica de los nombres.
    
    Args:
        nombre_archivo: Nombre del archivo CSV
        actividades_bd: Diccionario con actividades de la BD
        
    Returns:
        Tuple (tipo_actividad, nombre_actividad) o None si no encuentra
    """
    try:
        nombre_limpio = limpiar_nombre_archivo_relaciones(nombre_archivo)
        if not nombre_limpio:
            logger.warning(f"No se pudo limpiar el nombre: {nombre_archivo}")
            return None
            
        # Extraer número de actividad para matching más preciso
        numero_actividad = extraer_numero_actividad(nombre_limpio)
        nombre_normalizado = normalizar_nombre_actividad(nombre_limpio)
        
        def calcular_similitud(nombre_bd: str, nombre_limpio: str, numero_actividad: Optional[str]) -> float:
            """Calcula puntuación de similitud entre nombres de actividades"""
            bd_normalizado = normalizar_nombre_actividad(nombre_bd)
            limpio_normalizado = nombre_normalizado
            
            # Verificar número de actividad (criterio más importante)
            if numero_actividad:
                numero_bd = extraer_numero_actividad(nombre_bd)
                if numero_bd != numero_actividad:
                    return 0.0  # Números diferentes = no match
            
            puntuacion = 0.0
            
            # Coincidencia exacta después de normalización
            if bd_normalizado == limpio_normalizado:
                return 1.0
            
            # Coincidencia de palabras clave (sin el número)
            palabras_bd = set(bd_normalizado.split())
            palabras_limpio = set(limpio_normalizado.split())
            
            # Remover números para comparación de contenido semántico
            palabras_bd_sin_nums = {p for p in palabras_bd if not p.isdigit()}
            palabras_limpio_sin_nums = {p for p in palabras_limpio if not p.isdigit()}
            
            palabras_comunes = palabras_bd_sin_nums.intersection(palabras_limpio_sin_nums)
            
            if palabras_comunes:
                # Puntuación basada en overlap de palabras significativas
                max_palabras = max(len(palabras_bd_sin_nums), len(palabras_limpio_sin_nums))
                if max_palabras > 0:
                    puntuacion += 0.7 * (len(palabras_comunes) / max_palabras)
            
            # Bonus por coincidencia de substrings largos
            if len(nombre_limpio) > 10 and nombre_limpio.lower() in nombre_bd.lower():
                puntuacion += 0.3
            elif len(nombre_bd) > 10 and nombre_bd.lower() in nombre_limpio.lower():
                puntuacion += 0.3
            
            return puntuacion
        
        # Buscar coincidencias en todas las actividades
        mejores_coincidencias: List[Tuple[float, str, str]] = []
        
        # Buscar en cuestionarios
        for cuestionario in actividades_bd["cuestionarios"]:
            similitud = calcular_similitud(cuestionario, nombre_limpio, numero_actividad)
            if similitud > 0:
                mejores_coincidencias.append((similitud, "Cuestionario", cuestionario))
        
        # Buscar en ayudantías
        for ayudantia in actividades_bd["ayudantias"]:
            similitud = calcular_similitud(ayudantia, nombre_limpio, numero_actividad)
            if similitud > 0:
                mejores_coincidencias.append((similitud, "Ayudantia", ayudantia))
        
        # Seleccionar la mejor coincidencia
        if mejores_coincidencias:
            # Ordenar por similitud descendente
            mejores_coincidencias.sort(key=lambda x: x[0], reverse=True)
            mejor_similitud, mejor_tipo, mejor_nombre = mejores_coincidencias[0]
            
            # Umbral más bajo si tenemos número coincidente
            umbral_minimo = 0.3 if numero_actividad else 0.6
            
            if mejor_similitud >= umbral_minimo:
                logger.info(f"Coincidencia encontrada: {nombre_archivo} -> {mejor_nombre} ({mejor_tipo})")
                return (mejor_tipo, mejor_nombre)
        
        # Estrategia de fallback: buscar por número solamente
        if numero_actividad:
            actividades_mismo_numero: List[Tuple[str, str]] = []
            
            # Buscar actividades con el mismo número
            for cuestionario in actividades_bd["cuestionarios"]:
                num_bd = extraer_numero_actividad(cuestionario)
                if num_bd == numero_actividad:
                    actividades_mismo_numero.append(("Cuestionario", cuestionario))
            
            for ayudantia in actividades_bd["ayudantias"]:
                num_bd = extraer_numero_actividad(ayudantia)
                if num_bd == numero_actividad:
                    actividades_mismo_numero.append(("Ayudantia", ayudantia))
            
            if len(actividades_mismo_numero) == 1:
                # Solo una actividad con ese número - probable match
                mejor_tipo, mejor_nombre = actividades_mismo_numero[0]
                logger.info(f"Fallback por número: {nombre_archivo} -> {mejor_nombre} ({mejor_tipo})")
                return (mejor_tipo, mejor_nombre)
        
        logger.warning(f"No se encontró correspondencia para: {nombre_archivo} -> {nombre_limpio}")
        return None
        
    except Exception as e:
        logger.error(f"Error buscando correspondencia para {nombre_archivo}: {e}")
        return None

def procesar_csv(driver: Driver, recurso_path: Path, actividades_bd: Dict[str, List[str]]) -> None:
    """
    Procesa un archivo CSV individual usando el mapeo con actividades de BD.
    """
    logger.info(f"Iniciando procesamiento de {recurso_path.name}")
    
    if not recurso_path.exists():
        logger.error(f"Archivo no existe: {recurso_path}")
        return
    
    try:
        # Leer el CSV con type ignore para el warning específico
        df = pd.read_csv(recurso_path)  # type: ignore
        logger.info(f"CSV leído - {len(df)} filas, columnas: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Error leyendo CSV: {e}")
        return

    if df.empty:
        logger.warning("CSV vacío")
        return

    # Buscar columnas relevantes
    col_correo = next((c for c in df.columns if "correo" in c.lower()), None)
    
    if col_correo is None:
        logger.error(f"No hay columna de correo. Columnas disponibles: {list(df.columns)}")
        return

    # Encontrar correspondencia con actividad en BD
    correspondencia = encontrar_correspondencia_actividad(recurso_path.name, actividades_bd)
    
    if not correspondencia:
        logger.warning(f"No hay correspondencia para {recurso_path.name}")
        return
        
    tipo_recurso, nombre_actividad = correspondencia
    logger.info(f"Mapeo confirmado: {tipo_recurso} -> {nombre_actividad}")

    # Buscar otras columnas relevantes
    col_estado = next((c for c in df.columns if "estado" in c.lower()), None)
    col_comenzado = next((c for c in df.columns if "comenz" in c.lower()), None)
    col_finalizado = next((c for c in df.columns if "finaliz" in c.lower()), None)
    col_duracion = next((c for c in df.columns if "dur" in c.lower()), None)
    col_calificacion = next((c for c in df.columns if "calific" in c.lower()), None)

    # Preparar datos de alumnos
    df[col_correo] = df[col_correo].astype(str)
    csv_correos = {str(row[col_correo]).strip().lower() for _, row in df.iterrows() if str(row[col_correo]).strip()}

    alumnos_bd = obtener_lista_alumnos(driver)

    # Encontrar intersección de alumnos existentes
    alumnos_comunes = csv_correos.intersection(set(alumnos_bd))
    
    if not alumnos_comunes:
        logger.warning("No hay coincidencias entre correos del CSV y BD")
        return

    alumnos_procesados = 0
    errores = 0

    # Procesar cada alumno encontrado
    for correo in alumnos_comunes:
        try:
            alumno_data = df[df[col_correo].str.strip().str.lower() == correo]
            if alumno_data.empty:
                continue

            serie = alumno_data.iloc[0]

            # Parsear campos del progreso del alumno
            estado_val = serie[col_estado] if col_estado else None
            estado = str(estado_val).strip() if estado_val is not None and str(estado_val) != 'nan' and str(estado_val) != 'NaN' else ""
            
            comenzado_val = serie[col_comenzado] if col_comenzado else None
            comenzado_str = str(comenzado_val).strip() if comenzado_val is not None and str(comenzado_val) != 'nan' and str(comenzado_val) != 'NaN' else ""
            start_iso = parse_fecha_a_iso(comenzado_str)
            
            finalizado_val = serie[col_finalizado] if col_finalizado else None
            finalizado_str = str(finalizado_val).strip() if finalizado_val is not None and str(finalizado_val) != 'nan' and str(finalizado_val) != 'NaN' else ""
            end_iso = parse_fecha_a_iso(finalizado_str)
            
            duracion_val = serie[col_duracion] if col_duracion else None
            duracion_str = str(duracion_val).strip() if duracion_val is not None and str(duracion_val) != 'nan' and str(duracion_val) != 'NaN' else ""
            duration_seconds = parse_duracion_a_segundos(duracion_str)
            
            calificacion_val = serie[col_calificacion] if col_calificacion else None
            calificacion_str = str(calificacion_val).strip() if calificacion_val is not None and str(calificacion_val) != 'nan' and str(calificacion_val) != 'NaN' else ""
            score = parse_calificacion_a_float(calificacion_str)

            # Determinar tipo de relación basado en estado y calificación
            tipo_relacion: TipoRelacion = "Intento"
            if estado.lower() == "finalizado":
                tipo_relacion = "Completado"
                if score is not None and abs(score - 100.0) < 1e-6:
                    tipo_relacion = "Perfecto"

            # Seleccionar función según tipo de recurso
            funcion_relacion = relacionar_alumno_cuestionario if tipo_recurso == "Cuestionario" else relacionar_alumno_ayudantia
            
            # Crear relación en la base de datos
            with driver.session() as session:
                session.execute_write(funcion_relacion, correo, nombre_actividad, tipo_relacion,
                                      start_iso, end_iso, duration_seconds, score, estado)
            
            alumnos_procesados += 1

        except Exception as e:
            logger.error(f"Error procesando alumno {correo}: {e}")
            errores += 1

    logger.info(f"Procesamiento completado: {alumnos_procesados} alumnos exitosos, {errores} errores")
    logger.info(f"Resumen {tipo_recurso} {nombre_actividad}: {alumnos_procesados} alumnos, {errores} errores")
    
# ----------------------------
# Procesar todas las unidades (función principal)
# ----------------------------

def relacionar_alumnos(driver: Driver, base_path: Path) -> None:
    """
    Función principal para relacionar alumnos con actividades.
    
    Recorre todas las unidades del curso, procesa archivos CSV de cuestionarios
    y ayudantías, y establece relaciones con los alumnos correspondientes.
    
    Args:
        driver: Driver de conexión a Neo4J
        base_path: Ruta base donde se encuentran las carpetas de unidades
    """
    logger.info("Iniciando relacionamiento de alumnos con actividades...")
    
    # Verificar estado inicial de relaciones
    logger.info("ESTADO ANTES de relacionar alumnos:")
    verificar_estado_relaciones(driver)
    
    # Obtener actividades existentes en BD
    actividades_bd = obtener_actividades_bd(driver)
    
    if not actividades_bd["cuestionarios"] and not actividades_bd["ayudantias"]:
        logger.error("No hay actividades en la BD para relacionar")
        return

    unidades_procesadas = 0
    archivos_procesados = 0

    # Procesar cada unidad del curso
    for unidad_dir in base_path.iterdir():
        if not unidad_dir.is_dir() or unidad_dir.name.lower() == "alumnos":
            continue

        logger.info(f"Procesando unidad: {unidad_dir.name}")
        unidades_procesadas += 1

        # Procesar cuestionarios de la unidad
        cuestionarios_path = unidad_dir / "Cuestionarios"
        if cuestionarios_path.exists():
            for archivo in cuestionarios_path.glob("*.csv"):
                try:
                    procesar_csv(driver, archivo, actividades_bd)
                    archivos_procesados += 1
                except Exception as e:
                    logger.error(f"Error procesando {archivo}: {e}")

        # Procesar ayudantías de la unidad
        ayudantias_path = unidad_dir / "Ayudantías"
        if ayudantias_path.exists():
            for archivo in ayudantias_path.glob("*.csv"):
                try:
                    procesar_csv(driver, archivo, actividades_bd)
                    archivos_procesados += 1
                except Exception as e:
                    logger.error(f"Error procesando {archivo}: {e}")

    # Verificar estado final de relaciones
    logger.info("ESTADO DESPUÉS de relacionar alumnos:")
    verificar_estado_relaciones(driver)
    
    logger.info(f"Relacionamiento completado: {unidades_procesadas} unidades, {archivos_procesados} archivos procesados")