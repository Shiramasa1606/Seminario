"""
Módulo de Relacionamiento de Alumnos - Gestión de Progreso y Actividades en Neo4J

Este módulo se encarga de establecer las relaciones entre alumnos y actividades educativas
(cuestionarios y ayudantías) basándose en datos de progreso provenientes de archivos CSV.
Incluye el procesamiento de estados, calificaciones, duraciones y fechas para crear
un historial completo del progreso estudiantil.

Funciones principales:
    - procesar_unidades: Proceso principal de relacionamiento masivo
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
from typing import Literal, Optional, Any
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

def parse_fecha_a_iso(fecha_str: str) -> Optional[str]:
    """
    Convierte una fecha en formato español a formato ISO 8601.
    
    Soporta formatos como "15 de enero de 2024" o "15 de enero de 2024 14:30"
    y también formatos ISO directos. Maneja normalización de acentos y variantes.
    
    Args:
        fecha_str: String con la fecha en formato español o ISO
        
    Returns:
        Optional[str]: String en formato ISO 8601 o None si no se puede parsear
        
    Example:
        >>> parse_fecha_a_iso("15 de enero de 2024")
        '2024-01-15T00:00:00'
        >>> parse_fecha_a_iso("20 de marzo de 2024 14:30")
        '2024-03-20T14:30:00'
        >>> parse_fecha_a_iso("2024-01-15T10:30:00")
        '2024-01-15T10:30:00'
        
    Note:
        - Maneja meses con y sin acentos
        - Agrega segundos si faltan en el tiempo
        - Retorna None para valores vacíos o no parseables
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
        
    Example:
        >>> parse_duracion_a_segundos("2 horas 30 minutos")
        9000
        >>> parse_duracion_a_segundos("1 hora 15 minutos 30 segundos")
        4530
        >>> parse_duracion_a_segundos("45 minutos")
        2700
        
    Note:
        - Case-insensitive
        - Ignora elementos no numéricos
        - Retorna None para valores vacíos o sin componentes temporales
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
        
    Example:
        >>> parse_calificacion_a_float("95,5")
        95.5
        >>> parse_calificacion_a_float("100")
        100.0
        >>> parse_calificacion_a_float("'85.5'")
        85.5
        
    Note:
        - Asume que las comas son decimales y los puntos son separadores de miles
        - Remueve comillas y espacios
        - Retorna None para valores vacíos o no numéricos
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
    
    Función genérica que maneja la creación de relaciones con todos los atributos
    posibles: fechas, duración, calificación y estado. Usa MERGE para evitar
    duplicados y actualiza propiedades existentes.
    
    Args:
        tx: Transacción activa de Neo4J
        alumno_correo: Correo electrónico único del alumno
        nodo_nombre: Nombre del nodo destino (Cuestionario o Ayudantia)
        tipo_relacion: Tipo de relación a crear (Intento/Completado/Perfecto)
        nodo_label: Etiqueta del nodo destino ("Cuestionario" o "Ayudantia")
        start_iso: Fecha de inicio en formato ISO 8601
        end_iso: Fecha de finalización en formato ISO 8601
        duration_seconds: Duración total en segundos
        score: Calificación numérica (0-100)
        estado_raw: Estado original del sistema fuente
        
    Raises:
        ValueError: Si el tipo de relación o etiqueta de nodo son inválidos
        Exception: Si hay error en la operación de base de datos
        
    Example:
        >>> crear_relacion(tx, "alumno@email.com", "Evaluacion_1", "Completado", 
        ...                "Cuestionario", "2024-01-15T10:00:00", "2024-01-15T11:30:00", 
        ...                5400, 85.5, "Finalizado")
        ✅ Relación Completado insertada: alumno@email.com -> Evaluacion_1
        
    Note:
        - Usa MERGE para operación idempotente
        - Actualiza solo las propiedades no nulas
        - Valida tipos de relación y etiquetas
        - Maneja fechas como objetos datetime de Neo4J
    """
    if tipo_relacion not in VALID_RELACIONES:
        raise ValueError(f"Tipo de relación inválido: {tipo_relacion}")
    if nodo_label not in VALID_NODOS:
        raise ValueError(f"Etiqueta de nodo inválida: {nodo_label}")

    try:
        # Construir query de manera segura con FOREACH para propiedades opcionales
        query: str = (
            f"MATCH (al:Alumno {{correo:$correo}}) "
            f"MATCH (n:{nodo_label} {{nombre:$nombre}}) "
            f"MERGE (al)-[r:{tipo_relacion}]->(n) "
            "SET r.estado = $estado "
            "FOREACH (_ IN CASE WHEN $start_iso IS NULL THEN [] ELSE [1] END | SET r.start = datetime($start_iso)) "
            "FOREACH (_ IN CASE WHEN $end_iso IS NULL THEN [] ELSE [1] END | SET r.end = datetime($end_iso)) "
            "FOREACH (_ IN CASE WHEN $duration_seconds IS NULL THEN [] ELSE [1] END | SET r.duration_seconds = $duration_seconds) "
            "FOREACH (_ IN CASE WHEN $score IS NULL THEN [] ELSE [1] END | SET r.score = $score) "
            "RETURN r"
        )

        params: dict[str, Any] = {
            "correo": alumno_correo,
            "nombre": nodo_nombre,
            "start_iso": start_iso,
            "end_iso": end_iso,
            "duration_seconds": duration_seconds,
            "score": score,
            "estado": estado_raw or ""
        }

        result = tx.run(query, parameters=params)  # type: ignore
        record = result.single()
        if record:
            logger.info(f"✅ Relación {tipo_relacion} insertada: {alumno_correo} -> {nodo_nombre}")
        else:
            logger.warning(f"⚠️ No se pudo verificar la inserción de relación: {alumno_correo} -> {nodo_nombre}")
            
    except Exception as e:
        logger.error(f"❌ Error creando relación {tipo_relacion} para {alumno_correo} -> {nodo_nombre}: {e}")
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
    
    Args:
        tx: Transacción activa de Neo4J
        alumno_correo: Correo electrónico del alumno
        nombre: Nombre del cuestionario
        tipo_relacion: Tipo de relación a crear
        start_iso: Fecha de inicio en ISO
        end_iso: Fecha de fin en ISO
        duration_seconds: Duración en segundos
        score: Calificación numérica
        estado_raw: Estado original
        
    Example:
        >>> relacionar_alumno_cuestionario(tx, "alumno@email.com", "Quiz_1", 
        ...                               "Completado", "2024-01-15T10:00:00", 
        ...                               "2024-01-15T11:00:00", 3600, 90.0, "Finalizado")
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
    
    Args:
        tx: Transacción activa de Neo4J
        alumno_correo: Correo electrónico del alumno
        nombre: Nombre de la ayudantía
        tipo_relacion: Tipo de relación a crear
        start_iso: Fecha de inicio en ISO
        end_iso: Fecha de fin en ISO
        duration_seconds: Duración en segundos
        score: Calificación numérica
        estado_raw: Estado original
        
    Example:
        >>> relacionar_alumno_ayudantia(tx, "alumno@email.com", "Sesion_1", 
        ...                            "Intento", "2024-01-15T14:00:00", 
        ...                            "2024-01-15T15:30:00", 5400, None, "En progreso")
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
    
    Args:
        driver: Driver de conexión a Neo4J
        
    Returns:
        list[str]: Lista de correos electrónicos de alumnos existentes
        
    Example:
        >>> alumnos = obtener_lista_alumnos(driver)
        >>> print(f"Total alumnos: {len(alumnos)}")
        Total alumnos: 150
        >>> print(alumnos[:3])
        ['alumno1@email.com', 'alumno2@email.com', 'alumno3@email.com']
        
    Note:
        - Retorna lista vacía en caso de error
        - Normaliza correos a minúsculas
        - Filtra valores nulos o vacíos
    """
    try:
        with driver.session() as session:
            result = session.run("MATCH (al:Alumno) RETURN al.correo AS correo")
            alumnos = [row["correo"].strip().lower() for row in result if row["correo"]]
            logger.info(f"📋 Encontrados {len(alumnos)} alumnos en la base de datos")
            return alumnos
    except Exception as e:
        logger.error(f"❌ Error obteniendo lista de alumnos: {e}")
        return []

# ----------------------------
# Procesar CSV individual
# ----------------------------

def procesar_csv(driver: Driver, recurso_path: Path, tipo_recurso: Literal["Cuestionario", "Ayudantia"]) -> None:
    """
    Procesa un archivo CSV individual y crea las relaciones alumno-actividad.
    
    Lee un archivo CSV de calificaciones/progreso, parsea los datos, y crea
    las relaciones correspondientes en Neo4J para todos los alumnos encontrados
    tanto en la base de datos como en el CSV.
    
    Args:
        driver: Driver de conexión a Neo4J
        recurso_path: Path al archivo CSV a procesar
        tipo_recurso: Tipo de recurso ("Cuestionario" o "Ayudantia")
        
    Raises:
        FileNotFoundError: Si el archivo CSV no existe
        ValueError: Si el CSV no tiene la estructura esperada (falta columna de correo)
        
    Example:
        >>> procesar_csv(driver, Path("/ruta/cuestionario_1-calificaciones.csv"), "Cuestionario")
        📄 Procesando Cuestionario: cuestionario_1-calificaciones.csv
        ✅ Cuestionario cuestionario_1: 45 alumnos procesados, 2 errores
        
    Note:
        - Determina automáticamente el tipo de relación basado en estado y calificación
        - Solo procesa alumnos que existan en la base de datos
        - Continúa el procesamiento despite errores individuales por alumno
        - Limpia automáticamente el nombre del recurso removiendo sufijos
    """
    if not recurso_path.exists():
        raise FileNotFoundError(f"Archivo CSV no encontrado: {recurso_path}")
    
    logger.info(f"📄 Procesando {tipo_recurso}: {recurso_path.name}")
    
    try:
        df = pd.read_csv(recurso_path)  # type: ignore
    except Exception as e:
        logger.error(f"❌ Error leyendo CSV {recurso_path}: {e}")
        return

    if df.empty:
        logger.warning(f"⚠️ CSV vacío: {recurso_path}")
        return

    # Buscar columnas relevantes por patrones en los nombres
    col_correo = next((c for c in df.columns if "correo" in c.lower()), None)
    if col_correo is None:
        error_msg = f"No se encontró la columna de correo en {recurso_path}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    # Buscar otras columnas por patrones
    col_estado = next((c for c in df.columns if "estado" in c.lower()), None)
    col_comenzado = next((c for c in df.columns if "comenz" in c.lower()), None)
    col_finalizado = next((c for c in df.columns if "finaliz" in c.lower()), None)
    col_duracion = next((c for c in df.columns if "dur" in c.lower()), None)
    col_calificacion = next((c for c in df.columns if "calific" in c.lower()), None)

    # Limpiar y preparar datos del CSV
    df[col_correo] = df[col_correo].astype(str)
    csv_correos = {str(row[col_correo]).strip().lower() for _, row in df.iterrows() if str(row[col_correo]).strip()}
    alumnos_bd = obtener_lista_alumnos(driver)

    # Limpiar nombre del recurso (remover sufijos como "-calificaciones")
    recurso_nombre = recurso_path.stem
    if recurso_nombre.lower().endswith("-calificaciones"):
        recurso_nombre = recurso_nombre[: -len("-calificaciones")]

    alumnos_procesados = 0
    errores = 0

    for correo in alumnos_bd:
        if correo not in csv_correos:
            logger.debug(f"⚠️ Alumno no presente en CSV, se omite: {correo}")
            continue

        try:
            # Buscar datos del alumno en el CSV
            alumno_data = df[df[col_correo].str.strip().str.lower() == correo]
            if alumno_data.empty:
                logger.warning(f"⚠️ No se encontraron datos para alumno {correo} en CSV")
                continue

            serie = alumno_data.iloc[0]

            # Parsear todos los campos disponibles
            estado = str(serie[col_estado]).strip() if col_estado and pd.notna(serie[col_estado]) else ""
            start_iso = parse_fecha_a_iso(str(serie[col_comenzado]).strip() if col_comenzado and pd.notna(serie[col_comenzado]) else "")
            end_iso = parse_fecha_a_iso(str(serie[col_finalizado]).strip() if col_finalizado and pd.notna(serie[col_finalizado]) else "")
            duration_seconds = parse_duracion_a_segundos(str(serie[col_duracion]).strip() if col_duracion and pd.notna(serie[col_duracion]) else "")
            score = parse_calificacion_a_float(str(serie[col_calificacion]).strip() if col_calificacion and pd.notna(serie[col_calificacion]) else "")

            # Determinar tipo de relación basado en estado y calificación
            tipo_relacion: TipoRelacion = "Intento"
            if estado.lower() == "finalizado":
                tipo_relacion = "Completado"
                if score is not None and abs(score - 100.0) < 1e-6:
                    tipo_relacion = "Perfecto"

            # Seleccionar función específica según tipo de recurso
            funcion_relacion = relacionar_alumno_cuestionario if tipo_recurso == "Cuestionario" else relacionar_alumno_ayudantia
            
            # Ejecutar la creación de relación
            with driver.session() as session:
                session.execute_write(funcion_relacion, correo, recurso_nombre, tipo_relacion,
                                      start_iso, end_iso, duration_seconds, score, estado)
            
            alumnos_procesados += 1

        except Exception as e:
            logger.error(f"❌ Error procesando alumno {correo} en {recurso_path}: {e}")
            errores += 1
            continue

    logger.info(f"✅ {tipo_recurso} {recurso_nombre}: {alumnos_procesados} alumnos procesados, {errores} errores")

# ----------------------------
# Procesar todas las unidades (función principal)
# ----------------------------

def procesar_unidades(driver: Driver, base_path: Path) -> None:
    """
    Procesa recursivamente todas las unidades, cuestionarios y ayudantías.
    
    Función principal que orquesta el procesamiento completo de todas las
    actividades educativas. Recorre la estructura de directorios, encuentra
    archivos CSV de calificaciones y crea las relaciones correspondientes
    en la base de datos Neo4J.
    
    Args:
        driver: Driver de conexión a Neo4J
        base_path: Ruta base donde buscar las unidades y actividades
        
    Raises:
        FileNotFoundError: Si la ruta base no existe o no es un directorio
        
    Example:
        >>> procesar_unidades(driver, Path("/ruta/materiales"))
        🔍 Iniciando procesamiento de unidades en: /ruta/materiales
        📁 Procesando unidad: Unidad_01
        ✅ Cuestionario quiz_1: 45 alumnos procesados, 2 errores
        ✅ Ayudantía sesion_1: 40 alumnos procesados, 1 error
        ...
        ✅ Procesamiento completado: 5 unidades, 15 cuestionarios, 8 ayudantías
        
    Note:
        - Excluye automáticamente la carpeta 'Alumnos'
        - Procesa solo archivos CSV
        - Continúa el procesamiento despite errores en archivos individuales
        - Logging detallado de progreso y métricas finales
    """
    if not base_path.exists():
        raise FileNotFoundError(f"La ruta base no existe: {base_path}")
    
    if not base_path.is_dir():
        raise FileNotFoundError(f"La ruta base no es un directorio: {base_path}")

    logger.info(f"🔍 Iniciando procesamiento de unidades en: {base_path}")

    unidades_procesadas = 0
    cuestionarios_procesados = 0
    ayudantias_procesadas = 0

    for unidad_dir in base_path.iterdir():
        if not unidad_dir.is_dir() or unidad_dir.name.lower() == "alumnos":
            continue

        logger.info(f"📁 Procesando unidad: {unidad_dir.name}")
        unidades_procesadas += 1

        # Procesar cuestionarios de la unidad
        cuestionarios_path = unidad_dir / "Cuestionarios"
        if cuestionarios_path.exists() and cuestionarios_path.is_dir():
            for archivo in cuestionarios_path.iterdir():
                if archivo.is_file() and archivo.suffix.lower() == ".csv":
                    try:
                        procesar_csv(driver, archivo, "Cuestionario")
                        cuestionarios_procesados += 1
                    except Exception as e:
                        logger.error(f"❌ Error procesando cuestionario {archivo}: {e}")
                        continue

        # Procesar ayudantías de la unidad
        ayudantias_path = unidad_dir / "Ayudantías"
        if ayudantias_path.exists() and ayudantias_path.is_dir():
            for archivo in ayudantias_path.iterdir():
                if archivo.is_file() and archivo.suffix.lower() == ".csv":
                    try:
                        procesar_csv(driver, archivo, "Ayudantia")
                        ayudantias_procesadas += 1
                    except Exception as e:
                        logger.error(f"❌ Error procesando ayudantía {archivo}: {e}")
                        continue

    logger.info(f"✅ Procesamiento completado: {unidades_procesadas} unidades, "
                f"{cuestionarios_procesados} cuestionarios, {ayudantias_procesadas} ayudantías")