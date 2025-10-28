from pathlib import Path
from typing import Literal, Optional, Any
import re
from datetime import datetime
import pandas as pd
from neo4j import Driver, ManagedTransaction
import logging

# Setup logging
logger = logging.getLogger(__name__)

# ----------------------------
# Tipos de relaciones
# ----------------------------
TipoRelacion = Literal["Intento", "Completado", "Perfecto"]
VALID_RELACIONES: set[str] = {"Intento", "Completado", "Perfecto"}
VALID_NODOS: set[str] = {"Cuestionario", "Ayudantia"}

# ----------------------------
# Helpers: parseo de campos del CSV
# ----------------------------
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

def parse_fecha_a_iso(fecha_str: str) -> Optional[str]:
    """
    Convierte una fecha en español a formato ISO.
    
    Args:
        fecha_str: String con la fecha en formato español
        
    Returns:
        String en formato ISO o None si no se puede parsear
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
    Convierte una duración en texto a segundos.
    
    Args:
        duracion_str: String con la duración (ej: "2 horas 30 minutos")
        
    Returns:
        Duración en segundos o None si no se puede parsear
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
    Convierte una calificación en texto a float.
    
    Args:
        cal_str: String con la calificación
        
    Returns:
        Calificación como float o None si no se puede parsear
    """
    if not cal_str or str(cal_str).strip() in ("", "-", "–"):
        return None
    
    try:
        s = str(cal_str).strip().replace('"', "").replace("'", "")
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
    Crea una relación entre un alumno y un nodo (Cuestionario/Ayudantia).
    
    Args:
        tx: Transacción de Neo4j
        alumno_correo: Correo del alumno
        nodo_nombre: Nombre del nodo destino
        tipo_relacion: Tipo de relación a crear
        nodo_label: Etiqueta del nodo destino
        start_iso: Fecha de inicio en ISO
        end_iso: Fecha de fin en ISO
        duration_seconds: Duración en segundos
        score: Puntuación
        estado_raw: Estado original
        
    Raises:
        ValueError: Si el tipo de relación o etiqueta son inválidos
        Exception: Si hay error en la base de datos
    """
    if tipo_relacion not in VALID_RELACIONES:
        raise ValueError(f"Tipo de relación inválido: {tipo_relacion}")
    if nodo_label not in VALID_NODOS:
        raise ValueError(f"Etiqueta de nodo inválida: {nodo_label}")

    try:
        # Construir query de manera segura
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
# Funciones específicas
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
    Crea relación entre alumno y cuestionario.
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
    Crea relación entre alumno y ayudantía.
    """
    crear_relacion(tx, alumno_correo, nombre, tipo_relacion, nodo_label="Ayudantia",
                   start_iso=start_iso, end_iso=end_iso, duration_seconds=duration_seconds,
                   score=score, estado_raw=estado_raw)

# ----------------------------
# Listar alumnos
# ----------------------------
def obtener_lista_alumnos(driver: Driver) -> list[str]:
    """
    Obtiene la lista de correos de todos los alumnos en la base de datos.
    
    Args:
        driver: Driver de Neo4j
        
    Returns:
        Lista de correos electrónicos de alumnos
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
# Procesar CSV
# ----------------------------
def procesar_csv(driver: Driver, recurso_path: Path, tipo_recurso: Literal["Cuestionario", "Ayudantia"]) -> None:
    """
    Procesa un archivo CSV y crea las relaciones correspondientes.
    
    Args:
        driver: Driver de Neo4j
        recurso_path: Path al archivo CSV
        tipo_recurso: Tipo de recurso (Cuestionario o Ayudantia)
        
    Raises:
        ValueError: Si el CSV no tiene la estructura esperada
        FileNotFoundError: Si el archivo no existe
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

    # Buscar columnas relevantes
    col_correo = next((c for c in df.columns if "correo" in c.lower()), None)
    if col_correo is None:
        error_msg = f"No se encontró la columna de correo en {recurso_path}"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    col_estado = next((c for c in df.columns if "estado" in c.lower()), None)
    col_comenzado = next((c for c in df.columns if "comenz" in c.lower()), None)
    col_finalizado = next((c for c in df.columns if "finaliz" in c.lower()), None)
    col_duracion = next((c for c in df.columns if "dur" in c.lower()), None)
    col_calificacion = next((c for c in df.columns if "calific" in c.lower()), None)

    # Limpiar y preparar datos
    df[col_correo] = df[col_correo].astype(str)
    csv_correos = {str(row[col_correo]).strip().lower() for _, row in df.iterrows() if str(row[col_correo]).strip()}
    alumnos_bd = obtener_lista_alumnos(driver)

    # Limpiar nombre del recurso
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

            # Parsear datos
            estado = str(serie[col_estado]).strip() if col_estado and pd.notna(serie[col_estado]) else ""
            start_iso = parse_fecha_a_iso(str(serie[col_comenzado]).strip() if col_comenzado and pd.notna(serie[col_comenzado]) else "")
            end_iso = parse_fecha_a_iso(str(serie[col_finalizado]).strip() if col_finalizado and pd.notna(serie[col_finalizado]) else "")
            duration_seconds = parse_duracion_a_segundos(str(serie[col_duracion]).strip() if col_duracion and pd.notna(serie[col_duracion]) else "")
            score = parse_calificacion_a_float(str(serie[col_calificacion]).strip() if col_calificacion and pd.notna(serie[col_calificacion]) else "")

            # Determinar tipo de relación
            tipo_relacion: TipoRelacion = "Intento"
            if estado.lower() == "finalizado":
                tipo_relacion = "Completado"
                if score is not None and abs(score - 100.0) < 1e-6:
                    tipo_relacion = "Perfecto"

            # Crear relación
            funcion_relacion = relacionar_alumno_cuestionario if tipo_recurso == "Cuestionario" else relacionar_alumno_ayudantia
            
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
# Procesar todas las unidades
# ----------------------------
def procesar_unidades(driver: Driver, base_path: Path) -> None:
    """
    Procesa todas las unidades, cuestionarios y ayudantías.
    
    Args:
        driver: Driver de Neo4j
        base_path: Ruta base donde buscar las unidades
        
    Raises:
        FileNotFoundError: Si la ruta base no es válida
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

        # Procesar cuestionarios
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

        # Procesar ayudantías
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