from pathlib import Path
from typing import Literal, Optional, Any
import re
from datetime import datetime
import pandas as pd
from neo4j import Driver, ManagedTransaction, Session

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
    if not fecha_str or str(fecha_str).strip() in ("", "-", "–"):
        return None
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

def parse_duracion_a_segundos(duracion_str: str) -> Optional[int]:
    if not duracion_str or str(duracion_str).strip() in ("", "-", "–"):
        return None
    s = str(duracion_str).lower()
    horas = minutos = segundos = 0
    try:
        m = re.search(r"(\d+)\s*hora", s)
        if m: horas = int(m.group(1))
        m = re.search(r"(\d+)\s*min", s)
        if m: minutos = int(m.group(1))
        m = re.search(r"(\d+)\s*seg", s)
        if m: segundos = int(m.group(1))
        total = horas * 3600 + minutos * 60 + segundos
        return total if total > 0 else None
    except Exception:
        return None

def parse_calificacion_a_float(cal_str: str) -> Optional[float]:
    if not cal_str or str(cal_str).strip() in ("", "-", "–"):
        return None
    s = str(cal_str).strip().replace('"', "").replace("'", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
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
    if tipo_relacion not in VALID_RELACIONES:
        raise ValueError(f"Tipo de relación inválido: {tipo_relacion}")
    if nodo_label not in VALID_NODOS:
        raise ValueError(f"Etiqueta de nodo inválida: {nodo_label}")

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

    result = tx.run(query, parameters=params) # type: ignore
    if result.single():
        print(f"✅ Relación {tipo_relacion} insertada: {alumno_correo} -> {nodo_nombre}")
    else:
        print(f"⚠️ Error al insertar relación: {alumno_correo} -> {nodo_nombre}")

# ----------------------------
# Funciones específicas
# ----------------------------
def relacionar_alumno_cuestionario(tx: ManagedTransaction, alumno_correo: str, nombre: str, tipo_relacion: TipoRelacion,
                                   start_iso: Optional[str] = None, end_iso: Optional[str] = None,
                                   duration_seconds: Optional[int] = None, score: Optional[float] = None,
                                   estado_raw: Optional[str] = None) -> None:
    crear_relacion(tx, alumno_correo, nombre, tipo_relacion, nodo_label="Cuestionario",
                   start_iso=start_iso, end_iso=end_iso, duration_seconds=duration_seconds,
                   score=score, estado_raw=estado_raw)

def relacionar_alumno_ayudantia(tx: ManagedTransaction, alumno_correo: str, nombre: str, tipo_relacion: TipoRelacion,
                                start_iso: Optional[str] = None, end_iso: Optional[str] = None,
                                duration_seconds: Optional[int] = None, score: Optional[float] = None,
                                estado_raw: Optional[str] = None) -> None:
    crear_relacion(tx, alumno_correo, nombre, tipo_relacion, nodo_label="Ayudantia",
                   start_iso=start_iso, end_iso=end_iso, duration_seconds=duration_seconds,
                   score=score, estado_raw=estado_raw)

# ----------------------------
# Listar alumnos
# ----------------------------
def obtener_lista_alumnos(session: Session) -> list[str]:
    result = session.run("MATCH (al:Alumno) RETURN al.correo AS correo")
    return [row["correo"].strip().lower() for row in result]

# ----------------------------
# Procesar CSV
# ----------------------------
def procesar_csv(session: Session, recurso_path: Path, tipo_recurso: Literal["Cuestionario", "Ayudantia"]) -> None:
    df = pd.read_csv(recurso_path)  # type: ignore
    col_correo = next((c for c in df.columns if "correo" in c.lower()), None)
    if col_correo is None:
        raise ValueError(f"No se encontró la columna de correo en {recurso_path}")

    col_estado = next((c for c in df.columns if "estado" in c.lower()), None)
    col_comenzado = next((c for c in df.columns if "comenz" in c.lower()), None)
    col_finalizado = next((c for c in df.columns if "finaliz" in c.lower()), None)
    col_duracion = next((c for c in df.columns if "dur" in c.lower()), None)
    col_calificacion = next((c for c in df.columns if "calific" in c.lower()), None)

    df[col_correo] = df[col_correo].astype(str)
    csv_correos = {str(row[col_correo]).strip().lower() for _, row in df.iterrows() if str(row[col_correo]).strip()}
    alumnos_bd = obtener_lista_alumnos(session)

    recurso_nombre = recurso_path.stem
    if recurso_nombre.lower().endswith("-calificaciones"):
        recurso_nombre = recurso_nombre[: -len("-calificaciones")]

    for correo in alumnos_bd:
        if correo not in csv_correos:
            print(f"⚠️ Alumno no presente en CSV, se omite: {correo}")
            continue

        serie = df[df[col_correo].str.strip().str.lower() == correo].iloc[0]

        estado = str(serie[col_estado]).strip() if col_estado else ""
        start_iso = parse_fecha_a_iso(str(serie[col_comenzado]).strip() if col_comenzado else "")
        end_iso = parse_fecha_a_iso(str(serie[col_finalizado]).strip() if col_finalizado else "")
        duration_seconds = parse_duracion_a_segundos(str(serie[col_duracion]).strip() if col_duracion else "")
        score = parse_calificacion_a_float(str(serie[col_calificacion]).strip() if col_calificacion else "")

        tipo_relacion: TipoRelacion = "Intento"
        if estado.lower() == "finalizado":
            tipo_relacion = "Completado"
            if score is not None and abs(score - 100.0) < 1e-6:
                tipo_relacion = "Perfecto"

        funcion_relacion = relacionar_alumno_cuestionario if tipo_recurso == "Cuestionario" else relacionar_alumno_ayudantia
        session.execute_write(funcion_relacion, correo, recurso_nombre, tipo_relacion,
                              start_iso, end_iso, duration_seconds, score, estado)

# ----------------------------
# Procesar todas las unidades
# ----------------------------
def procesar_unidades(driver: Driver, base_path: Path) -> None:
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"La ruta base no es válida: {base_path}")

    with driver.session() as session:
        for unidad_dir in base_path.iterdir():
            if not unidad_dir.is_dir() or unidad_dir.name.lower() == "alumnos":
                continue

            print(f"📁 Procesando unidad: {unidad_dir.name}")

            # Cuestionarios
            cuestionarios_path = unidad_dir / "Cuestionarios"
            if cuestionarios_path.exists() and cuestionarios_path.is_dir():
                for archivo in cuestionarios_path.iterdir():
                    if archivo.is_file() and archivo.suffix.lower() == ".csv":
                        print(f"   📄 Procesando cuestionario: {archivo.name}")
                        procesar_csv(session, archivo, "Cuestionario")

            # Ayudantías
            ayudantias_path = unidad_dir / "Ayudantías"
            if ayudantias_path.exists() and ayudantias_path.is_dir():
                for archivo in ayudantias_path.iterdir():
                    if archivo.is_file() and archivo.suffix.lower() == ".csv":
                        print(f"   📄 Procesando ayudantía: {archivo.name}")
                        procesar_csv(session, archivo, "Ayudantia")
