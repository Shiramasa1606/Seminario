# Neo4J/consultar.py
from typing import List, Mapping, Any, Optional, Dict, Callable


# Define type aliases for better clarity
ActivityDict = Dict[str, Any]
ProgressItem = Dict[str, Any]
RecommendationResult = Optional[Dict[str, Any]]
FetchNextFunction = Callable[[], Optional[ActivityDict]]


def recomendar_siguiente_from_progress(progreso: List[Mapping[str, Any]]) -> RecommendationResult:
    """
    Recibe la lista de progreso (cada item con 'tipo','nombre','estado',...) y devuelve:
      {"estrategia": "refuerzo|mejora|avance", "actividad": {...}}
    Reglas:
      - Si hay 'Intento' -> refuerzo: sugerir repetir ese recurso (primero encontrado)
      - Else si hay 'Completado' -> mejora: sugerir mejorar (primero encontrado)
      - Else si hay 'Perfecto' -> avance: devuelve None (quedan que consultar con Neo4J para next)
    """
    if not progreso:
        return None

    # Fix: Remove explicit type annotation or use proper conversion
    intentos = [dict(p) for p in progreso if p.get("estado") == "Intento"]
    if intentos:
        return {"estrategia": "refuerzo", "actividad": intentos[0]}

    completados = [dict(p) for p in progreso if p.get("estado") == "Completado"]
    if completados:
        return {"estrategia": "mejora", "actividad": completados[0]}

    perfectos = [dict(p) for p in progreso if p.get("estado") == "Perfecto"]
    if perfectos:
        # Para 'avance' devolvemos la señal; la resolución del siguiente recurso
        # (buscar en el grafo) la hace el módulo de consultas Neo4J.
        return {"estrategia": "avance", "actividad": perfectos[0]}

    return None


def generar_roadmap_from_progress_and_fetcher(
    progreso: List[Mapping[str, Any]],
    fetch_next_for_avance: FetchNextFunction
) -> List[Dict[str, Any]]:
    """
    Genera un roadmap en memoria a partir del progreso inicial.
    `fetch_next_for_avance` es una función (correo) -> next_activity_dict o None
    que se invoca cuando la estrategia es 'avance' y necesitamos consultar el grafo
    para obtener la siguiente actividad disponible.
    NOTA: No modifica la DB.
    """
    roadmap: List[Dict[str, Any]] = []
    # Fix: Add explicit type annotation for the set
    seen: set[tuple[Optional[str], Optional[str], str]] = set()

    # copia en memoria para simular progresos que vamos cambiando
    prog_map: Dict[tuple[Optional[str], Optional[str]], ActivityDict] = {
        (p.get("tipo"), p.get("nombre")): dict(p) for p in progreso 
    }

    while True:
        rec = recomendar_siguiente_from_progress(list(prog_map.values()))
        if not rec:
            break

        estrategia: str = rec["estrategia"]
        actividad: ActivityDict = rec["actividad"]

        # Si avance, necesitarás resolver la siguiente actividad en el grafo
        if estrategia == "avance":
            # fetch_next_for_avance debe devolver {"tipo":.., "nombre":..} o None
            siguiente: Optional[ActivityDict] = fetch_next_for_avance()
            if not siguiente:
                break
            # usar siguiente como la actividad a añadir
            actividad = siguiente

        # Fix: Add explicit type annotations and handle potential None values
        act_tipo: Optional[str] = actividad.get("tipo")
        act_nombre: Optional[str] = actividad.get("nombre")
        act_key: tuple[Optional[str], Optional[str], str] = (act_tipo, act_nombre, estrategia)
        
        if act_key in seen:
            break
        seen.add(act_key)
        roadmap.append({"estrategia": estrategia, "actividad": actividad})

        # simular avance en prog_map
        prog_key: tuple[Optional[str], Optional[str]] = (act_tipo, act_nombre)
        if prog_key in prog_map:
            if estrategia == "refuerzo":
                prog_map[prog_key]["estado"] = "Completado"
            elif estrategia == "mejora":
                prog_map[prog_key]["estado"] = "Perfecto"
            else:
                prog_map[prog_key]["estado"] = "Perfecto"
        else:
            # si no existía, añadir como completado (simulado)
            prog_map[prog_key] = {
                "tipo": act_tipo, 
                "nombre": act_nombre, 
                "estado": "Completado"
            }

    return roadmap