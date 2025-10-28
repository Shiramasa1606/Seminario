"""
Módulo de conexión a Neo4j.

Este archivo maneja la creación y administración del driver para conectarse
a una base de datos Neo4j utilizando las credenciales almacenadas en variables
de entorno.

Variables de entorno necesarias (definir en .env o sistema):
    - NEO4J_URI: URI de conexión (ej: bolt://localhost:7687)
    - NEO4J_USER: Usuario de Neo4j (ej: neo4j)
    - NEO4J_PASSWORD: Contraseña de Neo4j

Funciones principales:
    - obtener_driver(): Devuelve una instancia singleton del driver.
    - driver_context(): Proporciona un contexto seguro para usar el driver,
      cerrándolo automáticamente al salir del bloque `with`.

Buenas prácticas:
    - Usar `driver_context()` en scripts pequeños o de prueba.
    - Usar `obtener_driver()` en aplicaciones grandes donde el driver debe vivir
      mientras la app esté corriendo.
"""

import os
import logging
from neo4j import GraphDatabase, Driver
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Generator, Optional, Any, List

# Setup logging
logger = logging.getLogger(__name__)

# === Cargar y validar variables de entorno ===
load_dotenv()

def _obtener_variables_entorno() -> tuple[str, str, str]:
    """
    Obtiene y valida las variables de entorno necesarias.
    
    Returns:
        Tupla con (URI, USER, PASSWORD)
        
    Raises:
        EnvironmentError: Si faltan variables de entorno
    """
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    
    if not all([uri, user, password]):
        missing: List[str] = []
        if not uri: 
            missing.append("NEO4J_URI")
        if not user: 
            missing.append("NEO4J_USER") 
        if not password: 
            missing.append("NEO4J_PASSWORD")
        
        error_msg = f"❌ Faltan variables de entorno para Neo4j: {', '.join(missing)}"
        logger.error(error_msg)
        raise EnvironmentError(error_msg)
    
    return uri, user, password  # type: ignore

# Cargar variables una vez al importar el módulo
NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD = _obtener_variables_entorno()

# Driver singleton (se inicializa solo una vez)
_driver: Optional[Driver] = None
_driver_lock = None  # Para thread safety en el futuro

def obtener_driver() -> Driver:
    """
    Devuelve una instancia global (singleton) del driver de Neo4j.

    Si el driver aún no fue creado, se inicializa usando las variables
    de entorno cargadas. En posteriores llamadas, devuelve el mismo driver.

    Returns:
        neo4j.Driver: Instancia del driver de Neo4j.
    
    Raises:
        Exception: Si la conexión falla durante la creación del driver.
    """
    global _driver
    
    if _driver is not None:
        # Verificar que el driver todavía esté conectado
        try:
            _driver.verify_connectivity()
            return _driver
        except Exception:
            # El driver está desconectado, crear uno nuevo
            logger.warning("Driver existente desconectado, creando nuevo...")
            _driver = None
    
    if _driver is None:
        try:
            _driver = GraphDatabase.driver(
                NEO4J_URI, 
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                # Configuraciones opcionales para mejor rendimiento
                max_connection_lifetime=3600,  # 1 hora
                connection_acquisition_timeout=60,  # 60 segundos
                connection_timeout=30,  # 30 segundos
            )
            
            # Verificación de conectividad
            _driver.verify_connectivity()
            logger.info("✅ Driver de Neo4j creado y conectado exitosamente.")
            
        except Exception as e:
            logger.error(f"❌ Error al crear el driver de Neo4j: {e}")
            _driver = None
            raise
    
    return _driver


@contextmanager
def driver_context(
    uri: Optional[str] = None,
    user: Optional[str] = None, 
    password: Optional[str] = None
) -> Generator[Driver, None, None]:
    """
    Context manager para manejar la conexión con Neo4j de forma segura.

    Crea un driver temporal y lo cierra automáticamente al salir del bloque `with`.
    Permite sobreescribir credenciales para casos específicos.

    Uso:
        # Con credenciales por defecto
        with driver_context() as driver:
            with driver.session() as session:
                result = session.run("MATCH (n) RETURN n LIMIT 5")
                
        # Con credenciales personalizadas
        with driver_context(uri="bolt://other:7687", user="other") as driver:
            # usar driver...
                    
    Raises:
        Exception: Si la creación del driver falla.
    """
    driver: Optional[Driver] = None
    try:
        # Usar credenciales proporcionadas o las por defecto
        final_uri = uri or NEO4J_URI
        final_user = user or NEO4J_USER
        final_password = password or NEO4J_PASSWORD
        
        driver = GraphDatabase.driver(
            final_uri,
            auth=(final_user, final_password),
            max_connection_lifetime=1800,  # 30 minutos para contextos temporales
        )
        
        driver.verify_connectivity()
        logger.debug("Driver temporal de Neo4j creado para contexto.")
        yield driver
        
    except Exception as e:
        logger.error(f"❌ Error en driver_context: {e}")
        raise
    finally:
        if driver is not None:
            driver.close()
            logger.debug("Driver temporal de Neo4j cerrado.")


def cerrar_driver() -> None:
    """
    Cierra manualmente el driver singleton, si existe.

    Recomendado al finalizar la aplicación para liberar recursos.
    """
    global _driver
    if _driver is not None:
        try:
            _driver.close()
            logger.info("Driver de Neo4j cerrado exitosamente.")
        except Exception as e:
            logger.error(f"❌ Error cerrando driver: {e}")
        finally:
            _driver = None


def verificar_conexion(timeout: int = 5) -> bool:
    """
    Verifica que la conexión a Neo4j esté funcionando.
    
    Args:
        timeout: Tiempo máximo de espera en segundos
        
    Returns:
        bool: True si la conexión es exitosa, False en caso contrario.
    """
    try:
        driver = obtener_driver()
        with driver.session() as session:
            # Query simple y rápida para verificar conectividad
            result = session.run("RETURN 1 as connection_test", 
                               timeout=timeout * 1000)  # timeout en milisegundos
            single_result = result.single()
            return single_result is not None and single_result["connection_test"] == 1
    except Exception as e:
        logger.error(f"❌ Error verificando conexión: {e}")
        return False


def obtener_estado_base_datos() -> dict[str, Any]:
    """
    Obtiene información del estado de la base de datos Neo4j.
    
    Returns:
        Dict con información del estado de la BD
    """
    try:
        driver = obtener_driver()
        with driver.session() as session:
            # Información básica de la base de datos
            result = session.run("""
                CALL dbms.components() 
                YIELD name, versions, edition
                RETURN name, versions[0] as version, edition
            """)
            db_info = result.single()
            
            # Conteo de nodos básicos
            counts_result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY label
            """)
            counts: dict[str, Any] = {}
            for record in counts_result:
                label = record["label"]
                count = record["count"]
                if label:
                    counts[label] = count
            
            estado: dict[str, Any] = {
                "database_name": db_info["name"] if db_info else "Desconocido",
                "version": db_info["version"] if db_info else "Desconocido",
                "edition": db_info["edition"] if db_info else "Desconocido",
                "node_counts": counts,
                "connection_status": "✅ Conectado"
            }
            return estado
            
    except Exception as e:
        logger.error(f"❌ Error obteniendo estado de la base de datos: {e}")
        return {
            "connection_status": f"❌ Error: {str(e)}",
            "node_counts": {}
        }


# Cleanup al importar el módulo para evitar conexiones huérfanas
import atexit
atexit.register(cerrar_driver)