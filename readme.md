# 🧠 Proyecto Prototipo Recomendador con Neo4j

## 📘 Descripción

Este proyecto crea un prototipo básico de sistema de recomendación para actividades de aprendizaje utilizando Neo4j como base de datos gráfica.  
Incluye código para insertar datos, ejecutar consultas y obtener recomendaciones basadas en las relaciones entre alumnos y actividades.

## ⚙️ Requisitos previos

- **Python 3.8 o superior**  
- **Neo4j Desktop** instalado en tu máquina → [Descargar aquí](https://neo4j.com/download/)  
- **Editor de código** (recomendado: VSCode)  

## 🚀 Paso a paso para usar el proyecto

### 1️⃣ Instalar Neo4j Desktop

- Descarga Neo4j Desktop desde [https://neo4j.com/download/](https://neo4j.com/download/)
- Instálalo y ábrelo
- Crea un nuevo proyecto y dentro de él, una base de datos local
- Asigna una contraseña (⚠️ recuerda esta contraseña, la necesitarás luego)
- Inicia la base de datos

### 2️⃣ (Opcional) Crear carpeta para la base de datos

- Puedes vincular la base de datos a una carpeta específica en tu PC desde Neo4j Desktop para guardar los datos
- No es obligatorio, pero ayuda a mantener el proyecto organizado

### 3️⃣ Clonar este repositorio y abrirlo

Ejecuta los siguientes comandos en tu terminal:

```bash
git clone https://github.com/Shiramasa1606/Seminario.git
cd Seminario
code .
```

### 4️⃣ Crear archivo .env con variables de entorno

En la raíz del proyecto, crea un archivo llamado `.env` con el siguiente contenido:

```ini
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=tu_contraseña_de_neo4j
```

⚠️ **Importante**: `tu_contraseña_de_neo4j` debe ser la contraseña que asignaste al crear la base de datos dentro de la aplicación **Neo4j Desktop**. No es una contraseña universal, es específica de tu base de datos local.

### 5️⃣ Instalar dependencias

Se recomienda usar un entorno virtual de Python para aislar las dependencias del proyecto.

```bash
python -m venv venv

# Windows:
venv\Scripts\activate

# Linux/macOS:
source venv/bin/activate

# Instalar dependencias:
pip install -r requirements.txt
```

Si no tienes `requirements.txt`, instala manualmente:

```bash
pip install neo4j python-dotenv pandas
```
### 6️⃣ Ejecutar el proyecto

Para cargar los datos iniciales y realizar una consulta de recomendación, ejecuta:

```bash
python main.py
```

Si todo está correcto, verás en consola un menú donde puedes realizar la carga de los datos, mirar el rendimiento de un alumno en específico y ver cómo está compuesto el grafo (cantidad de nodos, tipos de nodos, etc.).

## 🧩 Funcionalidades del sistema

- **Inserción de datos**: pobla la base con alumnos, actividades y relaciones
- **Consultas de progreso**: visualiza el estado de cada alumno
- **Recomendaciones inteligentes**: sugiere actividades basadas en el progreso
- **Roadmap de aprendizaje**: genera un plan personalizado
- **Análisis comparativo**: compara el rendimiento con estadísticas globales

## 📂 Estructura del proyecto

```text
Prototipo Recomendador/
├── main.py
├── requirements.txt
├── .env
└── Neo4J/
    ├── conn.py
    ├── consultar.py
    ├── neo_queries.py
    └── Inserts/
        ├── insertMain.py
        ├── insertarAlumnos.py
        ├── insertarMaterial.py
        ├── insertarCuestionariosAyudantias.py
        └── Relaciones/
            ├── relacionarAlumnos.py
            └── relacionarMaterial.py
```