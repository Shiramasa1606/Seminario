# Proyecto Prototipo Recomendador con Neo4j

## Descripción

Este proyecto crea un prototipo básico de sistema de recomendación para actividades de aprendizaje usando Neo4j como base de datos gráfica.  
Incluye código para insertar datos, ejecutar consultas y obtener recomendaciones basadas en las relaciones entre alumnos y actividades.

---

## Requisitos previos

- Tener instalado Python 3.8 o superior.  
- Neo4j Desktop instalado en tu máquina (https://neo4j.com/download/).  
- Editor de código (VSCode recomendado).

---

## Paso a paso para usar el proyecto

### 1. Instalar Neo4j Desktop

- Descarga Neo4j Desktop desde [https://neo4j.com/download/](https://neo4j.com/download/).  
- Instálalo y ábrelo.  
- Crea un nuevo proyecto y dentro de él una nueva base de datos local.  
- Cuando crees la base de datos, asigna una contraseña (recuerda esta contraseña, la necesitarás luego).  
- Inicia la base de datos.

### 2. (Opcional) Crear carpeta para la base de datos

- Puedes linkear la base de datos a una carpeta específica en tu PC desde Neo4j Desktop para guardar los datos.  
- No es obligatorio, pero ayuda a organizar.

### 3. Clonar este repositorio y abrirlo

```bash
git clone https://github.com/Shiramasa1606/Seminario.git
cd Seminario
code .

### 4. Crear archivo `.env` con variables de entorno

En la raíz del proyecto, crea un archivo llamado `.env` con el siguiente contenido:

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=tu_contraseña_de_neo4j


> ⚠️ **Importante**: `tu_contraseña_de_neo4j` debe ser la contraseña que asignaste al crear la base de datos dentro de la aplicación **Neo4j Desktop**. No es una contraseña universal, es específica de la base de datos que estás utilizando en tu entorno local.

---

### 5. Instalar dependencias

Es recomendable usar un entorno virtual para Python:

```bash
python -m venv venv

# Activar entorno virtual:
# En Windows:
venv\Scripts\activate

# En Linux/macOS:
source venv/bin/activate

#Comando para realizar la instalacion
pip install -r requirements.txt

#Si no tienes el archivo requirements.txt, puedes instalar directamente:
pip install neo4j python-dotenv

###6. Ejecutar el proyecto

Para cargar los datos iniciales y hacer una consulta de recomendación, ejecuta:

```bash
python main.py
```
Deberías ver mensajes en consola confirmando la carga de datos y las recomendaciones para el alumno Alumno_001.