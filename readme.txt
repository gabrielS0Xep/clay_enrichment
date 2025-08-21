## Waterfall Enrichment
Este proyecto ha sido transformado de un scraper batch a una API REST con Flask. Ahora se puede obtener informacion diaria de contactos, que estara cargada en bigquery.

## 📁 Estructura del Proyecto
```
waterfall-enrichment/
├── .env                    # Variables de entorno (crear manualmente)
├── config.py              # Configuración y variables de entorno
├── linkedin_scraper.py    # Clase principal del scraper
├── main.py                # API Flask
├── requirements.txt       # Dependencias (actualizado)
└── README_refactorizado.md # Este archivo
```
## 🚀 Configuración Inicial
1. Crear archivo .env
Crea un archivo .env en la raíz del proyecto con las siguientes variables:

# API Keys para el Waterfall Enrichment

# Configuración Flask
FLASK_HOST=0.0.0.0
PORT=5000
FLASK_DEBUG=False

2. Instalar dependencias
pip install -r requirements.txt