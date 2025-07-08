# config.py
"""
Módulo de configuración centralizada para la aplicación CEIC Data Explorer.
"""
from enum import Enum

# --- Configuración de la API y Archivos ---
API_BASE_URL = "https://api.ceicdata.com/v2"
SOURCES_FILE_PATH = "sources.json"

# --- Constantes de la Aplicación ---
SERIES_THRESHOLD_FOR_WARNING = 500
SERIES_PER_PAGE_IN_GRID = 50

# --- Claves para el Estado de Sesión de Streamlit ---
# Usar una clase/Enum previene errores de tipeo al acceder a st.session_state.
class StateKey(str, Enum):
    LOGGED_IN = 'logged_in'
    CEIC_CLIENT = 'ceic_client'
    SUMMARY_DATA = 'summary_data'
    SERIES_DETAILS = 'series_details'
    SERIES_DETAILS_SOURCE_ID = 'series_details_source_id'
    DETAILS_FILTER_KEYWORD = 'details_filter_keyword'
    SOURCE_ID_TO_LOAD = 'source_id_to_load'
    
    def __str__(self) -> str:
        return self.value