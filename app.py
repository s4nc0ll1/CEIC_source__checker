"""
CEIC Data Explorer Application

A Streamlit application for exploring and searching CEIC data series by source.
Provides authentication, source selection, and summary statistics for data series.
Includes pagination and filtering for sources with a large number of series.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable
import time

import pandas as pd
import streamlit as st
from ceic_api_client.pyceic import Ceic
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# ### MEJORA: Importar configuración centralizada
from config import API_BASE_URL, SOURCES_FILE_PATH, SERIES_THRESHOLD_FOR_WARNING, SERIES_PER_PAGE_IN_GRID, StateKey

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SessionManager:
    """
    ### MEJORA: Renombrada de SessionState a SessionManager para reflejar mejor su rol activo.
    Manages Streamlit session state with type safety using keys from the config.
    """
    
    @staticmethod
    def initialize() -> None:
        """Initialize session state variables if they don't exist."""
        # ### MEJORA: Usa StateKey para las claves, evitando errores de tipeo.
        default_states = {
            StateKey.LOGGED_IN: False,
            StateKey.CEIC_CLIENT: None,
            StateKey.SUMMARY_DATA: [],
            StateKey.SERIES_DETAILS: [],
            StateKey.SERIES_DETAILS_SOURCE_ID: None,
            StateKey.DETAILS_FILTER_KEYWORD: "",
            StateKey.SOURCE_ID_TO_LOAD: None
        }
        for key, default_value in default_states.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    @staticmethod
    def get_client() -> Optional[Ceic]:
        return st.session_state.get(StateKey.CEIC_CLIENT)

    @staticmethod
    def set_client(client: Ceic) -> None:
        st.session_state[StateKey.CEIC_CLIENT] = client
        st.session_state[StateKey.LOGGED_IN] = True

    @staticmethod
    def clear_session() -> None:
        """Clears user-specific session data on logout."""
        st.session_state[StateKey.LOGGED_IN] = False
        st.session_state[StateKey.CEIC_CLIENT] = None
        SessionManager.clear_search_results()

    @staticmethod
    def clear_search_results() -> None:
        """Clears all data related to a search (summary and details)."""
        st.session_state[StateKey.SUMMARY_DATA] = []
        SessionManager.clear_series_details()

    @staticmethod
    def clear_series_details() -> None:
        """Clears only the series details, keeping the summary."""
        st.session_state[StateKey.SERIES_DETAILS] = []
        st.session_state[StateKey.SERIES_DETAILS_SOURCE_ID] = None
        st.session_state[StateKey.DETAILS_FILTER_KEYWORD] = ""

    @staticmethod
    def set_summary_data(data: List[Dict[str, Any]]) -> None:
        st.session_state[StateKey.SUMMARY_DATA] = data
        st.session_state[StateKey.SOURCE_ID_TO_LOAD] = None # Reset load intent

    @staticmethod
    def get_summary_data() -> List[Dict[str, Any]]:
        return st.session_state.get(StateKey.SUMMARY_DATA, [])
        
    @staticmethod
    def set_series_details(data: List[Any], source_id: str) -> None:
        st.session_state[StateKey.SERIES_DETAILS] = data
        st.session_state[StateKey.SERIES_DETAILS_SOURCE_ID] = source_id
        st.session_state[StateKey.DETAILS_FILTER_KEYWORD] = ""
        st.session_state[StateKey.SOURCE_ID_TO_LOAD] = None


class DataLoader:
    """Handles loading of static data like sources."""
    
    @staticmethod
    @st.cache_data
    def load_sources() -> List[Dict[str, Any]]:
        """
        ### MEJORA: Docstring estilo Google y manejo de excepciones más específico.
        Loads source information from a JSON file.

        Returns:
            List[Dict[str, Any]]: A list of source dictionaries, or an empty list on error.
        """
        try:
            file_path = Path(SOURCES_FILE_PATH)
            if not file_path.exists():
                logger.error(f"Sources file not found: {SOURCES_FILE_PATH}")
                st.error(f"Error: Required file '{SOURCES_FILE_PATH}' not found.")
                return []
            
            with file_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
            
            sources = data.get("data", [])
            logger.info(f"Loaded {len(sources)} sources from {SOURCES_FILE_PATH}")
            return sources
        # ### MEJORA: Excepciones más específicas.
        except FileNotFoundError:
            logger.error(f"Sources file not found at path: {SOURCES_FILE_PATH}")
            st.error(f"Error: The source definition file '{SOURCES_FILE_PATH}' was not found.")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {SOURCES_FILE_PATH}: {e}")
            st.error(f"Error: Could not decode JSON from '{SOURCES_FILE_PATH}'. Please check its format.")
            return []
        except Exception as e:
            logger.error(f"Unexpected error loading sources: {e}", exc_info=True)
            st.error(f"An unexpected error occurred while loading sources.")
            return []

class DataProcessor:
    """Contains static methods for processing API results."""

    @staticmethod
    def create_summary_from_search(results_page: Any, source_id: str) -> Dict[str, Any]:
        """Creates a summary dictionary from the first page of search results."""
        if not (results_page and hasattr(results_page, 'data') and results_page.data):
            return {"ID": source_id, "Num Series": 0, "Info": "No series found or API error."}
        
        return {"ID": source_id, "Num Series": results_page.data.total}
    
    @staticmethod
    def process_full_metadata(metadata_list: List[Any]) -> Dict[str, Any]:
        """
        ### MEJORA: Esta lógica estaba dentro del método de servicio, ahora está separada.
        Calculates statistics from a full list of series metadata.
        """
        if not metadata_list:
            return {
                "Min Date": "N/A", "Max Date": "N/A", 
                "Active Series": 0, "Processed Series": 0
            }

        update_dates = [
            meta.last_update_time for meta in metadata_list 
            if hasattr(meta, 'last_update_time') and meta.last_update_time
        ]
        active_count = sum(
            1 for meta in metadata_list 
            if hasattr(meta, 'status') and getattr(meta.status, 'name', '') == "Active"
        )
        
        min_date = min(update_dates) if update_dates else None
        max_date = max(update_dates) if update_dates else None

        return {
            "Min Date": min_date.strftime('%Y-%m-%d') if min_date else "N/A",
            "Max Date": max_date.strftime('%Y-%m-%d') if max_date else "N/A",
            "Active Series": active_count,
            "Processed Series": len(metadata_list)
        }


class AuthenticationService:
    @staticmethod
    def authenticate(username: str, password: str) -> bool:
        if not username or not password:
            st.warning("Please enter both Access ID and Secret Key.")
            return False
        try:
            with st.spinner("Authenticating..."):
                client = Ceic.login(username, password)
                SessionManager.set_client(client)
            st.success("Authentication successful!")
            logger.info(f"User authenticated successfully: {username}")
            return True
        except Exception as e:
            # ### MEJORA: `exc_info=True` en el log para obtener el traceback completo para depuración.
            logger.error(f"Authentication failed for {username}: {e}", exc_info=True)
            st.error(f"Authentication failed. Please check your credentials and network connection.")
            SessionManager.clear_session()
            return False

class SearchService:
    """Handles all interactions with the CEIC Search API."""

    @staticmethod
    def search_by_source(source_id: str, source_name: str) -> None:
        client = SessionManager.get_client()
        if not client:
            st.error("Client not initialized. Please log in again.")
            return

        SessionManager.clear_search_results()
        try:
            with st.spinner(f"Searching series for '{source_name}'..."):
                # A quick search just to get the total count.
                search_iterator = client.search(source=[source_id])
                first_page = next(iter(search_iterator), None)
                summary = DataProcessor.create_summary_from_search(first_page, source_id)
            
            SessionManager.set_summary_data([summary])
            logger.info(f"Initial search for source {source_id} found {summary['Num Series']} series.")
        except Exception as e:
            logger.error(f"Search error for source {source_id}: {e}", exc_info=True)
            st.error(f"An error occurred during the search: {e}")
            SessionManager.clear_search_results()

    @staticmethod
    def get_all_series_for_source(source_id: str, total_series: int) -> None:
        """
        ### MEJORA: Este método ahora orquesta las llamadas a helpers y gestiona la UI.
        La lógica de obtención y procesamiento de datos está en métodos privados.
        """
        client = SessionManager.get_client()
        if not client:
            st.error("Client not initialized. Please log in again.")
            return

        progress_bar = st.progress(0, text=f"Preparing to load metadata for {total_series} series...")
        
        try:
            def update_progress(processed_count: int):
                """Callback function to update the Streamlit progress bar."""
                progress = min(1.0, processed_count / total_series)
                text = f"Processed {processed_count} of {total_series} series metadata..."
                progress_bar.progress(progress, text=text)

            # 1. Fetch data
            all_metadata = SearchService._fetch_series_metadata_pages(client, source_id, total_series, update_progress)
            
            # 2. Process data
            detailed_stats = DataProcessor.process_full_metadata(all_metadata)

            # 3. Update state
            summary_data = SessionManager.get_summary_data()
            if summary_data:
                summary_data[0].update(detailed_stats)
                summary_data[0]["Num Series"] = total_series # Ensure total is correct
                SessionManager.set_summary_data(summary_data)
            
            SessionManager.set_series_details(all_metadata, source_id)
            
            time.sleep(0.5) # Give user time to see the final progress
            progress_bar.empty()
            st.success("All series metadata loaded successfully!")
            logger.info(f"Fully loaded {len(all_metadata)} series for source {source_id}.")

        except Exception as e:
            progress_bar.empty()
            logger.error(f"Error fetching all series for source {source_id}: {e}", exc_info=True)
            st.error(f"An error occurred while fetching series details: {e}")
            SessionManager.clear_series_details()

    @staticmethod
    def _fetch_series_metadata_pages(
        client: Ceic, source_id: str, total_series: int, progress_callback: Callable[[int], None]
    ) -> List[Any]:
        """
        ### MEJORA: Nuevo método privado con una única responsabilidad: obtener datos.
        Iterates through all pages of a search result and returns all metadata items.
        Invokes a callback to report progress.
        """
        all_metadata = []
        processed_count = 0
        search_iterator = client.search(source=[source_id])
        
        for page in search_iterator:
            if page.data and hasattr(page.data, 'items'):
                items_on_page = page.data.items
                for item in items_on_page:
                    if hasattr(item, 'metadata'):
                        all_metadata.append(item.metadata)
                
                processed_count += len(items_on_page)
                progress_callback(processed_count)
        
        # Final callback update to ensure it reaches 100%
        progress_callback(total_series)
        return all_metadata


class UIComponents:
    """Handles rendering of all UI components for the application."""
    
    @staticmethod
    def render_login_page() -> None:
        st.set_page_config(page_title="Login - CEIC Explorer", layout="centered")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image("images/ceic.webp", width=250)
            st.title("CEIC Data Explorer")
            username = st.text_input("Username (Access ID)")
            password = st.text_input("Password (Secret Key)", type="password")
            if st.button("Sign In"):
                if AuthenticationService.authenticate(username, password):
                    st.rerun()

    @staticmethod
    def render_sidebar() -> Optional[Tuple[str, str]]:
        with st.sidebar:
            st.image("images/ceic.webp", width=200)
            st.header("Search Options")
            sources_list = DataLoader.load_sources()
            if not sources_list:
                st.warning("Could not load sources. Check logs for details.")
                return None
            
            source_options = {source['name']: source['id'] for source in sources_list}
            selected_source_name = st.selectbox("Select a source:", options=list(source_options.keys()))
            
            if st.button("Search Source"):
                selected_source_id = source_options[selected_source_name]
                return selected_source_id, selected_source_name
            return None

    @staticmethod
    def render_summary_table() -> None:
        st.header("Search Summary")
        summary_data = SessionManager.get_summary_data()
        if not summary_data:
            st.info("Select a source from the sidebar and click 'Search Source' to begin.")
            return
        
        summary = summary_data[0]
        if "Info" in summary:
            st.warning(summary["Info"])
            return

        col1, col2 = st.columns(2)
        col1.metric("Source ID", summary.get('ID', 'N/A'))
        num_series = summary.get('Num Series', 0)
        col2.metric("Total Series Found", f"{num_series:,}")

        if "Min Date" in summary: # Details are loaded
            st.markdown("---")
            st.subheader("Detailed Statistics")
            col3, col4, col5 = st.columns(3)
            col3.metric("Oldest Update", summary.get('Min Date', 'N/A'))
            col4.metric("Newest Update", summary.get('Max Date', 'N/A'))
            col5.metric("Active Series", f"{summary.get('Active Series', 0):,}")
        
        source_id = summary.get("ID")
        # ### MEJORA: Lógica de botón más clara
        # Mostrar el botón si hay series y los detalles para ESTA fuente no se han cargado.
        details_loaded_for_this_source = (st.session_state[StateKey.SERIES_DETAILS_SOURCE_ID] == source_id)
        if num_series > 0 and source_id and not details_loaded_for_this_source:
            if st.button(f"Load Metadata for all {num_series:,} Series"):
                st.session_state[StateKey.SOURCE_ID_TO_LOAD] = {"id": source_id, "count": num_series}
                st.rerun() # Dispara la lógica de carga en el flujo principal

    @staticmethod
    def render_series_details_section() -> None:
        """
        ### MEJORA: Método refactorizado para orquestar la renderización de la sección de detalles.
        """
        series_details = st.session_state.get(StateKey.SERIES_DETAILS, [])
        if not series_details:
            return

        st.markdown("---")
        st.header("Series Metadata Details")
        
        filtered_details = UIComponents._filter_series_details(series_details)
        if not filtered_details:
            st.warning("No series match your filter criteria.")
            return

        df_filtered = UIComponents._prepare_dataframe_for_grid(filtered_details)
        
        selected_row = UIComponents._render_interactive_grid(df_filtered)

        if selected_row:
            series_id = selected_row.get("Series ID")
            # Find the full metadata object from the original list
            selected_meta = next((meta for meta in filtered_details if str(getattr(meta, 'id', '')) == str(series_id)), None)
            if selected_meta:
                UIComponents._render_single_series_metadata(selected_meta)

    @staticmethod
    def _filter_series_details(series_details: List[Any]) -> List[Any]:
        """Handles the filtering logic based on user input."""
        filter_keyword = st.text_input(
            "Filter series by name or ID:", 
            value=st.session_state[StateKey.DETAILS_FILTER_KEYWORD],
            key="details_filter_input"
        )
        # If filter text changes, reset page and rerun
        if filter_keyword != st.session_state[StateKey.DETAILS_FILTER_KEYWORD]:
            st.session_state[StateKey.DETAILS_FILTER_KEYWORD] = filter_keyword
            st.rerun()

        if not filter_keyword:
            return series_details
        
        keyword_lower = filter_keyword.lower()
        return [
            meta for meta in series_details 
            if keyword_lower in str(getattr(meta, 'name', '')).lower() or \
               keyword_lower in str(getattr(meta, 'id', '')).lower()
        ]

    @staticmethod
    def _prepare_dataframe_for_grid(filtered_details: List[Any]) -> pd.DataFrame:
        """Converts metadata list to a DataFrame for AgGrid."""
        details_list = [
            {
                "Series ID": getattr(meta, 'id', 'N/A'),
                "Name": getattr(meta, 'name', 'N/A'),
                "Status": getattr(getattr(meta, 'status', None), 'name', 'N/A'),
                "Frequency": getattr(getattr(meta, 'frequency', None), 'name', 'N/A'),
                "Last Update": getattr(meta, 'last_update_time', pd.NaT)
            } for meta in filtered_details
        ]
        df = pd.DataFrame(details_list)
        # Format date for display
        if 'Last Update' in df.columns:
            df['Last Update'] = pd.to_datetime(df['Last Update']).dt.strftime('%Y-%m-%d %H:%M:%S').replace('NaT', 'N/A')
        return df

    @staticmethod
    def _render_interactive_grid(df: pd.DataFrame) -> Optional[Dict]:
        """Renders the AgGrid and returns the selected row."""
        st.subheader(f"Interactive List ({len(df):,} series shown)")
        st.info("Click on a row to select a series and view its details below.")

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_pagination(paginationPageSize=SERIES_PER_PAGE_IN_GRID)
        gb.configure_selection('single', use_checkbox=False)
        gb.configure_default_column(resizable=True, sortable=True, filter=True, wrapText=True, autoHeight=True)
        gb.configure_column("Name", width=400)

        grid_response = AgGrid(
            df,
            gridOptions=gb.build(),
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            fit_columns_on_grid_load=True,
            height=500,
            key='series_aggrid'
        )
        
        selected_rows_df = grid_response['selected_rows']

        if selected_rows_df is not None and not selected_rows_df.empty:
            return selected_rows_df.to_dict('records')[0]

        return None

    @staticmethod
    def _render_single_series_metadata(meta: Any) -> None:
        """
        ### MEJORA: Versión completamente rediseñada para manejar la estructura de datos real.
        Renders the detailed metadata for a single selected series in a structured and readable format.
        """
        if not meta:
            st.warning("No metadata available to display for the selected series.")
            return

        series_name = getattr(meta, 'name', 'N/A')
        st.markdown("---")
        st.header(f"Details for: {series_name}")

        # --- 1. Key Metrics (Lo más importante a simple vista) ---
        st.subheader("Key Metrics")
        col1, col2, col3 = st.columns(3)

        last_value = getattr(meta, 'last_value', 'N/A')
        # Formato numérico para mejor legibilidad
        formatted_value = f"{last_value:,.2f}" if isinstance(last_value, (int, float)) else str(last_value)
        col1.metric("Last Value", formatted_value)

        last_update = getattr(meta, 'last_update_time', None)
        col2.metric("Last Update", last_update.strftime('%Y-%m-%d %H:%M') if last_update else 'N/A')

        # Acceso seguro al objeto anidado 'status'
        status = getattr(getattr(meta, 'status', None), 'name', 'N/A')
        col3.metric("Status", status)

        # --- 2. Core Attributes (Descripción principal de la serie) ---
        st.subheader("Core Attributes")
        
        # Usamos getattr de forma segura para todos los atributos
        unit = getattr(getattr(meta, 'unit', None), 'name', 'N/A')
        frequency = getattr(getattr(meta, 'frequency', None), 'name', 'N/A')
        source_name = getattr(getattr(meta, 'source', None), 'name', 'N/A')
        start_date = getattr(meta, 'start_date', 'N/A')
        end_date = getattr(meta, 'end_date', 'N/A')
        num_obs = getattr(meta, 'number_of_observations', 0)

        st.markdown(f"""
        - **Series ID**: `{getattr(meta, 'id', 'N/A')}`
        - **Unit**: `{unit}`
        - **Frequency**: `{frequency}`
        - **Source**: `{source_name}`
        - **Date Range**: `{start_date}` to `{end_date}`
        - **Observations**: `{num_obs:,}`
        """)

        # --- 3. Classification and Geography (Expander para datos complejos) ---
        with st.expander("View Classification & Geography Details"):
            st.markdown("##### Indicator Path")
            indicator_paths = getattr(meta, 'indicators', [])
            if indicator_paths:
                for path in indicator_paths:
                    path_names = []
                    for node in path:
                        if hasattr(node, 'name'):
                            path_names.append(node.name)
                        elif isinstance(node, dict) and 'name' in node:
                            path_names.append(node['name'])
                        else:
                            path_names.append('Unknown')
                    
                    st.markdown(f"- `{' -> '.join(path_names)}`")
            else:
                st.info("No indicator path information available.")

            # Procesamiento de la información geográfica
            st.markdown("##### Geographical Information")
            geo_info = getattr(meta, 'geo_info', [])
            if geo_info:
                country_name = "N/A"
                regions = []
                # Iteramos para encontrar el país y las regiones
                for geo_item in geo_info:
                    item_type = getattr(geo_item, 'type', '')
                    item_name = getattr(geo_item, 'name', 'Unknown')
                    if item_type == 'COUNTRY':
                        country_name = item_name
                    elif item_type == 'REGION':
                        regions.append(item_name)
                
                st.markdown(f"**Country:** {country_name}")
                if regions:
                    st.markdown(f"**Associated Regions:**")
                    # Mostramos las regiones como una lista
                    for region in sorted(regions):
                        st.markdown(f"- {region}")
            else:
                st.info("No geographical information available.")

        # --- 4. Technical Flags (Expander para datos booleanos) ---
        with st.expander("View Technical Flags"):
            flags = {
                "Is Forecast": getattr(meta, 'is_forecast', False),
                "Is Key Series": getattr(meta, 'key_series', False),
                "Has Continuous Series": getattr(meta, 'has_continuous_series', False),
                "Has Vintage Data": getattr(meta, 'has_vintage', False),
                "Is New Series": getattr(meta, 'new_series', False),
                "Has Schedule": getattr(meta, 'has_schedule', False),
            }
            
            # Presentación más amigable que un JSON
            for flag_name, flag_value in flags.items():
                value_str = "✔️ Yes" if flag_value else "❌ No"
                st.markdown(f"- **{flag_name}**: {value_str}")

        # --- 5. Raw Data (Para depuración) ---
        with st.expander("View Raw Data Object"):
            # st.write es mejor que st.code para objetos de Python
            st.write(meta)
            
class CEICExplorerApp:
    """Main application class orchestrating the UI and services."""
    
    def __init__(self):

        Ceic.set_server(API_BASE_URL)
        SessionManager.initialize()

    def run(self) -> None:
        """Main execution loop of the Streamlit application."""
        if not st.session_state[StateKey.LOGGED_IN]:
            UIComponents.render_login_page()
        else:
            self._render_main_app()

    def _render_main_app(self) -> None:
        st.set_page_config(page_title="CEIC Series Search", layout="wide")
        st.title("CEIC Series Search by Source")

        # Handle sidebar actions
        search_request = UIComponents.render_sidebar()
        if search_request:
            source_id, source_name = search_request
            SearchService.search_by_source(source_id, source_name)
            # No need to rerun, Streamlit's flow will continue and redraw

        # Main content area
        col_main, _, col_logout = st.columns([20, 1, 2])
        with col_logout:
            if st.button("Logout"):
                SessionManager.clear_session()
                st.rerun()

        UIComponents.render_summary_table()
        self._handle_data_loading_logic()
        UIComponents.render_series_details_section()

    def _handle_data_loading_logic(self) -> None:
        """
        ### MEJORA: Lógica de carga extraída a su propio método para mayor claridad.
        Checks if a data load has been requested and triggers it.
        """
        load_request = st.session_state.get(StateKey.SOURCE_ID_TO_LOAD)
        if not load_request:
            return

        source_id = load_request["id"]
        num_series = load_request["count"]

        # If data is already loaded for this source, clear the request and exit.
        if st.session_state[StateKey.SERIES_DETAILS_SOURCE_ID] == source_id:
            st.session_state[StateKey.SOURCE_ID_TO_LOAD] = None
            return
        
        # Ask for confirmation if the dataset is large.
        if num_series > SERIES_THRESHOLD_FOR_WARNING:
            st.warning(
                f"This source contains {num_series:,} series. "
                f"Loading all metadata may take a few moments."
            )
            if st.button("Proceed to Load"):
                SearchService.get_all_series_for_source(source_id, num_series)
                st.rerun()
        else:
            # Load directly for smaller datasets.
            SearchService.get_all_series_for_source(source_id, num_series)
            st.rerun()

def main() -> None:
    """Entry point for the application."""
    app = CEICExplorerApp()
    app.run()

if __name__ == '__main__':
    main()