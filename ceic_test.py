"""
CEIC Data Explorer Application

A Streamlit application for exploring and searching CEIC data series by source.
Provides authentication, source selection, and summary statistics for data series.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import streamlit as st
from ceic_api_client.pyceic import Ceic

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SessionState:
    """Manages Streamlit session state with type safety."""
    
    @staticmethod
    def initialize() -> None:
        """Initialize session state variables with default values."""
        default_states = {
            'logged_in': False,
            'ceic_client': None,
            'summary_data': []
        }
        
        for key, default_value in default_states.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    @staticmethod
    def get_client() -> Optional[Ceic]:
        """Get the CEIC client from session state."""
        return st.session_state.get('ceic_client')

    @staticmethod
    def set_client(client: Ceic) -> None:
        """Set the CEIC client in session state."""
        st.session_state.ceic_client = client
        st.session_state.logged_in = True

    @staticmethod
    def clear_session() -> None:
        """Clear authentication session."""
        st.session_state.logged_in = False
        st.session_state.ceic_client = None

    @staticmethod
    def set_summary_data(data: List[Dict[str, Any]]) -> None:
        """Set summary data in session state."""
        st.session_state.summary_data = data

    @staticmethod
    def get_summary_data() -> List[Dict[str, Any]]:
        """Get summary data from session state."""
        return st.session_state.get('summary_data', [])


class DataLoader:
    """Handles data loading operations."""
    
    @staticmethod
    @st.cache_data
    def load_sources(json_file: str = "sources.json") -> List[Dict[str, Any]]:
        """
        Load sources data from JSON file with error handling.
        
        Args:
            json_file: Path to the JSON file containing sources data
            
        Returns:
            List of source dictionaries, empty list if error occurs
        """
        try:
            file_path = Path(json_file)
            if not file_path.exists():
                logger.error(f"Sources file not found: {json_file}")
                st.error(f"Error: Required file '{json_file}' not found.")
                return []
            
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            
            sources = data.get("data", [])
            logger.info(f"Loaded {len(sources)} sources from {json_file}")
            return sources
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {json_file}: {e}")
            st.error(f"Error: Could not decode JSON from '{json_file}'. Please check the content.")
            return []
        except Exception as e:
            logger.error(f"Unexpected error loading sources: {e}")
            st.error(f"Unexpected error loading sources: {e}")
            return []


class SearchResultProcessor:
    """Processes search results from CEIC API."""
    
    @staticmethod
    def process_results(results_page: Any, source_id: str) -> Dict[str, Any]:
        """
        Process API response and extract summary information.
        
        Args:
            results_page: API response page object
            source_id: Source identifier
            
        Returns:
            Dictionary containing processed summary data
        """
        if not SearchResultProcessor._validate_results(results_page):
            return {
                "ID": source_id,
                "Num Series": 0,
                "Min Date": "N/A",
                "Max Date": "N/A",
                "Info": "No series found for this source."
            }

        num_series = results_page.data.total
        update_dates = SearchResultProcessor._extract_update_dates(results_page)
        
        min_date_str, max_date_str = SearchResultProcessor._format_date_range(update_dates)

        return {
            "ID": source_id,
            "Num Series": num_series,
            "Min Date": min_date_str,
            "Max Date": max_date_str
        }

    @staticmethod
    def _validate_results(results_page: Any) -> bool:
        """Validate that results page contains data."""
        return (results_page and 
                hasattr(results_page, 'data') and 
                results_page.data and 
                hasattr(results_page.data, 'items') and 
                results_page.data.items)

    @staticmethod
    def _extract_update_dates(results_page: Any) -> List[datetime]:
        """Extract update dates from results."""
        update_dates = []
        
        for item in results_page.data.items:
            if (hasattr(item, 'metadata') and 
                hasattr(item.metadata, 'last_update_time') and 
                item.metadata.last_update_time is not None):
                update_dates.append(item.metadata.last_update_time)
        
        return update_dates

    @staticmethod
    def _format_date_range(update_dates: List[datetime]) -> tuple[str, str]:
        """Format date range from list of datetime objects."""
        if not update_dates:
            return "N/A", "N/A"
        
        min_date = min(update_dates).strftime('%Y-%m-%d')
        max_date = max(update_dates).strftime('%Y-%m-%d')
        
        return min_date, max_date


class AuthenticationService:
    """Handles CEIC authentication."""
    
    @staticmethod
    def authenticate(username: str, password: str) -> bool:
        """
        Authenticate user with CEIC API.
        
        Args:
            username: CEIC Access ID
            password: CEIC Secret Key
            
        Returns:
            True if authentication successful, False otherwise
        """
        if not username or not password:
            st.warning("Please enter both username and password.")
            return False
        
        try:
            with st.spinner("Authenticating..."):
                client = Ceic.login(username, password)
                SessionState.set_client(client)
                st.success("Authentication successful!")
                logger.info(f"User authenticated successfully: {username}")
                return True
                
        except Exception as e:
            error_msg = f"Authentication failed: {e}. Please check your credentials."
            st.error(error_msg)
            logger.error(f"Authentication failed for {username}: {e}")
            SessionState.clear_session()
            return False


class SearchService:
    """Handles search operations."""
    
    @staticmethod
    def search_by_source(source_id: str, source_name: str) -> None:
        """
        Search for series by source and update session state.
        
        Args:
            source_id: CEIC source identifier
            source_name: Human-readable source name
        """
        client = SessionState.get_client()
        if not client:
            st.error("Error: CEIC client not initialized. Please log in again.")
            return

        try:
            with st.spinner(f"Searching and processing series for '{source_name}'..."):
                # Get first page of results
                search_iterator = client.search(source=[source_id])
                first_page = next(iter(search_iterator), None)
                
                # Process results
                summary = SearchResultProcessor.process_results(first_page, source_id)
                SessionState.set_summary_data([summary])
                
                logger.info(f"Search completed for source {source_id}: {summary['Num Series']} series found")
                
        except Exception as e:
            error_msg = f"Error during search: {e}"
            st.error(error_msg)
            logger.error(f"Search error for source {source_id}: {e}")
            SessionState.set_summary_data([])


class UIComponents:
    """UI component builders."""
    
    @staticmethod
    def render_login_page() -> None:
        """Render the login page."""
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
    def render_sidebar() -> Optional[tuple[str, str]]:
        """
        Render sidebar with source selection.
        
        Returns:
            Tuple of (source_id, source_name) if search requested, None otherwise
        """
        with st.sidebar:
            st.image("images/ceic.webp", width=200)
            st.header("Search Options")

            sources_list = DataLoader.load_sources()
            
            if not sources_list:
                st.error("Could not load sources for search.")
                return None

            source_options = {source['name']: source['id'] for source in sources_list}
            
            selected_source_name = st.selectbox(
                "Select a source:",
                options=list(source_options.keys())
            )
            
            selected_source_id = source_options[selected_source_name]
            
            if st.button("Search Series for This Source"):
                return selected_source_id, selected_source_name
            
            return None

    @staticmethod
    def render_summary_table() -> None:
        """Render the summary results table."""
        st.header("Search Summary")
        
        summary_data = SessionState.get_summary_data()
        
        if not summary_data:
            st.info("Select a source from the left menu and click 'Search' to see the summary here.")
            return
        
        df = pd.DataFrame(summary_data)
        
        # Handle case where no series were found
        if "Info" in df.columns:
            st.warning(df["Info"].iloc[0])
            return
        
        # Rename columns for better display
        column_mapping = {
            'ID': 'Source ID',
            'Num Series': 'Total Series Found',
            'Min Date': 'Oldest Update Date',
            'Max Date': 'Most Recent Update Date'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Display table
        st.dataframe(df.set_index('Source ID'), use_container_width=True)
        
        # Add explanatory note
        st.caption(
            "Note: Update dates (min and max) are calculated from the first page of API results "
            "(typically the first 100 series) to ensure fast response. The 'Total Series Found' "
            "represents the complete and accurate count."
        )


class CEICExplorerApp:
    """Main application class."""
    
    def __init__(self):
        """Initialize the application."""
        SessionState.initialize()
        Ceic.set_server("https://api.ceicdata.com/v2")

    def run(self) -> None:
        """Run the main application."""
        if st.session_state.logged_in:
            self._render_main_app()
        else:
            UIComponents.render_login_page()

    def _render_main_app(self) -> None:
        """Render the main application interface."""
        st.set_page_config(page_title="CEIC Series Search", layout="wide")
        st.title("Series Search by Source")

        # Handle sidebar interactions
        search_request = UIComponents.render_sidebar()
        if search_request:
            source_id, source_name = search_request
            SearchService.search_by_source(source_id, source_name)

        # Render main content
        UIComponents.render_summary_table()


def main() -> None:
    """Application entry point."""
    app = CEICExplorerApp()
    app.run()


if __name__ == '__main__':
    main()