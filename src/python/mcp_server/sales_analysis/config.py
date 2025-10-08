import logging
import os
import re
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file in the workshop folder
workshop_dir = Path(__file__).parent.parent.parent / "workshop"
env_path = workshop_dir / ".env"
load_dotenv(dotenv_path=env_path)

# Configure basic logging to show INFO level messages
logging.basicConfig(level=logging.INFO, format="%(levelname)s:     %(message)s")
logger = logging.getLogger(__name__)


class Config:
    """Configuration class for managing application settings."""

    def __init__(self) -> None:
        """Initialize configuration with environment variables."""
        # Load and clean PostgreSQL URL
        self._postgres_url: str = self._clean_env_value(
            os.getenv("POSTGRES_URL", "postgresql://store_manager:StoreManager123!@db:5432/zava")
        )


        # Load and clean Application Insights connection string
        appinsights_raw = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
        self._appinsights_connection_string: str = self._clean_env_value(appinsights_raw)

        # Always log configuration info
        self._log_config_info()

        # Validate that all required environment variables are set
        self.validate_required_env_vars()

    def _clean_env_value(self, value: str) -> str:
        """Strip surrounding quotes that might be added by Docker."""
        return value.strip('"').strip("'") if value else ""

    def _log_config_info(self) -> None:
        """Log configuration information."""
        logger.info("POSTGRES_URL: '%s'", self._postgres_url)
        logger.info(
            "APPLICATIONINSIGHTS_CONNECTION_STRING: '%s'",
            self._appinsights_connection_string,
        )

    @property
    def postgres_url(self) -> str:
        """Returns the PostgreSQL connection URL."""
        return self._postgres_url

    @property
    def applicationinsights_connection_string(self) -> str:
        """
        Returns the Application Insights connection string with cleaned endpoint URLs.
        Ensures endpoint URLs do not have trailing slashes.
        """
        if not self._appinsights_connection_string:
            return ""

        # Remove trailing slashes from IngestionEndpoint and LiveEndpoint
        return re.sub(
            r"(IngestionEndpoint|LiveEndpoint)=([^;]+)/",
            r"\1=\2",
            self._appinsights_connection_string,
        )

    def validate_required_env_vars(self) -> None:
        """
        Validate that all required environment variables are set.

        Raises:
            ValueError: If any required environment variables are missing or invalid
        """
        missing_vars = []

        if not self._postgres_url:
            missing_vars.append("POSTGRES_URL")

        if not self.applicationinsights_connection_string:
            missing_vars.append("APPLICATIONINSIGHTS_CONNECTION_STRING")

        if missing_vars:
            raise ValueError(f"Missing or invalid required environment variables: {', '.join(missing_vars)}")
