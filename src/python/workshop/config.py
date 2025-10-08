import logging
import os
import re
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure basic logging to show INFO level messages
logging.basicConfig(level=logging.INFO, format="%(levelname)s:     %(message)s")
logger = logging.getLogger(__name__)


class Config:
    """Configuration class for managing application settings."""

    def __init__(self) -> None:
        """Initialize configuration with environment variables."""
        # Agent configuration
        self._agent_name = "Zava DIY Sales Analysis Agent"

        # Load and clean Azure configuration from environment variables
        self._gpt_model_deployment_name: str = self._clean_env_value(os.environ["GPT_MODEL_DEPLOYMENT_NAME"])
        self._embedding_model_deployment_name: str = self._clean_env_value(
            os.environ["EMBEDDING_MODEL_DEPLOYMENT_NAME"]
        )
        self._project_endpoint: str = self._clean_env_value(os.environ["PROJECT_ENDPOINT"])

        # Load and clean Application Insights connection string
        appinsights_raw = os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"]
        self._appinsights_connection_string: str = self._clean_env_value(appinsights_raw)

        # Model parameters
        self._max_completion_tokens = 4 * 10240
        self._max_prompt_tokens = 20 * 10240
        self._temperature = 0.1
        self._top_p = 0.1

        # Chat/Response timeout settings
        self._response_timeout_seconds = 60

        # Compute dev tunnel URL
        self._dev_tunnel_url: str = self._compute_dev_tunnel_url()

        # Always log configuration info
        self._log_config_info()

        # Validate that all required environment variables are set
        self.validate_required_env_vars()

    def _clean_env_value(self, value: str) -> str:
        """Strip surrounding quotes that might be added by Docker."""
        return value.strip('"').strip("'") if value else ""

    def _log_config_info(self) -> None:
        """Log configuration information."""
        logger.info("AGENT_NAME: '%s'", self._agent_name)
        logger.info("GPT_MODEL_DEPLOYMENT_NAME: '%s'", self._gpt_model_deployment_name)
        logger.info("EMBEDDING_MODEL_DEPLOYMENT_NAME: '%s'", self._embedding_model_deployment_name)
        logger.info("PROJECT_ENDPOINT: '%s'", self._project_endpoint)
        logger.info("APPLICATIONINSIGHTS_CONNECTION_STRING: '%s'", self._appinsights_connection_string)
        logger.info("DEV_TUNNEL_URL: '%s'", self._dev_tunnel_url)

    def _compute_dev_tunnel_url(self) -> str:
        """Compute the dev tunnel URL at initialization time."""
        try:
            # Look for dev_tunnel.log in the shared scripts directory
            log_file_path =  Path(__file__).parent / "dev_tunnel.log"
            print(f"Looking for dev tunnel log file at: {log_file_path.resolve()}")
            with log_file_path.open("r") as file:
                for line in file:
                    line = line.strip()
                    if line.startswith("Connect via browser:"):
                        # Extract URLs from the line
                        urls_part = line.split("Connect via browser:")[1].strip()
                        urls = [url.strip() for url in urls_part.split(",")]
                        if len(urls) >= 2:
                            # Use the second URL and append /mcp
                            return urls[1].rstrip("/") + "/mcp"
            # remove the file if we reach here
            log_file_path.unlink(missing_ok=True)
            raise RuntimeError(
                "Dev tunnel URL not found in log file. Ensure the devtunnel is authenticated (devtunnel login) and running. Be sure to stop the Dev Tunnel task before restarting the application."
            )
        except (FileNotFoundError, Exception):
            raise RuntimeError(
                "The devtunnel log file not found or could not be read. Ensure the devtunnel is authenticated (devtunnel login) and running. Be sure to stop the Dev Tunnel task before restarting the application."
            ) from None

    # Properties for all configuration values
    @property
    def agent_name(self) -> str:
        """Returns the agent name."""
        return self._agent_name

    @property
    def gpt_model_deployment_name(self) -> str:
        """Returns the GPT model deployment name."""
        return self._gpt_model_deployment_name

    @property
    def embedding_model_deployment_name(self) -> str:
        """Returns the embedding model deployment name."""
        return self._embedding_model_deployment_name

    @property
    def project_endpoint(self) -> str:
        """Returns the project endpoint."""
        return self._project_endpoint

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

    @property
    def dev_tunnel_url(self) -> str:
        """Returns the dev tunnel URL."""
        return self._dev_tunnel_url

    @property
    def max_completion_tokens(self) -> int:
        """Returns the maximum completion tokens."""
        return self._max_completion_tokens

    @property
    def max_prompt_tokens(self) -> int:
        """Returns the maximum prompt tokens."""
        return self._max_prompt_tokens

    @property
    def temperature(self) -> float:
        """Returns the temperature setting."""
        return self._temperature

    @property
    def top_p(self) -> float:
        """Returns the top_p setting."""
        return self._top_p

    @property
    def response_timeout_seconds(self) -> int:
        """Returns the response timeout in seconds."""
        return self._response_timeout_seconds

    class Rls:
        """RLS configuration for PostgreSQL Row Level Security."""

        ZAVA_HEADOFFICE_USER_ID: str = "00000000-0000-0000-0000-000000000000"
        ZAVA_SEATTLE_USER_ID: str = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
        ZAVA_BELLEVUE_USER_ID: str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        ZAVA_TACOMA_USER_ID: str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        ZAVA_SPOKANE_USER_ID: str = "d8e9f0a1-b2c3-4567-8901-234567890abc"
        ZAVA_EVERETT_USER_ID: str = "3b9ac9fa-cd5e-4b92-a7f2-b8c1d0e9f2a3"
        ZAVA_REDOND_USER_ID: str = "e7f8a9b0-c1d2-3e4f-5678-90abcdef1234"
        ZAVA_KIRKLAND_USER_ID: str = "9c8b7a65-4321-fed0-9876-543210fedcba"
        ZAVA_ONLINE_USER_ID: str = "2f4e6d8c-1a3b-5c7e-9f0a-b2d4f6e8c0a2"

    def validate_required_env_vars(self) -> None:
        """
        Validate that all required environment variables are set.

        Raises:
            ValueError: If any required environment variables are missing or invalid
        """
        missing_vars = []

        if not self._project_endpoint:
            missing_vars.append("PROJECT_ENDPOINT")
        if not self._gpt_model_deployment_name:
            missing_vars.append("GPT_MODEL_DEPLOYMENT_NAME")
        if not self._embedding_model_deployment_name:
            missing_vars.append("EMBEDDING_MODEL_DEPLOYMENT_NAME")
        if not self.applicationinsights_connection_string:
            missing_vars.append("APPLICATIONINSIGHTS_CONNECTION_STRING")

        if missing_vars:
            raise ValueError(f"Missing or invalid required environment variables: {', '.join(missing_vars)}")
