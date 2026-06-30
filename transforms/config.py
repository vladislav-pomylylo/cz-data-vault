from pathlib import Path
from sqlmesh.core.config import (
    Config,
    DuckDBConnection,
    ModelDefaultsConfig,
    GatewayConfig,
)

PROJECT_DIR = Path(__file__).parent

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnection(
                database=str(PROJECT_DIR / "europe_data.db")
            ),
        ),
    },
    default_gateway="local",
)
