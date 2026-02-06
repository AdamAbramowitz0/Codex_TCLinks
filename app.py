"""Run the local market API server."""

from __future__ import annotations

import argparse

from tc_market.runtime import default_db_path, default_model_config_path
from tc_market.server import run_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tyler Cowen links market API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default=default_db_path(), help="SQLite DB path")
    parser.add_argument(
        "--model-config",
        default=default_model_config_path(),
        help="Path to model agent config file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_server(host=args.host, port=args.port, db_path=args.db, config_path=args.model_config)
