"""
ETL Package - Extract, Transform, Load operations for Star Schema
"""

from .load import run_etl
from .setup_star_schema import setup_star_schema

__all__ = ["run_etl", "setup_star_schema"]
