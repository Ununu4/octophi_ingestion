"""Ingestion modules (Postgres, template-only)."""

from .schema_loader import Schema
from .template_mapper import TemplateMapper
from .type_normalizer import Normalizer
from .deep_cleaner import DeepCleaner
from .postgres_ingest_engine import PostgresIngestEngine

__all__ = [
    'Schema',
    'TemplateMapper',
    'Normalizer',
    'DeepCleaner',
    'PostgresIngestEngine',
]
