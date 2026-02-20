"""
Template validation before cleaning.

Validates that mapping template is consistent with schema and covers required fields.
"""

from typing import List

from .schema_loader import Schema


def validate_template(schema: Schema, mapper) -> List[str]:
    """
    Validate template against schema. Call before running DeepCleaner.

    Checks:
    1. Every expected_schema field in template exists in schema (lead or owner).
    2. No duplicate mappings: two different incoming columns mapped to same expected field.
    3. Required schema fields (except 'source') are either mapped or derived.

    Returns:
        List of error messages (empty if valid).
    """
    errors: List[str] = []
    direct_pairs = mapper.get_direct_pairs()
    combinations = mapper.get_combinations()

    lead_fields = set(schema.fields('lead'))
    owner_fields = set(schema.fields('owner'))
    schema_fields = lead_fields | owner_fields

    expected_set = set()
    for _incoming, expected in direct_pairs:
        expected_set.add(expected)
    for combo in combinations:
        expected_set.add(combo['target_field'])

    # 1) Every expected_schema must exist in schema
    for expected in expected_set:
        if expected not in schema_fields:
            errors.append(f"Template maps to field '{expected}' which is not in schema (lead or owner).")

    # 2) Duplicate mapping: two different incoming columns -> same expected (direct mappings only)
    seen_expected_to_incoming: dict = {}
    for incoming, expected in direct_pairs:
        if expected in seen_expected_to_incoming and seen_expected_to_incoming[expected] != incoming:
            errors.append(
                f"Duplicate mapping: two incoming columns ('{seen_expected_to_incoming[expected]}' and '{incoming}') "
                f"map to same expected field '{expected}'."
            )
        seen_expected_to_incoming[expected] = incoming

    # 3) Required fields must be mapped or derived
    for entity in ('lead', 'owner'):
        for field in schema.fields(entity):
            if not schema.is_required(entity, field):
                continue
            if field == 'source':
                continue
            if schema.derived_from(entity, field):
                continue
            if field in expected_set:
                continue
            errors.append(
                f"Required field '{field}' ({entity}) is not mapped in template and is not derived."
            )

    return errors
