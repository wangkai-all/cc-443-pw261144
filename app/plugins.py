"""Parser plugin engine.

A plugin *version* is a declarative spec with three parts:

  parser         -- how to turn a raw payload into raw fields
                    (delimited / keyvalue / json)
  field_mapping  -- raw field -> canonical field, with type conversion.
                    Only mapped targets may appear in canonical output;
                    everything else lands in `extras`. This is the new-field
                    isolation boundary: a v2-only field can never leak into
                    output shaped by a v1 mapping.
  validation     -- rules evaluated against canonical values.

All functions here are pure: no I/O, no shared state, safe to call from
concurrent batch processors and from compatibility-report dry runs.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

FORMATS = {"delimited", "keyvalue", "json"}
TYPES = {"string", "int", "decimal", "bool", "datetime"}
RULES = {"required", "regex", "range", "enum", "length"}


class SpecError(ValueError):
    """Raised when a plugin spec is structurally invalid."""


@dataclass
class Outcome:
    status: str                      # ok | error | empty
    canonical: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------- spec check

def validate_spec(spec: dict) -> None:
    if not isinstance(spec, dict):
        raise SpecError("spec must be an object")
    parser = spec.get("parser")
    mapping = spec.get("field_mapping")
    rules = spec.get("validation", [])
    if not isinstance(parser, dict) or parser.get("format") not in FORMATS:
        raise SpecError(f"parser.format must be one of {sorted(FORMATS)}")
    if parser["format"] == "delimited":
        if not parser.get("delimiter"):
            raise SpecError("delimited parser requires delimiter")
        if not isinstance(parser.get("columns"), list) or not parser["columns"]:
            raise SpecError("delimited parser requires non-empty columns")
    if parser["format"] == "keyvalue" and not parser.get("pair_delimiter"):
        raise SpecError("keyvalue parser requires pair_delimiter")
    if not isinstance(mapping, dict) or not mapping:
        raise SpecError("field_mapping must be a non-empty object")
    targets = []
    for raw_field, m in mapping.items():
        if not isinstance(m, dict) or "target" not in m:
            raise SpecError(f"mapping for {raw_field!r} requires target")
        if m.get("type", "string") not in TYPES:
            raise SpecError(f"mapping for {raw_field!r} has unknown type")
        targets.append(m["target"])
    if len(set(targets)) != len(targets):
        raise SpecError("duplicate canonical targets in field_mapping")
    if not isinstance(rules, list):
        raise SpecError("validation must be a list")
    for r in rules:
        if not isinstance(r, dict) or r.get("rule") not in RULES:
            raise SpecError(f"validation rule must be one of {sorted(RULES)}")
        if r.get("field") not in targets:
            raise SpecError(f"validation rule targets unmapped field {r.get('field')!r}")


# ---------------------------------------------------------------- extraction

def _extract_raw_fields(parser: dict, raw: Any) -> dict[str, Any]:
    fmt = parser["format"]
    if fmt == "json":
        if not isinstance(raw, dict):
            raise ValueError("expected JSON object payload")
        return dict(raw)
    if not isinstance(raw, str):
        raise ValueError("expected string payload")
    text = raw.strip() if parser.get("trim", True) else raw
    if fmt == "delimited":
        parts = text.split(parser["delimiter"])
        parts = [p.strip() for p in parts] if parser.get("trim", True) else parts
        columns = parser["columns"]
        if len(parts) > len(columns):
            # extra columns are never silently absorbed into canonical output
            raise ValueError(
                f"column count mismatch: expected {len(columns)}, got {len(parts)}")
        if len(parts) < len(columns) and not parser.get("allow_missing_columns"):
            raise ValueError(
                f"column count mismatch: expected {len(columns)}, got {len(parts)}")
        return dict(zip(columns, parts))  # missing trailing columns -> absent
    if fmt == "keyvalue":
        kv_sep = parser.get("kv_separator", "=")
        out: dict[str, Any] = {}
        for pair in text.split(parser["pair_delimiter"]):
            pair = pair.strip()
            if not pair:
                continue
            if kv_sep not in pair:
                raise ValueError(f"malformed pair {pair!r}")
            k, v = pair.split(kv_sep, 1)
            out[k.strip()] = v.strip()
        return out
    raise ValueError(f"unknown format {fmt!r}")


# ---------------------------------------------------------------- conversion

def _convert(value: Any, type_name: str) -> Any:
    if value is None:
        return None
    if type_name == "string":
        return str(value)
    if type_name == "int":
        return int(str(value).strip())
    if type_name == "decimal":
        return float(str(value).strip())
    if type_name == "bool":
        v = str(value).strip().lower()
        if v in {"true", "1", "yes", "y"}:
            return True
        if v in {"false", "0", "no", "n"}:
            return False
        raise ValueError(f"not a bool: {value!r}")
    if type_name == "datetime":
        return datetime.fromisoformat(str(value).strip()).isoformat()
    raise ValueError(f"unknown type {type_name!r}")


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v == "")


# ---------------------------------------------------------------- validation

def _check_rule(rule: dict, canonical: dict) -> str | None:
    name = rule["rule"]
    value = canonical.get(rule["field"])
    if name == "required":
        if _is_blank(value):
            return f"{rule['field']}: required but missing"
        return None
    if _is_blank(value):
        return None  # non-required blank fields skip other checks
    if name == "regex":
        if not re.fullmatch(rule["pattern"], str(value)):
            return f"{rule['field']}: {value!r} does not match {rule['pattern']!r}"
    elif name == "range":
        try:
            num = float(value)
        except (TypeError, ValueError):
            return f"{rule['field']}: not numeric for range check"
        if "min" in rule and num < rule["min"]:
            return f"{rule['field']}: {num} < min {rule['min']}"
        if "max" in rule and num > rule["max"]:
            return f"{rule['field']}: {num} > max {rule['max']}"
    elif name == "enum":
        if value not in rule["values"]:
            return f"{rule['field']}: {value!r} not in enum"
    elif name == "length":
        n = len(str(value))
        if "min" in rule and n < rule["min"]:
            return f"{rule['field']}: length {n} < min {rule['min']}"
        if "max" in rule and n > rule["max"]:
            return f"{rule['field']}: length {n} > max {rule['max']}"
    return None


# ---------------------------------------------------------------- entrypoint

def parse_record(spec: dict, raw: Any) -> Outcome:
    """Parse one raw payload. Pure: no I/O, no shared mutable state."""
    errors: list[str] = []
    try:
        raw_fields = _extract_raw_fields(spec["parser"], raw)
    except ValueError as exc:
        return Outcome(status="error", errors=[f"parse: {exc}"])

    canonical: dict[str, Any] = {}
    consumed = set()
    for raw_field, m in spec["field_mapping"].items():
        consumed.add(raw_field)
        value = raw_fields.get(raw_field)
        if _is_blank(value):
            value = m.get("default")
        if _is_blank(value):
            canonical[m["target"]] = None
            if m.get("required"):
                errors.append(f"{m['target']}: required but missing")
            continue
        try:
            canonical[m["target"]] = _convert(value, m.get("type", "string"))
        except (ValueError, TypeError) as exc:
            canonical[m["target"]] = None
            errors.append(f"{m['target']}: conversion failed ({exc})")

    # Isolation: anything the mapping does not know about is sidecarred into
    # extras, never merged into canonical output.
    extras = {k: v for k, v in raw_fields.items() if k not in consumed}

    for rule in spec.get("validation", []):
        problem = _check_rule(rule, canonical)
        if problem:
            errors.append(problem)

    if errors:
        return Outcome(status="error", canonical=canonical, extras=extras, errors=errors)
    if all(_is_blank(v) for v in canonical.values()):
        return Outcome(status="empty", canonical=canonical, extras=extras)
    return Outcome(status="ok", canonical=canonical, extras=extras)


# ---------------------------------------------------------------- spec diff

def diff_specs(a: dict, b: dict) -> dict:
    """Structural diff between two plugin specs, for the compare view."""
    out = {"parser_changes": [], "mapping_added": [], "mapping_removed": [],
           "mapping_changed": [], "validation_added": [], "validation_removed": []}
    pa, pb = a.get("parser", {}), b.get("parser", {})
    for key in sorted(set(pa) | set(pb)):
        if pa.get(key) != pb.get(key):
            out["parser_changes"].append(
                {"key": key, "from": pa.get(key), "to": pb.get(key)})
    ma, mb = a.get("field_mapping", {}), b.get("field_mapping", {})
    for f in sorted(set(mb) - set(ma)):
        out["mapping_added"].append({"raw": f, **mb[f]})
    for f in sorted(set(ma) - set(mb)):
        out["mapping_removed"].append({"raw": f, **ma[f]})
    for f in sorted(set(ma) & set(mb)):
        if ma[f] != mb[f]:
            out["mapping_changed"].append({"raw": f, "from": ma[f], "to": mb[f]})
    ra = {repr(r) for r in a.get("validation", [])}
    rb = {repr(r) for r in b.get("validation", [])}
    out["validation_added"] = sorted(rb - ra)
    out["validation_removed"] = sorted(ra - rb)
    return out
