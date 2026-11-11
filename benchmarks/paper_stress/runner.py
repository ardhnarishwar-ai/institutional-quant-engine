"""Deterministic stress runner and canonical SHA-256 evidence."""
from __future__ import annotations
import hashlib
import json
from decimal import Decimal
from .workload import PROFILES, make_stack, run_profile_workload, make_synthetic_paper_fill


def canonical_digest(value) -> str:
    def normalize(obj):
        if isinstance(obj, Decimal):
            return format(obj, "f")
        if isinstance(obj, dict):
            return {str(k): normalize(obj[k]) for k in sorted(obj, key=str)}
        if isinstance(obj, (tuple, list)):
            return [normalize(x) for x in obj]
        if hasattr(obj, "value"):
            return obj.value
        if hasattr(obj, "__dataclass_fields__"):
            return {k: normalize(getattr(obj, k)) for k in obj.__dataclass_fields__}
        return obj
    payload = json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def dual_run(profile):
    first = run_profile_workload(profile)
    second = run_profile_workload(profile)
    digest_a, digest_b = canonical_digest(first), canonical_digest(second)
    if digest_a != digest_b:
        raise AssertionError(f"determinism mismatch for {profile.key}: {digest_a} != {digest_b}")
    return {"instrument": profile.key, "currency": profile.currency,
            "run_1_sha256": digest_a, "run_2_sha256": digest_b, "matched": True,
            "observation": first}


def build_report():
    rows = [dual_run(profile) for profile in PROFILES]
    report = {
        "stage": "5C.4.3",
        "execution_mode": "DISABLED",
        "instrument_count": len(PROFILES),
        "scenario_groups": ["order_pressure", "fill_pressure", "ledger_pressure",
                            "replay_stability", "instrument_isolation"],
        "dual_run_count": 2,
        "determinism": "PASS" if all(row["matched"] for row in rows) else "FAIL",
        "instruments": rows,
    }
    report["report_sha256"] = canonical_digest(report)
    return report
