from benchmarks.scenario_runner import (
    DeterminismError, ScenarioBenchmarkRunner, canonicalize, result_digest,
)


def test_canonical_digest_is_stable_for_mapping_order():
    assert result_digest({"b": 2, "a": 1}) == result_digest({"a": 1, "b": 2})


def test_dual_run_matrix_produces_identical_sha256_digests(
    scenario_matrix, deterministic_executor
):
    report = ScenarioBenchmarkRunner(deterministic_executor).run_matrix(scenario_matrix)
    assert report["scenario_count"] == 48
    assert report["repeat_count"] == 2
    assert report["determinism"] == "PASS"
    assert all(row["run_1_digest"] == row["run_2_digest"] for row in report["scenarios"])
    assert len(report["report_digest"]) == 64


def test_non_deterministic_executor_is_detected(scenario_matrix):
    calls = {}
    def unstable(spec, run_index):
        calls[spec.scenario_id] = calls.get(spec.scenario_id, 0) + 1
        return {"run": calls[spec.scenario_id]}
    runner = ScenarioBenchmarkRunner(unstable)
    import pytest
    with pytest.raises(DeterminismError):
        runner.run_one(scenario_matrix[0])


def test_canonicalizer_rejects_non_finite_float():
    import pytest
    with pytest.raises(ValueError, match="non-finite"):
        canonicalize(float("nan"))
