from benchmarks.orchestrator_executor import execute_scenario, run_end_to_end_matrix
from benchmarks.scenarios.catalog import build_scenario_matrix

def test_all_48_real_orchestrator_scenarios_are_dual_run_deterministic():
    report=run_end_to_end_matrix()
    assert report["stage"]=="5C.3" and report["scenario_count"]==48
    assert report["run_count"]==2 and report["all_deterministic"]
    assert len(report["report_sha256"])==64
    assert all(x["run_1_sha256"]==x["run_2_sha256"] for x in report["scenarios"])

def test_marketable_and_partial_fills_reach_real_ledger():
    specs=[x for x in build_scenario_matrix() if x.template_key in ("marketable_limit","partial_depth")]
    assert len(specs)==8
    for spec in specs:
        result=execute_scenario(spec,1)
        assert result["fills"] and result["ledger_entries"]
        assert result["currency"]==spec.instrument.currency
        assert all(x["execution_source"]=="PAPER_SIMULATOR" for x in result["ledger_entries"])

def test_non_marketable_limit_does_not_post_a_fill():
    for spec in (x for x in build_scenario_matrix() if x.template_key=="non_marketable_limit"):
        result=execute_scenario(spec,1)
        assert result["orders"] and result["fills"]==[] and result["ledger_entries"]==[]

def test_currency_and_execution_invariants():
    report=run_end_to_end_matrix()
    assert report["currency_by_instrument"]=={
        "NIFTY":"INR","BANKNIFTY":"INR","FINNIFTY":"INR","BTCUSDT":"USDT"}
    assert report["execution_mode"]=="DISABLED"
    assert all(x["observation"]["execution_mode"]=="DISABLED" for x in report["scenarios"])
