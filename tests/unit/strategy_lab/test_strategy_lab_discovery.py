from __future__ import annotations

from dataclasses import replace

import pytest

from gribuki_trade.strategy_lab.discovery import (
    CandidateRejectionReason,
    FactorEconomicFamily,
    FactorSearchBudget,
    FactorSearchBudgetExceeded,
    FactorTemplate,
    FactorTemplateGrammar,
    RedundancyFilterConfig,
    RedundancyRejectionReason,
    TemplateParameter,
    TemplateParameterKind,
    default_factor_template_grammar,
    filter_redundant_candidates,
    generate_factor_candidates,
)


def _single_window_grammar(*templates: FactorTemplate) -> FactorTemplateGrammar:
    return FactorTemplateGrammar(
        grammar_version="test-grammar@1",
        window_whitelist=(5,),
        column_whitelist=("close", "volume"),
        templates=templates,
    )


def test_default_generation_is_deterministic_versioned_and_fully_counted() -> None:
    grammar = default_factor_template_grammar()

    first = generate_factor_candidates(grammar, FactorSearchBudget(max_trials=40))
    second = generate_factor_candidates(grammar, FactorSearchBudget(max_trials=40))

    assert first == second
    assert first.inventory_id == second.inventory_id
    assert first.grammar_sha256 == grammar.grammar_sha256
    assert first.grammar_version == "technical-factor-grammar@1"
    assert first.trial_count == 33
    assert first.trial_count == first.unique_hypothesis_count + first.rejected_count
    assert first.unique_hypothesis_count == len(first.candidates)
    assert "MULTIPLE_HYPOTHESIS_TESTING_REQUIRED" in first.warnings
    assert first.research_only is True
    assert len({item.candidate_id for item in first.candidates}) == len(first.candidates)
    assert {item.economic_family for item in first.candidates} == set(
        FactorEconomicFamily
    )
    assert all(item.grammar_sha256 == grammar.grammar_sha256 for item in first.candidates)


def test_budget_overrun_fails_before_any_silent_truncation() -> None:
    grammar = default_factor_template_grammar()

    with pytest.raises(FactorSearchBudgetExceeded) as captured:
        generate_factor_candidates(grammar, FactorSearchBudget(max_trials=32))

    assert captured.value.required_trials == 33
    assert captured.value.max_trials == 32


def test_cartesian_overrun_stops_after_first_proven_budget_excess() -> None:
    windows = tuple(range(1, 101))
    grammar = FactorTemplateGrammar(
        grammar_version="large-grid@1",
        window_whitelist=windows,
        column_whitelist=("close",),
        templates=(
            FactorTemplate(
                "large-grid",
                FactorEconomicFamily.TREND,
                "ma(close, {left}) / ma(close, {right})",
                (
                    TemplateParameter("left", TemplateParameterKind.WINDOW),
                    TemplateParameter("right", TemplateParameterKind.WINDOW),
                ),
            ),
        ),
    )

    with pytest.raises(FactorSearchBudgetExceeded) as captured:
        generate_factor_candidates(grammar, FactorSearchBudget(max_trials=5))

    assert captured.value.required_trials == 6
    assert "at least 6" in str(captured.value)


def test_search_budget_has_an_absolute_engineering_cap() -> None:
    with pytest.raises(ValueError, match="must not exceed"):
        FactorSearchBudget(max_trials=100_001)


def test_canonical_duplicates_and_dsl_rejections_keep_every_reason() -> None:
    window = TemplateParameter(
        "window",
        TemplateParameterKind.WINDOW,
        (5,),
    )
    grammar = _single_window_grammar(
        FactorTemplate(
            "first",
            FactorEconomicFamily.MOMENTUM,
            "return(close, {window})",
            (window,),
        ),
        FactorTemplate(
            "invalid",
            FactorEconomicFamily.MOMENTUM,
            "close ** {window}",
            (window,),
        ),
        FactorTemplate(
            "same-canonical",
            FactorEconomicFamily.MOMENTUM,
            "ret(close,{window})",
            (window,),
        ),
    )

    result = generate_factor_candidates(grammar, FactorSearchBudget(max_trials=3))

    assert result.trial_count == 3
    assert result.unique_hypothesis_count == 1
    assert result.rejected_count == 2
    assert result.duplicate_count == 1
    by_template = {item.template_id: item for item in result.rejected}
    assert by_template["invalid"].reason_codes == (
        CandidateRejectionReason.EXPRESSION_REJECTED_BY_DSL,
    )
    duplicate = by_template["same-canonical"]
    assert duplicate.reason_codes == (
        CandidateRejectionReason.DUPLICATE_CANONICAL_EXPRESSION,
    )
    assert duplicate.duplicate_of_candidate_id == result.candidates[0].candidate_id
    assert all(item.attempt_id.startswith("factor-attempt-") for item in result.rejected)


def test_static_window_outside_whitelist_is_rejected_not_executed() -> None:
    grammar = _single_window_grammar(
        FactorTemplate(
            "outside-window",
            FactorEconomicFamily.MOMENTUM,
            "return(close, 3)",
        )
    )

    result = generate_factor_candidates(grammar, FactorSearchBudget(max_trials=1))

    assert result.candidates == ()
    assert result.rejected[0].reason_codes == (
        CandidateRejectionReason.WINDOW_OUTSIDE_WHITELIST,
    )


def test_template_window_domains_must_be_explicitly_whitelisted() -> None:
    with pytest.raises(ValueError, match="window_whitelist"):
        _single_window_grammar(
            FactorTemplate(
                "bad-window-domain",
                FactorEconomicFamily.MOMENTUM,
                "return(close, {window})",
                (
                    TemplateParameter(
                        "window",
                        TemplateParameterKind.WINDOW,
                        (3,),
                    ),
                ),
            )
        )


def test_ast_complexity_and_depth_are_enforced_by_the_grammar() -> None:
    grammar = FactorTemplateGrammar(
        grammar_version="tiny-complexity@1",
        window_whitelist=(5,),
        column_whitelist=("close",),
        templates=(
            FactorTemplate(
                "too-complex",
                FactorEconomicFamily.MOMENTUM,
                "close + close + close + close",
            ),
        ),
        max_ast_nodes=5,
        max_ast_depth=3,
    )

    result = generate_factor_candidates(grammar, FactorSearchBudget(max_trials=1))

    assert result.unique_hypothesis_count == 0
    assert result.rejected[0].reason_codes == (
        CandidateRejectionReason.AST_NODE_LIMIT_EXCEEDED,
    )


def test_ordered_window_constraint_is_applied_before_trial_budget() -> None:
    short = TemplateParameter(
        "short",
        TemplateParameterKind.WINDOW,
        (5, 10),
    )
    long = TemplateParameter(
        "long",
        TemplateParameterKind.WINDOW,
        (5, 10),
    )
    grammar = FactorTemplateGrammar(
        grammar_version="ordered@1",
        window_whitelist=(5, 10),
        column_whitelist=("close",),
        templates=(
            FactorTemplate(
                "ordered",
                FactorEconomicFamily.TREND,
                "ma(close, {short}) / ma(close, {long})",
                (short, long),
                (("short", "long"),),
            ),
        ),
    )

    result = generate_factor_candidates(grammar, FactorSearchBudget(max_trials=1))

    assert result.trial_count == 1
    assert result.candidates[0].parameters == (("long", "10"), ("short", "5"))


def test_redundancy_filter_is_order_stable_and_retains_exact_reason() -> None:
    inventory = generate_factor_candidates(
        default_factor_template_grammar(),
        FactorSearchBudget(max_trials=40),
    )
    first, second, third = inventory.candidates[:3]
    values = tuple(float(index) for index in range(30))
    observations = {
        first.candidate_id: values,
        second.candidate_id: tuple(value * 2.0 for value in values),
        third.candidate_id: tuple(
            float((index * 7) % 11) for index in range(30)
        ),
    }

    result = filter_redundant_candidates(
        (first, second, third),
        observations,
        RedundancyFilterConfig(
            maximum_absolute_correlation=0.99,
            minimum_overlap=20,
        ),
    )

    assert result.retained == (first, third)
    assert result.rejected_count == 1
    rejection = result.rejected[0]
    assert rejection.candidate_id == second.candidate_id
    assert rejection.compared_with_candidate_id == first.candidate_id
    assert rejection.reason_codes == (
        RedundancyRejectionReason.REDUNDANT_CORRELATION,
    )
    assert rejection.correlation == pytest.approx(1.0)


def test_redundancy_filter_fail_closed_reasons_are_all_auditable() -> None:
    inventory = generate_factor_candidates(
        default_factor_template_grammar(),
        FactorSearchBudget(max_trials=40),
    )
    first, second, third, fourth = inventory.candidates[:4]
    candidates = (
        replace(first, candidate_id="missing"),
        replace(second, candidate_id="short"),
        replace(third, candidate_id="constant"),
        replace(fourth, candidate_id="valid"),
    )
    observations = {
        "short": tuple(float(index) for index in range(5)),
        "constant": (1.0,) * 30,
        "valid": tuple(float(index) for index in range(30)),
    }

    result = filter_redundant_candidates(
        candidates,
        observations,
        RedundancyFilterConfig(minimum_overlap=20),
    )

    reasons = {
        item.candidate_id: item.reason_codes[0]
        for item in result.rejected
    }
    assert reasons == {
        "missing": RedundancyRejectionReason.MISSING_SERIES,
        "short": RedundancyRejectionReason.SERIES_LENGTH_MISMATCH,
        "constant": RedundancyRejectionReason.ZERO_VARIANCE,
    }
    assert result.retained == (candidates[-1],)


def test_redundancy_filter_can_limit_comparisons_to_same_economic_family() -> None:
    inventory = generate_factor_candidates(
        default_factor_template_grammar(),
        FactorSearchBudget(max_trials=40),
    )
    first = inventory.candidates[0]
    different_family = next(
        item
        for item in inventory.candidates
        if item.economic_family is not first.economic_family
    )
    values = tuple(float(index) for index in range(30))

    result = filter_redundant_candidates(
        (first, different_family),
        {
            first.candidate_id: values,
            different_family.candidate_id: values,
        },
        RedundancyFilterConfig(within_family_only=True),
    )

    assert result.retained == (first, different_family)
    assert result.rejected == ()
