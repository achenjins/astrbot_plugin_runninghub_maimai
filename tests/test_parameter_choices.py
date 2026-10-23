"""Choice shorthand and numbered command input preserve administrator limits."""
from __future__ import annotations

import pytest

from rh_generic_lib.config import InputNodeSection
from rh_generic_lib.validation import (
    command_parameter_value,
    format_parameter_choices,
    parameter_value,
)


def ratio_node(**overrides):
    return InputNodeSection(**{
        "node_id": "424", "field_name": "aspect_ratio", "value_type": "text",
        "label": "长宽比例", "choices": ["9:16 (Portrait)", "16:9 (Widescreen)", "1:1 (Square)"],
        **overrides,
    })


@pytest.mark.parametrize("value", ["16:9", "16：9", "16:9 (WIDESCREEN)", " 16:9 "])
def test_choice_shorthand_resolves_to_configured_platform_value(value):
    assert parameter_value(ratio_node(), value) == "16:9 (Widescreen)"


def test_choice_shorthand_rejects_ambiguous_label_and_unknown_value():
    node = ratio_node(choices=["16:9 (A)", "16:9 (B)"])
    with pytest.raises(ValueError, match="可选"):
        parameter_value(node, "16：9")
    with pytest.raises(ValueError, match="可选"):
        parameter_value(node, "2:1")
    assert parameter_value(node, "16:9 (A)") == "16:9 (A)"


def test_command_choice_numbers_are_only_used_by_explicit_command_helper():
    node = ratio_node()
    assert command_parameter_value(node, "2") == "16:9 (Widescreen)"
    with pytest.raises(ValueError, match="可选"):
        parameter_value(node, "2")
    for value in ("0", "4"):
        with pytest.raises(ValueError, match="序号"):
            command_parameter_value(node, value)
    assert format_parameter_choices(node) == ["1) 9:16 (Portrait)", "2) 16:9 (Widescreen)", "3) 1:1 (Square)"]


def test_numeric_choice_value_takes_precedence_over_numbered_position():
    node = ratio_node(param_type="integer", choices=["2", "1"])
    assert command_parameter_value(node, "1") == "1"
    assert parameter_value(node, 2) == "2"


def test_large_integer_and_decimal_bounds_keep_exact_validation():
    seed = ratio_node(param_type="integer", choices=["9007199254740993"])
    assert parameter_value(seed, "9007199254740993") == "9007199254740993"
    assert command_parameter_value(seed, "1") == "9007199254740993"
    bounded = ratio_node(param_type="number", minimum=0.1, maximum=0.3, choices=[])
    assert parameter_value(bounded, "0.300000000000000000") == "0.300000000000000000"
    with pytest.raises(ValueError, match="不能大于"):
        parameter_value(bounded, "0.300000000000000001")
    with pytest.raises(ValueError, match="有限数字"):
        parameter_value(bounded, "NaN")


def test_choice_command_cannot_edit_a_fixed_node():
    node = ratio_node(value_type="default", field_value="fixed")
    with pytest.raises(ValueError, match="不是可编辑配置"):
        command_parameter_value(node, "1")
    assert format_parameter_choices(node) == []
