from common.demand_utils import build_label, probability_to_label, sigmoid


def test_build_label_high_low():
    assert build_label(11, 10) == 1
    assert build_label(10, 10) == 0
    assert build_label(2, 10) == 0


def test_sigmoid_basic():
    assert round(sigmoid(0), 4) == 0.5
    assert sigmoid(10) > 0.99
    assert sigmoid(-10) < 0.01


def test_probability_to_label():
    assert probability_to_label(0.7) == "HIGH"
    assert probability_to_label(0.49) == "LOW"
