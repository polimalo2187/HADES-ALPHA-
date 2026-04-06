import ast
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_functions(relative_path: str, names: list[str], extra_globals: dict | None = None):
    path = ROOT / relative_path
    source = path.read_text()
    tree = ast.parse(source, filename=str(path))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    module = ast.Module(body=selected, type_ignores=[])
    namespace = {}
    if extra_globals:
        namespace.update(extra_globals)
    exec(compile(module, str(path), 'exec'), namespace)
    return [namespace[name] for name in names]


def test_small_price_precision_is_not_flattened():
    _price_round_digits, _round_price_dynamic, calculate_entry_zone = _load_functions(
        'app/signals.py',
        ['_price_round_digits', '_round_price_dynamic', 'calculate_entry_zone'],
    )
    entry = 0.003512347891
    stop = _round_price_dynamic(0.003498765432)
    tp1 = _round_price_dynamic(0.003589654321)
    tp2 = _round_price_dynamic(0.003642198765)

    assert _price_round_digits(entry) >= 8
    assert len({ _round_price_dynamic(entry), stop, tp1, tp2 }) == 4

    low, high = calculate_entry_zone(entry)
    assert low != high
    assert abs(high - low) > 0


def test_text_formatters_show_more_than_four_decimals_for_small_prices():
    analysis_price_digits, analysis_fmt_price = _load_functions(
        'app/analysis_ui.py',
        ['_price_digits', '_fmt_price'],
    )
    tracking_price_digits, tracking_fmt_price = _load_functions(
        'app/tracking_ui.py',
        ['_price_digits', '_fmt_price'],
    )
    watchlist_price_digits, watchlist_fmt_price = _load_functions(
        'app/watchlist_ui.py',
        ['_price_digits', '_fmt_price'],
    )
    value = 0.003512347891

    assert analysis_price_digits(value) >= 8
    assert tracking_price_digits(value) >= 8
    assert watchlist_price_digits(value) >= 8
    assert analysis_fmt_price(value).startswith('0.003512')
    assert tracking_fmt_price(value).startswith('0.003512')
    assert watchlist_fmt_price(value).startswith('0.003512')


def test_strategy_normalize_price_preserves_small_price_precision():
    _price_digits, _normalize_price = _load_functions(
        'app/strategy.py',
        ['_price_digits', '_normalize_price'],
        extra_globals={'Decimal': Decimal, 'ROUND_HALF_UP': ROUND_HALF_UP},
    )
    value = 0.003512347891
    normalized = _normalize_price(value)

    assert _price_digits(value) >= 8
    assert normalized != round(value, 4)
    assert str(normalized).startswith('0.003512')
