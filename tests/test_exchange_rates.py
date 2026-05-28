from decimal import Decimal

import pytest
from common.exchange_rates import convert_amount_to_eur


def test_convert_eur_to_eur_returns_same_amount():
    rates = {"EUR": 0.92, "USD": 1.0}

    amount_eur, conversion_rate = convert_amount_to_eur(
        amount=Decimal("100"),
        currency="EUR",
        rates=rates,
    )

    assert amount_eur == Decimal("100.00")
    assert conversion_rate == Decimal("1.00000000")


def test_convert_usd_to_eur_happy_path():
    rates = {"USD": 1.0, "EUR": 0.92}

    amount_eur, conversion_rate = convert_amount_to_eur(
        amount=Decimal("100"),
        currency="USD",
        rates=rates,
    )

    assert amount_eur == Decimal("92.00")
    assert conversion_rate == Decimal("0.92000000")


def test_unknown_currency_raises_value_error():
    rates = {"USD": 1.0, "EUR": 0.92}

    with pytest.raises(ValueError):
        convert_amount_to_eur(
            amount=Decimal("100"),
            currency="GBP",
            rates=rates,
        )


def test_zero_source_rate_raises_value_error():
    rates = {"USD": 0, "EUR": 0.92}

    with pytest.raises(ValueError):
        convert_amount_to_eur(
            amount=Decimal("100"),
            currency="USD",
            rates=rates,
        )


def test_convert_amount_to_eur_uses_round_half_up():
    rates = {"USD": 1.0, "EUR": 1.0}

    amount_eur, _ = convert_amount_to_eur(
        amount=Decimal("0.005"),
        currency="USD",
        rates=rates,
    )

    assert amount_eur == Decimal("0.01")
