import logging
import os
from decimal import ROUND_HALF_UP, Decimal

import requests

from common.constants import SUPPORTED_CURRENCIES

logger = logging.getLogger(__name__)

OPENEXCHANGE_LATEST_URL = "https://openexchangerates.org/api/latest.json"


def get_openexchange_app_id() -> str:
    app_id = os.environ.get("OPENEXCHANGE_APP_ID", "").strip()

    if not app_id:
        raise ValueError(
            "OPENEXCHANGE_APP_ID is not set. Add it to .env and restart Airflow containers."
        )

    return app_id


def fetch_latest_rates(required_currencies=None) -> dict:
    app_id = get_openexchange_app_id()

    requested_currencies = set(required_currencies or SUPPORTED_CURRENCIES)
    requested_currencies.add("EUR")

    params = {
        "app_id": app_id,
        "symbols": ",".join(sorted(requested_currencies)),
    }

    logger.info(
        "Fetching latest exchange rates from OpenExchangeRates: currencies=%s",
        params["symbols"],
    )

    response = requests.get(
        OPENEXCHANGE_LATEST_URL,
        params=params,
        timeout=30,
    )

    if response.status_code != 200:
        raise RuntimeError(
            "OpenExchangeRates request failed: "
            f"status_code={response.status_code}, response_body={response.text[:1000]}"
        )

    payload = response.json()

    base = payload.get("base")
    rates = payload.get("rates")

    if not base:
        raise ValueError(f"OpenExchangeRates response does not contain base: {payload}")

    if not isinstance(rates, dict):
        raise ValueError(f"OpenExchangeRates response does not contain rates: {payload}")

    missing_currencies = sorted(requested_currencies - set(rates.keys()))

    if missing_currencies:
        raise ValueError(
            "OpenExchangeRates response is missing required currencies: "
            f"{missing_currencies}"
        )

    logger.info(
        "Fetched exchange rates successfully: base=%s rates_count=%s eur_rate=%s",
        base,
        len(rates),
        rates["EUR"],
    )

    return {
        "base": base,
        "timestamp": payload.get("timestamp"),
        "rates": rates,
    }


def convert_amount_to_eur(amount, currency: str, rates: dict) -> tuple[Decimal, Decimal]:
    if currency not in rates:
        raise ValueError(f"Missing exchange rate for currency={currency}")

    if "EUR" not in rates:
        raise ValueError("Missing EUR exchange rate.")

    amount_decimal = Decimal(str(amount))
    source_rate = Decimal(str(rates[currency]))
    eur_rate = Decimal(str(rates["EUR"]))

    if source_rate <= 0:
        raise ValueError(f"Invalid exchange rate for currency={currency}: {source_rate}")

    amount_eur = amount_decimal / source_rate * eur_rate
    amount_eur = amount_eur.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    exchange_rate_to_eur = eur_rate / source_rate
    exchange_rate_to_eur = exchange_rate_to_eur.quantize(
        Decimal("0.00000001"),
        rounding=ROUND_HALF_UP,
    )

    return amount_eur, exchange_rate_to_eur
