from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup


VIETCOMBANK_API_URL = (
    "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx?b=68"
)
VIETCOMBANK_PAGE_URL = (
    "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/exrate.aspx"
)


class ExchangeRateError(RuntimeError):
    pass


def fetch_vietcombank_api(
    currency: str = "USD",
    timeout: float = 15,
) -> tuple[Decimal, str, str]:
    response = requests.get(VIETCOMBANK_API_URL, timeout=timeout)
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)
    date_time = root.findtext("DateTime", default="")

    for item in root.findall("Exrate"):
        if item.attrib.get("CurrencyCode", "").upper() == currency.upper():
            raw_transfer = item.attrib.get("Transfer")
            raw_sell = item.attrib.get("Sell")
            raw_rate = raw_transfer if raw_transfer not in (None, "-") else raw_sell
            if not raw_rate:
                break
            try:
                rate = Decimal(raw_rate.replace(",", "").strip())
            except InvalidOperation as error:
                raise ExchangeRateError(f"Invalid API exchange rate: {raw_rate}") from error
            return rate, "API", date_time

    raise ExchangeRateError(f"{currency} was not found in Vietcombank API response")


def fetch_vietcombank_scraping(
    currency: str = "USD",
    timeout: float = 15,
) -> tuple[Decimal, str, str]:
    response = requests.get(VIETCOMBANK_PAGE_URL, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for row in soup.select("table tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
        if cells and currency.upper() in " ".join(cells).upper():
            numeric_values = []
            for cell in cells:
                normalized = cell.replace(",", "").strip()
                if normalized.replace(".", "", 1).isdigit():
                    numeric_values.append(Decimal(normalized))
            if numeric_values:
                rate = numeric_values[-2] if len(numeric_values) >= 2 else numeric_values[-1]
                return rate, "WEB_SCRAPING", datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    raise ExchangeRateError(f"{currency} was not found in Vietcombank web page")


class VietcombankExchangeRate:
    def __init__(self, cache_minutes: int = 1440) -> None:
        self.cache_minutes = cache_minutes
        self._rate: Decimal | None = None
        self._source = ""
        self._published_at = ""
        self._expires_at: datetime | None = None

    def get_usd_vnd(self, force_refresh: bool = False) -> tuple[Decimal, str, str]:
        if (
            not force_refresh
            and self._rate is not None
            and self._expires_at is not None
            and datetime.now() < self._expires_at
        ):
            return self._rate, self._source, self._published_at

        api_error: Exception | None = None
        try:
            rate, source, published_at = fetch_vietcombank_api("USD")
        except Exception as error:
            api_error = error
            try:
                rate, source, published_at = fetch_vietcombank_scraping("USD")
            except Exception as scraping_error:
                raise ExchangeRateError(
                    f"Cannot get Vietcombank USD/VND rate. "
                    f"API error: {api_error}; scraping error: {scraping_error}"
                ) from scraping_error

        self._rate = rate
        self._source = source
        self._published_at = published_at
        self._expires_at = datetime.now() + timedelta(minutes=self.cache_minutes)
        return rate, source, published_at


def convert_usd_to_vnd(amount: str, rate: Decimal) -> Decimal:
    cleaned = amount.replace("$", "").replace(",", "").strip()
    try:
        usd = Decimal(cleaned)
    except InvalidOperation as error:
        raise ValueError(f"Invalid USD amount: {amount}") from error
    return (usd * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
