import logging
import json
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.okx as okx

logger = logging.getLogger(__name__)


def get_ticker(inst_id, *, headers={}, **kwargs):
    """ 
    Retrieve the latest price snapshot, best bid/ask price, and trading volume in the last 24 hours. 
    Best ask price may be lower than the best bid price during the pre-open period.

    Link: 
        https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-ticker
    Args:
        inst_id (dict): Instrument ID, e.g. BTC-USD-SWAP
        headers (dict): HTTP headers.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body.
    Raises:
        RequestFailed: If the request fails due to a transport- or protocol-level failure.
        ApiError: If the response is semantically invalid or indicates an API-level error.
        Exception: Propagates any other unexpected exceptions.
    Notes: 
        Makes HTTP request by `requests` or `requests.Session` if provided.
    """
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    url = f"{base_url}/api/v5/market/ticker?instId={inst_id}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.order_book_trading.market_data.get_ticker') 
    return execute_request(send, read, check, kwargs)
