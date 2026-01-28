import logging
import json
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.okx as okx

logger = logging.getLogger(__name__)

def place_algo_order(api, order, *, headers={}, **kwargs):
    """ 
    The algo order includes trigger order, oco order, chase order, conditional order, twap order and trailing order.

    Link: 
        https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-place-algo-order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        order (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): HTTP headers.
        kwargs (dict):
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
    method = 'POST'
    endpoint = '/api/v5/trade/order-algo'
    url = base_url + endpoint
    payload = json.dumps(order, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(): 
        okx.sign_headers(headers, api, method, endpoint, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('okx.api.order_book_trading.algo_trading_rest.place_algo_order') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to place algo order on OKX: %s', e); raise
