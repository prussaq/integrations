import logging
import json
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.okx as okx

logger = logging.getLogger(__name__)


def place_algo_order(api, order, **kwargs):
    """ 
    The algo order includes trigger order, oco order, chase order, conditional order, twap order and trailing order.

    Link: 
        https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-place-algo-order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        order (dict): Request body parameters (JSON). See the documentation at `Link`.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            full (bool): If True, return both the parsed response body and the HTTP response object.
            Additional `requests` params like timeout, headers, etc.
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
    headers = kwargs.pop('headers', {})
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', okx.BASE_URL)
    timeout = kwargs.pop('timeout', okx.TIMEOUT)
    method = 'POST'
    endpoint = '/api/v5/trade/order-algo'
    url = base_url + endpoint
    payload = json.dumps(order, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(settings): 
        okx.sign_headers(headers, api, method, endpoint, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.order_book_trading.algo_trading_rest.place_algo_order') 
    return execute_request(send, read, check, kwargs)
