import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bitget as bitget

logger = logging.getLogger(__name__)

def place_order(api, data, *, headers={}, **kwargs):
    """ 
    Place an order.

    Link: 
        https://www.bitget.com/api-doc/contract/trade/Place-Order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): HTTP headers.
        kwargs (dict):
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    base_url = kwargs.get('base_url', bitget.MAIN_DOMAIN)
    timeout = kwargs.get('timeout', bitget.TIMEOUT)
    method = 'POST'
    path = '/api/v2/mix/position/single-position'
    url = base_url + path
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(): 
        bitget.sign_headers(headers, api, method, path, payload)
        return http.post(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bitget.futures.trade.place_order')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to place order on Bitget: %s', e); raise

