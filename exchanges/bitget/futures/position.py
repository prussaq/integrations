import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bitget as bitget

logger = logging.getLogger(__name__)


def get_single_position(api, symbol, product_type, margin_coin, *, headers={}, **kwargs):
    """ 
    Returns position information of a single symbol, response including estimated liquidation price.

    Link: 
        https://www.bitget.com/api-doc/contract/position/get-single-position
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        symbol (str): Trading pair, e.g. BTCUSDT
        product_type (str): Product type: USDT-FUTURES, COIN-FUTURES, USDC-FUTURES
        margin_coin (str): Margin coin, capitalized, e.g. USDT
        headers (dict): HTTP headers.
        kwargs:
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
    method = 'GET'
    params = {"symbol": symbol, "productType": product_type, "marginCoin": margin_coin}
    query = urlencode(sorted(params.items(), key=lambda d: d[0]))
    path = '/api/v2/mix/position/single-position' + (f"?{query}" if query else '')
    url = base_url + path

    def send(): 
        bitget.sign_headers(headers, api, method, path)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('bitget.futures.position.get_single_position')  
    return execute_request(send, read, check, kwargs)

