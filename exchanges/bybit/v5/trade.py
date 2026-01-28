import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bybit as bybit

logger = logging.getLogger(__name__)

def place_order(api, data, *, headers={}, **kwargs):
    """
    Place order for Spot, Margin trading, USDT/USDC perpetual, USDT/USDC futures, Inverse Futures and Options.

    Link: 
        https://bybit-exchange.github.io/docs/v5/order/create-order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/order/create"
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    try:
        rate_limiter.acquire('bybit.v5.trade.place_order')  
        bybit.sign_headers(headers, api, recv_window, payload)
        response = http.post(url, data=payload, headers=headers, timeout=timeout)
        body = response.json()
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)
        if kwargs.get('full'): return response, body
        return body
    except Exception as e: logger.error('Failed to place order on Bybit: %s', e); raise

