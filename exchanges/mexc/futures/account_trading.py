import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.mexc as mexc

logger = logging.getLogger(__name__)


def get_account_assets(api, *, headers={}, **kwargs):
    """ 
    Get all account assets.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints#get-all-account-assets
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        headers (dict): HTTP headers, e.g. Recv-Window
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
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    method = 'GET'
    url = f"{base_url}/api/v1/private/account/assets"

    def send(): 
        mexc.sign_headers(headers, api, method)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.account_trading.get_account_assets') 
    return execute_request(send, read, check, kwargs)


def get_currency_asset(api, currency, *, headers={}, **kwargs):
    """ 
    Get single currency asset info.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints#get-single-currency-asset-information
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        currency (str): Currency name.
        headers (dict): HTTP headers, e.g. Recv-Window
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
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    method = 'GET'
    url = f"{base_url}/api/v1/private/account/asset/{currency}"

    def send(): 
        mexc.sign_headers(headers, api, method)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.account_trading.get_currency_asset') 
    return execute_request(send, read, check, kwargs)


def get_open_positions(api, params={}, *, headers={}, **kwargs):
    """ 
    Get info about open positions.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints#get-open-positions
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        params (dict):
            symbol (str): Symbol of the contract.
            positionId (long): Position ID.
        headers (dict): HTTP headers, e.g. Recv-Window
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
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    method = 'GET'
    query = urlencode({k: v for k, v in sorted(params.items()) if v is not None})
    url = f"{base_url}/api/v1/private/position/open_positions" + (f"?{query}" if query else '')

    def send(): 
        mexc.sign_headers(headers, api, method, query=query)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.account_trading.get_open_positions') 
    return execute_request(send, read, check, kwargs)
