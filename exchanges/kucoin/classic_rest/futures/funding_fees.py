import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.kucoin as kucoin

logger = logging.getLogger(__name__)


def get_current_funding_rate(symbol, *, headers={}, **kwargs):
    """ 
    Get current funding rate for the contract.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-current-funding-rate
    Args:
        symbol (str): Symbol of the contract.
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v1/funding-rate/{symbol}/current"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.futures.funding_fees.get_current_funding_rate') 
    return execute_request(send, read, check, kwargs)


def get_public_funding_history(symbol, from_, to, *, headers={}, **kwargs):
    """ 
    Get public funding history for the contract.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history
    Args:
        symbol (str): Symbol of the contract.
        from_ (int): Begin time (milliseconds).
        to (int): End time (milliseconds).
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v1/contract/funding-rates?symbol={symbol}&from={from_}&to={to}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.futures.funding_fees.get_public_funding_history')
    return execute_request(send, read, check, kwargs)


def get_private_funding_history(api, symbol, params={}, *, headers={}, **kwargs):
    """ 
    Get private funding history for the contract. Maximum for 3 months.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-private-funding-history
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        symbol (str): Symbol of the contract.
        params (dict):
            startAt (int): Begin time (milliseconds).
            endAt (int): End time (milliseconds).
            reverse (bool): 
            offset (int): Start offset.
            forward (bool): 
            maxCount (int): Maximum records. Default: 10
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    method = 'GET'
    params['symbol'] = symbol
    endpoint = f"/api/v1/funding-history?{urlencode(params)}"
    url = base_url + endpoint

    def send(): 
        kucoin.sign_headers(headers, api, method, endpoint)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.futures.funding_fees.get_private_funding_history') 
    return execute_request(send, read, check, kwargs)
