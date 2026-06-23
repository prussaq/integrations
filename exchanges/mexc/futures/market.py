import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.mexc as mexc

logger = logging.getLogger(__name__)


def get_contract_info(params={}, **kwargs):
    """ 
    Get information about all contracts or specified one.

    Link: 
        https://www.mexc.com/api-docs/futures/market-endpoints#get-contract-info
    Args:
        params (dict):
            symbol (str): Symbol of the contract.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    headers = {}
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    url = f"{base_url}/api/v1/contract/detail"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.market.get_contract_info') 
    return execute_request(send, read, check, kwargs)


def get_index_price(symbol, **kwargs):
    """ 
    Get index price of the contract.

    Link: 
        https://www.mexc.com/api-docs/futures/market-endpoints#get-index-price
    Args:
        symbol (str): Symbol of the contract.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    headers = {}
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    url = f"{base_url}/api/v1/contract/index_price/{symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.market.get_index_price') 
    return execute_request(send, read, check, kwargs)


def get_funding_rate(symbol, **kwargs):
    """ 
    Get funding rate of the contract.

    Link: 
        https://www.mexc.com/api-docs/futures/market-endpoints#get-funding-rate
    Args:
        symbol (str): Symbol of the contract.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    headers = {}
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    url = f"{base_url}/api/v1/contract/funding_rate/{symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.market.get_funding_rate') 
    return execute_request(send, read, check, kwargs)


def get_candlestick_data(symbol, params={}, **kwargs):
    """ 
    Get candlestick data of the contract. Max 2000 entries. Default interval Min1.

    Link: 
        https://www.mexc.com/api-docs/futures/market-endpoints#get-candlestick-data
    Args:
        symbol (str): Symbol of the contract.
        params (dict):
            interval (str): Min1, Min5, Min15, Min30, Min60, Hour4, Hour8, Day1, Week1, Month1
            start (int): Start timestamp (seconds).
            end (int): End timestamp (seconds).
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    headers = {}
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    url = f"{base_url}/api/v1/contract/kline/{symbol}"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.market.get_candlestick_data') 
    return execute_request(send, read, check, kwargs)


def get_ticker(params={}, **kwargs):
    """ 
    Get ticker for all contracts or specified one.

    Link: 
        https://www.mexc.com/api-docs/futures/market-endpoints#get-ticker-contract-market-data
    Args:
        params (dict):
            symbol (str): Symbol of the contract.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    headers = {}
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    url = f"{base_url}/api/v1/contract/ticker"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.market.get_ticker') 
    return execute_request(send, read, check, kwargs)


def get_funding_rate_history(symbol, page_num=1, page_size=20, **kwargs):
    """ 
    Get funding rate history.

    Link: 
        https://www.mexc.com/api-docs/futures/market-endpoints#get-funding-rate-history
    Args:
        symbol (str): Contract symbol.
        page_num (int): Current page, default 1
        page_size (int): Page size, default 20, max 1000
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
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
    headers = {}
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    url = f"{base_url}/api/v1/contract/funding_rate/history?symbol={symbol}&page_num={page_num}&page_size={page_size}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    rate_limiter.acquire('mexc.futures.market.get_funding_rate_history') 
    return execute_request(send, read, check, kwargs)
