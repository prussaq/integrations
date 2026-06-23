import logging
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.htx as htx

logger = logging.getLogger(__name__)


def query_funding_rate(contract_code, **kwargs):
    """ 
    Query current funding rate for the contract.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb7ec03-77b5-11ed-9966-0242ac110003
    Args:
        contract_code (str): Contract code (case-insensitive), e.g. "BTC-USDT".
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-api/v1/swap_funding_rate?contract_code={contract_code}"

    def send(settings): return http.get(url, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err_code')}: {body.get('err_msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.reference_data.query_funding_rate') 
    return execute_request(send, read, check, kwargs)


def query_batch_funding_rate(params=None, **kwargs):
    """ 
    Query a batch of current funding rate.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb7ed6a-77b5-11ed-9966-0242ac110003
    Args:
        params (dict): 
            contract_code (str): Contract code; defaults to all.
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-api/v1/swap_batch_funding_rate"

    def send(settings): return http.get(url, params=params, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err_code')}: {body.get('err_msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.reference_data.query_batch_funding_rate') 
    return execute_request(send, read, check, kwargs)


def query_historical_funding_rate(contract_code, params=None, **kwargs):
    """ 
    Query historical funding rate.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb7ee4a-77b5-11ed-9966-0242ac110003
    Args:
        contract_code (str): Contract code (case-insensitive), e.g. "BTC-USDT".
        params (dict): 
            page_index (int): Page index; 1 by default.
            page_size (int): Page size (1-50); 20 by default; 50 at most.
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
    if params is None: params = {}
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', htx.TIMEOUT)
    params['contract_code'] = contract_code
    url = f"{base_url}/linear-swap-api/v1/swap_historical_funding_rate"

    def send(settings): return http.get(url, params=params, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err_code')}: {body.get('err_msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.reference_data.query_historical_funding_rate') 
    return execute_request(send, read, check, kwargs)


def query_contract_info(params=None, **kwargs):
    """ 
    Query contract info.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb802c2-77b5-11ed-9966-0242ac110003
    Args:
        params (dict): 
            contract_code (str): Contract code, e.g. swap: "BTC-USDT", futures: "BTC-USDT-210625".
            support_margin_mode (str): Margin mode： cross, isolated, all
            pair (str): Pair, e.g. BTC-USDT.
            contract_type (str): Contract type: swap, this_week, next_week, quarter, next_quarter
            business_type (str): Business type: futures, swap, all; defaults to swap.
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-api/v1/swap_contract_info"

    def send(settings): return http.get(url, params=params, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err_code')}: {body.get('err_msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.reference_data.query_contract_info') 
    return execute_request(send, read, check, kwargs)


def query_contract_elements(params=None, **kwargs):
    """ 
    Query contract elements.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb89359-77b5-11ed-9966-18bd764260c
    Args:
        params (dict): 
            contract_code (str): Contract code; defaults to all.
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-api/v1/swap_query_elements"

    def send(settings): return http.get(url, params=params, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err_code')}: {body.get('err_msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.reference_data.query_contract_elements') 
    return execute_request(send, read, check, kwargs)
