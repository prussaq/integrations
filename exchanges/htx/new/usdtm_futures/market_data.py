import logging
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.htx as htx

logger = logging.getLogger(__name__)


def get_market_depth(contract_code, type, *, headers={}, **kwargs):
    """ 
    Get market depth.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb808ad-77b5-11ed-9966-0242ac110003
    Args:
        contract_code (str): Contract code or contract type , e.g. BTC-USDT, BTC-USDT-220325, BTC-USDT-CW.
        type (str): Get depth data within step 150, use step0, step1, step2, step3...
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
    base_url = kwargs.get('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-ex/market/depth?contract_code={contract_code}&type={type}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err-code')}: {body.get('err-msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.market_data.get_market_depth') 
    return execute_request(send, read, check, kwargs)


def get_market_BBO_data(params={}, *, headers={}, **kwargs):
    """ 
    Get market BBO data.

    `business_type` is a required parameter when query info of futures contract, and its value must be futures or all.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb8098e-77b5-11ed-9966-0242ac110003
    Args:
        params (dict):
            contract_code (str): Contract code or contract type , e.g. BTC-USDT, BTC-USDT-220325, BTC-USDT-CW.
            business_type (str): Business type: futures, swap, all. Default is swap.  
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
    base_url = kwargs.get('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-ex/market/bbo"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err-code')}: {body.get('err-msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.market_data.get_market_BBO_data') 
    return execute_request(send, read, check, kwargs)


def get_kline_data(contract_code, period, params, *, headers={}, **kwargs):
    """ 
    Get kline data for up to the last two years.

    Either `size` or both `from` and `to` must be specified in `params`. 
    If all are specified, `from` and `to` are ignored.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb80aca-77b5-11ed-9966-0242ac110003
    Args:
        contract_code (str): Contract code or contract type , e.g. BTC-USDT, BTC-USDT-220325, BTC-USDT-CW.
        period (str): KLine type: 1min, 5min, 15min, 30min, 60min, 1hour, 4hour, 1day, 1mon
        params (dict): 
            size (int): Acquisition quantity (1, 2000); defaults to 150.
            from (long): Start timestamp (seconds).
            to (long): End timestamp (seconds).
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
    base_url = kwargs.get('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', htx.TIMEOUT)
    params['contract_code'] = contract_code
    params['period'] = period
    url = f"{base_url}/linear-swap-ex/market/history/kline"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err-code')}: {body.get('err-msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.market_data.get_kline_data') 
    return execute_request(send, read, check, kwargs)


def get_last_trade(params={}, *, headers={}, **kwargs):
    """ 
    Query the last trade of a contract.

    The request parameter "contract_code" supports the contract code of futures, in that the format is BTC-USDT-210625; 
        and supports contract type: BTC-USDT, BTC-USDT-CW, BTC-USDT-NW, BTC-USDT-CQ, BTC-USDT-NQ.
    `business_type` is a required parameter when query info of futures contract, and its value must be futures or all.

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=8cb80f4c-77b5-11ed-9966-0242ac110003
    Args:
        params (dict):
            contract_code (str): Contract code or contract type , e.g. BTC-USDT, BTC-USDT-220325, BTC-USDT-CW.
            business_type (str): Business type: futures, swap, all. Default is swap.  
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
    base_url = kwargs.get('base_url', htx.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', htx.TIMEOUT)
    url = f"{base_url}/linear-swap-ex/market/trade"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        status = body.get('status')
        if status != 'ok': 
            raise ApiError(f"HTX returned {status}: {body.get('err-code')}: {body.get('err-msg')}", 
                response=response, body=body)

    rate_limiter.acquire('htx.new.usdtm_futures.market_data.get_last_trade') 
    return execute_request(send, read, check, kwargs)
