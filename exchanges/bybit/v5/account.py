import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bybit as bybit

logger = logging.getLogger(__name__)

def get_transferable_amount_unified(api, coin, *, headers={}, **kwargs):
    """ 
    Query the available amount to transfer of a specific coin in the Unified wallet.

    Link: 
        https://bybit-exchange.github.io/docs/v5/account/unified-trans-amnt
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        coin (str): Coin name, uppercase only.
            Supports up to 20 coins per request, use comma to separate: BTC,USDC,USDT,SOL
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    params = {'coinName': coin}
    query = urlencode(params)
    url = f"{base_url}/v5/account/withdrawal?{query}"

    def send(): 
        bybit.sign_headers(headers, api, recv_window, query)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.account.get_transferable_amount_unified')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get transferable amount (unified) from Bybit: %s', e); raise

def get_transaction_log(api, params={}, *, headers={}, **kwargs):
    """ 
    Query for transaction logs in your Unified account. It supports up to 2 years worth of data.

    startTime and endTime are not passed, return 24 hours by default
    Only startTime is passed, return range between startTime and startTime+24 hours
    Only endTime is passed, return range between endTime-24 hours and endTime
    If both are passed, the rule is endTime - startTime <= 7 days

    Link: 
        https://bybit-exchange.github.io/docs/v5/account/transaction-log
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        params (dict):
            accountType (str): Account Type. UNIFIED
            category (str): Product type spot,linear,option,inverse
            currency (str): Currency, uppercase only
            baseCoin (str): BaseCoin, uppercase only. e.g., BTC of BTCPERP
            type (str): Types of transaction logs
            transSubType (str): movePosition, used to filter trans logs of Move Position only
            startTime (int): The start timestamp (ms)
            endTime (int): The end timestamp (ms)
            limit (int): Limit for data size per page. [1, 50]. Default: 20
            cursor (str): Cursor. Use the nextPageCursor token from the response to retrieve the next page of the result set
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    query = urlencode(params)
    url = f"{base_url}/v5/account/transaction-log?{query}"

    def send(): 
        bybit.sign_headers(headers, api, recv_window, query)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.account.get_transaction_log')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get transaction log (unified) from Bybit: %s', e); raise

def get_account_info(api, *, headers={}, **kwargs):
    """ 
    Query the account information, like margin mode, account mode, etc.

    Link: 
        https://bybit-exchange.github.io/docs/v5/account/account-info
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/account/info"

    def send(): 
        bybit.sign_headers(headers, api, recv_window)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.account.get_account_info')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get account info from Bybit: %s', e); raise
