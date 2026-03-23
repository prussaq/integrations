import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.okx as okx

logger = logging.getLogger(__name__)


def get_instruments(inst_type, params={}, *, headers={}, **kwargs):
    """ 
    Retrieve a list of instruments with open contracts for OKX.

    Link: 
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments
    Args:
        inst_type (str): Instrument type: SPOT, MARGIN, SWAP (Perpetual), FUTURES (Expiry), OPTION
        params (dict):
            instFamily (str): Instrument family. Applicable to FUTURES/SWAP/OPTION. 
                If instType is OPTION, instFamily is required.
            instId (str): Instrument ID.
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    params['instType'] = inst_type
    url = f"{base_url}/api/v5/public/instruments"

    def send(): return http.get(url, params=params, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.public_data.rest.get_instruments') 
    return execute_request(send, read, check, kwargs)


def get_funding_rate(inst_id, *, headers={}, **kwargs):
    """ 
    Retrieve funding rate. Only applicable to SWAP.

    Link: 
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate
    Args:
        inst_id (str): Instrument ID, e.g. BTC-USD-SWAP, or ANY to return the funding rate of all swap symbols.
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    url = f"{base_url}/api/v5/public/funding-rate?instId={inst_id}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.public_data.rest.get_funding_rate') 
    return execute_request(send, read, check, kwargs)


def get_funding_rate_history(inst_id, params={}, *, headers={}, **kwargs):
    """ 
    Retrieve funding rate history up to three months. Only applicable to SWAP.

    Link: 
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate-history
    Args:
        inst_id (str): Instrument ID, e.g. BTC-USD-SWAP
        params (dict):
            before (str): Pagination of data to return records newer than the requested fundingTime
            after (str): Pagination of data to return records earlier than the requested fundingTime
            limit (str): Number of results per request. The maximum is 400; The default is 400
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    params['instId'] = inst_id
    url = f"{base_url}/api/v5/public/funding-rate-history"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.public_data.rest.get_funding_rate_history') 
    return execute_request(send, read, check, kwargs)


def get_mark_price(inst_type, params={}, *, headers={}, **kwargs):
    """ 
    Retrieve mark price.

    Link: 
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-mark-price
    Args:
        inst_type (str): Instrument type: MARGIN, SWAP, FUTURES, OPTION
        params (dict):
            instFamily (str): Instrument family. Applicable to FUTURES/SWAP/OPTION
            instId (str): Instrument ID, e.g. BTC-USD-SWAP
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    params['instType'] = inst_type
    url = f"{base_url}/api/v5/public/mark-price"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.public_data.rest.get_mark_price') 
    return execute_request(send, read, check, kwargs)


def get_mark_price_candlesticks(instId, params={}, *, headers={}, **kwargs):
    """ 
    Retrieve the candlestick charts of mark price. This endpoint can retrieve the latest 1,440 data entries. 
    Charts are returned in groups based on the requested bar.

    Link: 
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-mark-price-candlesticks
    Args:
        instId (str): Instrument ID, e.g. BTC-USD-SWAP
        params (dict):
            after (str): Pagination of data to return records earlier than the requested ts
            before (str): Pagination of data to return records newer than the requested ts. 
                The latest data will be returned when using before individually.
            bar (str): Bar size, the default is 1m. e.g. [1m/3m/5m/15m/30m/1H/2H/4H]
                UTC+8 opening price k-line: [6H/12H/1D/1W/1M/3M]
                UTC+0 opening price k-line: [6Hutc/12Hutc/1Dutc/1Wutc/1Mutc/3Mutc]
            limit (str): Number of results per request. The maximum is 100; The default is 100
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    params['instId'] = instId
    url = f"{base_url}/api/v5/market/mark-price-candles"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('okx.api.public_data.rest.get_mark_price_candlesticks') 
    return execute_request(send, read, check, kwargs)
