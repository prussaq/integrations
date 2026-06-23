import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.kucoin as kucoin

logger = logging.getLogger(__name__)


def get_public_token(**kwargs):
    """ 
    Get public futures websocket token and additional info.

    Link: 
        https://www.kucoin.com/docs-new/websocket-api/base-info/get-public-token-futures
    Args:
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
    base_url = kwargs.pop('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v1/bullet-public"

    def send(settings): return http.post(url, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_websocket.base_info.futures.get_public_token')
    return execute_request(send, read, check, kwargs)


def get_private_token(api, **kwargs):
    """ 
    Get private futures websocket token and additional info.

    Link: 
        https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-futures
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
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
    headers = kwargs.pop('headers', {})
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', kucoin.TIMEOUT)
    method = 'POST'
    endpoint = '/api/v1/bullet-private'
    url = base_url + endpoint

    def send(settings): 
        kucoin.sign_headers(headers, api, method, endpoint)
        return http.post(url, headers=headers, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_websocket.base_info.futures.get_private_token') 
    return execute_request(send, read, check, kwargs)
