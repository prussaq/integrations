import logging
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.htx as htx

logger = logging.getLogger(__name__)

def query_unified_account_assets(api, params={}, *, headers={}, **kwargs):
    """ 
    Query unified account assets (positions).

    Link: 
        https://www.htx.com/en-us/opend/newApiPages/?id=10000074-77b7-11ed-9966-0242ac110003
    Args:
        api (dict): API credentials. See `sign_params` api parameter.
        params (dict): 
            contract_code (str): Contract code; defaults to all.
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
    method = 'GET'
    host = base_url.replace('https://', '')
    path = '/linear-swap-api/v3/unified_account_info'
    url = f"{base_url}{path}"

    def send(): 
        htx.sign_params(params, api, method, host, path)
        return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != 200: 
            raise ApiError(f"HTX returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('htx.new.usdtm_futures.unified_account.query_unified_account_assets') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get unified account assets from HTX: %s', e); raise
