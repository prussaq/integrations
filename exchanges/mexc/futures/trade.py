import logging
import json
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.mexc as mexc

logger = logging.getLogger(__name__)


def place_order(api, data, **kwargs):
    """
    Place an order.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
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
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', mexc.TIMEOUT)
    method = 'POST'
    url = f"{base_url}/api/v1/private/order/create"
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    rate_limiter.acquire('mexc.futures.trade.place_order')
    mexc.sign_headers(headers, api, method, body=payload)
    response = http.post(url, data=payload, timeout=timeout, **kwargs)
    body = response.json()

    if not isinstance(body, dict):
        raise ApiError("unexpected response type", response=response, body=body)

    if not body.get('success'):
        raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}",
                     response=response, body=body)

    if kwargs.get('full'):
        return response, body
    return body
