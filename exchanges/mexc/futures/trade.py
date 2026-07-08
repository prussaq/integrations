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
        data (dict): Request body parameters (JSON).
            Required:
                symbol (str): Contract symbol.
                price (decimal): Order price.
                vol (decimal): Order quantity.
                side (int): Direction: 1 open long, 2 close short, 3 open short, 4 close long.
                type (int): Order type: 1 limit, 2 Post Only, 3 IOC, 4 FOK, 5 market.
                openType (int): Margin mode: 1 isolated, 2 cross.
            Optional:
                leverage (int): Leverage (required when opening position).
                externalOid (str): External order ID.
                positionId (int): Position ID.
                stopLossPrice (decimal): Stop-loss price.
                takeProfitPrice (decimal): Take-profit price.
                lossTrend (int): Stop-loss price type: 1 latest, 2 fair, 3 index.
                profitTrend (int): Take-profit price type: 1 latest, 2 fair, 3 index.
                priceProtect (int): Trigger protection:
                    1 enabled, 0 disabled (default).
                    Required for plan orders / TP-SL orders.
                positionMode (int): Position mode: 1 hedge, 2 one-way (default: hedge).
                reduceOnly (bool): Reduce-only order (one-way mode only).
                marketCeiling (bool): 100% market open.
                flashClose (bool): Flash close.
                bboTypeNum (int): BBO type:
                    0 none, 1 opposite-1, 2 opposite-5, 3 same-side-1, 4 same-side-5.
                    externalOid is not supported with BBO orders.
                stpMode (int): Self-trade prevention:
                    0 none, 1 cancel both, 2 cancel maker, 3 cancel taker.
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', mexc.FUTURES_BASE_URL)
    timeout = kwargs.pop('timeout', mexc.TIMEOUT)
    method = 'POST'
    url = f"{base_url}/api/v1/private/order/create"
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'
    full = kwargs.pop('full', False)

    rate_limiter.acquire('mexc.futures.trade.place_order')
    mexc.sign_headers(headers, api, method, body=payload)
    response = http.post(url, data=payload, headers=headers, timeout=timeout, **kwargs)
    body = response.json()

    if not isinstance(body, dict):
        raise ApiError("unexpected response type", response=response, body=body)

    if not body.get('success'):
        raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}",
                     response=response, body=body)

    if full: return response, body
    return body
