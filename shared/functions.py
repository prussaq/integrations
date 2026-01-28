import logging
import time
import requests

from integrations.shared.exceptions import RequestFailed
from integrations.shared.settings import RETRIES, DELAY, BACKOFF

logger = logging.getLogger(__name__)

def execute_request(send, read, check, settings={}):
    """
    Execute a request with retries.

    Retries are applied only to transport- and protocol-level failures
    (e.g., network errors, timeouts, malformed response bodies).
    HTTP 4xx client errors and any exception raised by `check` are
    considered non-retryable and are propagated immediately.

    Args:
        send (callable): Performs the HTTP request.
        read (callable): Parses the HTTP response.
        check (callable): Validates the request result and may raise domain-specific exceptions.
        settings (dict):
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body. 
    Raises:
        RequestFailed:
            Raised when:
            - a non-retryable transport/protocol error is encountered, or
            - the retry budget is exhausted.
            The exception is inspectable and exposes:
            - `errors`: a list of exceptions raised during individual attempts, ordered by occurrence.
        Any exception raised by `check`:
            Propagated immediately without retry.
    """
    attempt = 0
    errors = []
    response = None
    body = None
    retries = settings.get('retries', RETRIES)
    delay = settings.get('delay', DELAY)
    backoff = settings.get('backoff', BACKOFF)

    while attempt < retries:
        attempt += 1
        try:
            response = send()
            response.raise_for_status()
            body = read(response)
            break
        except requests.exceptions.HTTPError as e: 
            logger.warning("Request attempt %d failed with HTTP error: %s", attempt, e, exc_info=True, 
                extra={"status_code": response.status_code})
            errors.append(e)
            if response.status_code < 500: 
                raise RequestFailed(f"non-retryable error encountered on attempt {attempt}", errors)
        except Exception as e:
            logger.warning("Request attempt %d failed: %s", attempt, e, exc_info=True, 
                extra={'response': response, 'body_truncated': str(body)[:1000]})
            errors.append(e)
        if attempt == retries: raise RequestFailed(f"retry budget of {retries} attempt(s) exhausted", errors)
        time.sleep(delay)
        delay *= backoff

    check(response, body)
    if settings.get('full'): return response, body
    return body
