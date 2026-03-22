class RequestFailed(Exception):
    """
    Raised when a request fails after retries or encounters a non-retryable error.

    Attributes:
        errors (list[Exception]): Exceptions raised during individual attempts.
    """
    def __init__(self, message, errors):
        super().__init__(f"{message}: {errors}")
        self.errors = errors

class ApiError(Exception):
    """
    API-level failure with a valid response and body.
    """
    def __init__(self, message, *, response=None, body=None):
        super().__init__(message)
        self.response = response
        self.body = body
