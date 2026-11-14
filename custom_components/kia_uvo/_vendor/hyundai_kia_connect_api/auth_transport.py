"""Typed transient EU authentication failures. Never contains response bodies."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from .exceptions import APIError, RequestTimeoutError
import requests

class TransientAuthError(APIError):
    def __init__(self, retry_after=0, status=None):
        self.retry_after = max(0, float(retry_after))
        self.status = status
        super().__init__("Temporary authentication service failure")

def retry_after(value):
    try:
        seconds = float(value)
        return seconds if 0 <= seconds < float("inf") else 0
    except (ValueError, TypeError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0

class AuthProtocolError(TransientAuthError):
    """An invalid auth response is not evidence of invalid credentials."""
    def __init__(self, retry_after=0, status=None):
        super().__init__(retry_after, status)
        self.args = ("Invalid Kia authentication protocol response",)


class AuthConfigurationError(APIError):
    """Local endpoint configuration needs review; never a login retry."""


TRANSIENT_IO = (requests.Timeout, requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError, RequestTimeoutError)
CONFIG_IO = (requests.exceptions.InvalidURL, requests.exceptions.InvalidSchema,
             requests.exceptions.MissingSchema)


def check_auth_response(response):
    if response.status_code == 429 or response.status_code >= 500:
        error = TransientAuthError(retry_after(response.headers.get("Retry-After")), response.status_code)
        response.close()
        raise error
    return response


def auth_json(response):
    try:
        value = response.json()
    except (ValueError, TypeError):
        raise AuthProtocolError(status=response.status_code) from None
    if not isinstance(value, dict):
        raise AuthProtocolError(status=response.status_code)
    return value


def auth_value(data, name, default="", *, header=False):
    """Validate server token text before headers, Token construction or storage."""
    value = data.get(name, default)
    if value is None:
        value = ""  # optional null/absent values are represented as empty
    if not isinstance(value, str):
        raise AuthProtocolError() from None
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise AuthProtocolError() from None
    if header and (value[:1].isspace() or any(ord(c) > 255 for c in value)):
        raise AuthProtocolError() from None
    return value


def auth_token(data, *names):
    for name in names:
        value = auth_value(data, name, header=True)
        if value.strip():
            return value
    raise AuthProtocolError()


def auth_ttl(value, default):
    if value is None:
        return default
    try:
        ttl = int(value)
        if isinstance(value, bool) or ttl <= 0 or str(ttl) != str(value):
            raise ValueError()
    except (ValueError, TypeError, OverflowError):
        raise AuthProtocolError() from None
    return ttl


def auth_request(method, *args, session=None, **kwargs):
    response = None
    stream = kwargs.get("stream", False)
    kwargs["stream"] = True
    try:
        response = getattr(session if session is not None else requests, method.lower())(*args, **kwargs)
        response = check_auth_response(response)
        if not stream:
            response.content
        return response
    except TRANSIENT_IO:
        if response is not None:
            response.close()
        raise TransientAuthError(
            retry_after(response.headers.get("Retry-After")) if response is not None else 0,
            response.status_code if response is not None else None,
        ) from None
    except CONFIG_IO:
        raise AuthConfigurationError("Invalid Kia auth endpoint configuration") from None


# Only EU login opts in. Vehicle sessions keep their existing transport contract.
from .ApiImpl import ApiImplSession

class AuthSession(ApiImplSession):
    def request(self, *args, **kwargs):
        try:
            return super().request(*args, **kwargs)
        except CONFIG_IO:
            raise AuthConfigurationError("Invalid Kia auth endpoint configuration") from None

    def send(self, request, **kwargs):
        response = None
        stream = kwargs.get("stream", False)
        kwargs["stream"] = True
        try:
            # Check status/Retry-After before reading a potentially broken body.
            response = check_auth_response(super().send(request, **kwargs))
            if not stream:
                response.content
            return response
        except TRANSIENT_IO:
            if response is not None:
                response.close()
            raise TransientAuthError(
                retry_after(response.headers.get("Retry-After")) if response is not None else 0,
                response.status_code if response is not None else None,
            ) from None
