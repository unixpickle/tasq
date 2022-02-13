from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass
class Task:
    id: str
    contents: str


class TasqClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def push(self, contents: str) -> str:
        """Push a task and get its resulting ID."""
        return self._post_form("/task/push", dict(contents=contents), check_type=str)

    def pop(self) -> Tuple[Optional[Task], Optional[float]]:
        """
        Pop a pending task from the queue.

        If no task is returned, a retry time may be returned indicating the
        number of seconds until the next in-progress task will expire. If this
        retry time is also None, then the queue has been exhausted.
        """
        result = self._get("/task/pop", check_type=dict)
        if "id" in result and "contents" in result:
            if not isinstance(result["id"], str) or not isinstance(
                result["contents"], str
            ):
                raise TasqMisbehavingServerError("invalid types for id or contents")
            return Task(id=result["id"], contents=result["contents"]), None
        elif "done" not in result:
            raise TasqMisbehavingServerError("no done field in response")
        elif result["done"]:
            return None, None
        elif "retry" not in result:
            raise TasqMisbehavingServerError("missing retry value")
        else:
            if not isinstance(result["retry"], float) and not isinstance(
                result["retry"], int
            ):
                raise TasqMisbehavingServerError(
                    f"invalid type for retry field: {result['retry']}"
                )
            return None, float(result["retry"])

    def _get(self, path: str, check_type: Optional[Any] = None) -> Any:
        return _process_response(requests.get(self._url_for_path(path)), check_type)

    def _post_form(
        self, path: str, args: Dict[str, str], check_type: Optional[Any] = None
    ) -> Any:
        return _process_response(
            requests.post(self._url_for_path(path), data=args), check_type
        )

    def _post_json(self, path: str, data: Any, check_type: Optional[Any] = None) -> Any:
        return _process_response(
            requests.post(self._url_for_path(path), json=data), check_type
        )

    def _url_for_path(self, path: str) -> str:
        return self.base_url + path


class TasqRemoteError(Exception):
    """An error returned by a remote server."""


class TasqMisbehavingServerError(Exception):
    """An error when a tasq server misbehaves."""


def _process_response(response: requests.Response, check_type: Optional[Any]) -> Any:
    try:
        parsed = response.json()
    except Exception as exc:
        raise TasqMisbehavingServerError("failed to get JSON from response") from exc
    if "error" in parsed:
        raise TasqRemoteError(parsed["error"])
    elif "data" in parsed:
        result = parsed["data"]
        if check_type is not None:
            if not isinstance(result, check_type):
                raise TasqMisbehavingServerError(
                    f"expected response of type {check_type} but got {type(result)}"
                )
        return result
    else:
        raise TasqMisbehavingServerError("missing error or data fields in response")
