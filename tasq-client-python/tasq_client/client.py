import sys
import time
import urllib.parse
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing import Process, Queue
from typing import Any, Dict, List, Optional, Tuple

import requests

from .check_type import CheckTypeException, OptionalKey, check_type


@dataclass
class Task:
    """A task returned by the remote server."""

    id: str
    contents: str


class TasqClient:
    def __init__(
        self, base_url: str, keepalive_interval: float = 30.0, context: str = ""
    ):
        self.base_url = base_url.rstrip("/")
        self.keepalive_interval = keepalive_interval
        self.context = context
        self.session = requests.Session()

    def push(self, contents: str) -> str:
        """Push a task and get its resulting ID."""
        return self._post_form("/task/push", dict(contents=contents), type_template=str)

    def push_batch(self, ids: List[str]) -> List[str]:
        """Push a batch of tasks and get their resulting IDs."""
        return self._post_json("/task/push_batch", ids, type_template=[str])

    def pop(self) -> Tuple[Optional[Task], Optional[float]]:
        """
        Pop a pending task from the queue.

        If no task is returned, a retry time may be returned indicating the
        number of seconds until the next in-progress task will expire. If this
        retry time is also None, then the queue has been exhausted.
        """
        result = self._get(
            "/task/pop",
            type_template={
                OptionalKey("id"): str,
                OptionalKey("contents"): str,
                OptionalKey("retry"): float,
                OptionalKey("done"): bool,
            },
        )
        if "id" in result and "contents" in result:
            return Task(id=result["id"], contents=result["contents"]), None
        elif "done" not in result:
            raise TasqMisbehavingServerError("no done field in response")
        elif result["done"]:
            return None, None
        elif "retry" not in result:
            raise TasqMisbehavingServerError("missing retry value")
        else:
            return None, float(result["retry"])

    def pop_batch(self, n: int) -> Tuple[List[Task], Optional[float]]:
        """
        Retrieve at most n tasks from the queue.

        If fewer than n tasks are returned, a retry time may be returned to
        indicate when the next pending task will expire.

        If no tasks are returned and the retry time is None, then the queue has
        been exhausted.
        """
        response = self._post_form(
            "/task/pop_batch",
            dict(count=n),
            type_template={
                "done": bool,
                "tasks": [dict(id=str, contents=str)],
                OptionalKey("retry"): float,
            },
        )

        if response["done"]:
            return [], None

        retry = float(response["retry"]) if "retry" in response else None

        if len(response["tasks"]):
            return [
                Task(id=x["id"], contents=x["contents"]) for x in response["tasks"]
            ], retry
        elif retry is not None:
            return [], retry
        else:
            raise TasqMisbehavingServerError(
                "no retry time specified when tasks are empty and done is false"
            )

    def completed(self, id: str):
        """Indicate that an in-progress task has been completed."""
        self._post_form("/task/completed", dict(id=id))

    def completed_batch(self, ids: List[str]):
        """Indicate that some in-progress tasks have been completed."""
        self._post_json("/task/completed_batch", ids)

    def keepalive(self, id: str):
        """Reset the timeout interval for a still in-progress task."""
        self._post_form("/task/keepalive", dict(id=id))

    @contextmanager
    def pop_running_task(self) -> Optional["RunningTask"]:
        """
        Pop a task from the queue and wrap it in a RunningTask, blocking until
        the queue is completely empty or a task is successfully popped.

        The resulting RunningTask manages a background process that sends
        keepalives for the returned task ID at regular intervals.

        This is meant to be used in a `with` clause. When the `with` clause is
        exited, the keepalive loop is stopped, and the task will be marked as
        completed unless the with clause is exited with an exception.
        """
        while True:
            task, timeout = self.pop()
            if task is not None:
                rt = RunningTask(self, id=task.id, contents=task.contents)
                try:
                    yield rt
                    rt.completed()
                finally:
                    rt.cancel()
                return
            elif timeout is not None:
                time.sleep(timeout)
            else:
                yield None
                return

    def _get(self, path: str, type_template: Optional[Any] = None) -> Any:
        return _process_response(
            self.session.get(self._url_for_path(path)), type_template
        )

    def _post_form(
        self, path: str, args: Dict[str, str], type_template: Optional[Any] = None
    ) -> Any:
        return _process_response(
            self.session.post(self._url_for_path(path), data=args), type_template
        )

    def _post_json(
        self, path: str, data: Any, type_template: Optional[Any] = None
    ) -> Any:
        return _process_response(
            self.session.post(self._url_for_path(path), json=data), type_template
        )

    def _url_for_path(self, path: str) -> str:
        return self.base_url + path + "?context=" + urllib.parse.quote(self.context)


@dataclass
class RunningTask(Task):
    """
    A task object that periodically sends keepalives in the background until
    cancel() or completed() is called.

    When used as a context manager, the background keepalive loop will always
    be terminated when the context is exited. The task will be marked as
    complete unless the context was exited with an exception.
    """

    def __init__(self, client: TasqClient, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self._proc = Process(
            target=RunningTask._keepalive_worker,
            name="tasq-keepalive-worker",
            args=(client.base_url, client.context, client.keepalive_interval, self.id),
            daemon=True,
        )
        self._proc.start()

    def cancel(self):
        if self._proc is None:
            return
        self._proc.kill()
        self._proc.join()
        self._proc = None

    def completed(self):
        self.cancel()
        self.client.completed(self.id)

    @staticmethod
    def _keepalive_worker(base_url: str, context: str, interval: float, task_id: str):
        client = TasqClient(base_url, context=context)
        while True:
            try:
                client.keepalive(task_id)
            except Exception as exc:
                print(f"exception in tasq keepalive worker: {exc}", file=sys.stderr)
            time.sleep(interval)


class TasqRemoteError(Exception):
    """An error returned by a remote server."""


class TasqMisbehavingServerError(Exception):
    """An error when a tasq server misbehaves."""


def _process_response(response: requests.Response, type_template: Optional[Any]) -> Any:
    try:
        parsed = response.json()
    except Exception as exc:
        raise TasqMisbehavingServerError("failed to get JSON from response") from exc

    check_template = {
        OptionalKey("error"): str,
        OptionalKey("data"): object if type_template is None else type_template,
    }
    try:
        check_type(check_template, parsed)
    except CheckTypeException as exc:
        raise TasqMisbehavingServerError(f"invalid response object: {exc}")

    if "error" in parsed:
        raise TasqRemoteError(parsed["error"])
    elif "data" in parsed:
        return parsed["data"]
    else:
        raise TasqMisbehavingServerError("missing error or data fields in response")
