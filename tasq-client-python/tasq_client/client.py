import random
import sys
import time
import urllib.parse
from contextlib import contextmanager
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

from .check_type import CheckTypeException, OptionalKey, OptionalValue, check_type


@dataclass
class Task:
    """A task returned by the remote server."""

    id: str
    contents: str


@dataclass
class QueueCounts:
    """Task counts for a single queue."""

    pending: int
    running: int
    expired: int
    completed: int

    # Won't be set if a time window wasn't specified in the request, or if the
    # server is old enough to not support rate estimation.
    rate: Optional[float] = None

    modtime: Optional[int] = None


class TasqClient:
    """
    An object for interacting with a specific queue in a tasq server.

    :param base_url: the URL for the server, possibly with a trailing
                     slash, but with no extra path appended to it.
                     E.g. "http://tasq.mydomain.com/".
    :param keepalive_interval: for running tasks from pop_running_task(),
                               this is how often keepalives are sent.
    :param context: the queue context (empty string uses default queue).
    :param username: optional username for basic authentication.
    :param password: optional password for basic authentication.
    :param max_timeout: the maximum amount of time (in seconds) to wait
                        between attempts to pop a task in pop_running_task(),
                        or push a task in push_blocking().
                        Lower values mean waiting less long to pop in the case
                        that a new task is pushed or all tasks are finished.
    :param task_timeout: if specified, override the timeout on the server with
                         a custom timeout. This can be useful if we know we
                         will be sending frequent keepalives, but the server
                         has a longer timeout period.
    :param retry_server_errors: if True, retry requests if the server returns
                                certain 5xx status codes.
    """

    def __init__(
        self,
        base_url: str,
        keepalive_interval: float = 30.0,
        context: str = "",
        username: Optional[str] = None,
        password: Optional[str] = None,
        max_timeout: float = 30.0,
        task_timeout: Optional[float] = None,
        retry_server_errors: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.keepalive_interval = keepalive_interval
        self.context = context
        self.username = username
        self.password = password
        self.max_timeout = max_timeout
        self.task_timeout = task_timeout
        self.retry_server_errors = retry_server_errors
        self.session = requests.Session()
        self._configure_session()

    def push(self, contents: str, limit: int = 0) -> Optional[str]:
        """
        Push a task and get its resulting ID.

        If limit is specified, then the task will not be pushed if the queue is
        full, in which case None is returned.
        """
        return self._post_form(
            f"/task/push", dict(contents=contents, limit=limit), type_template=OptionalValue(str)
        )

    def push_batch(self, ids: List[str], limit: int = 0) -> Optional[List[str]]:
        """
        Push a batch of tasks and get their resulting IDs.

        If limit is specified, then tasks will not be pushed if the queue does
        not have room for all the tasks at once, in which case None is
        returned.

        If limit is negative, then (-limit + batch_size) is used as the limit.
        This effectively limits the size of the queue before a push rather than
        after the push, to prevent large batches from being less likely to be
        pushed than larger batches.
        """
        if limit < 0:
            limit = -limit + len(ids)
        return self._post_json(
            f"/task/push_batch?limit={limit}", ids, type_template=OptionalValue([str])
        )

    def push_blocking(
        self, contents: List[str], limit: int, init_wait_time: float = 1.0
    ) -> List[str]:
        """
        Push one or more tasks atomically and block until they are pushed.

        If the queue cannot fit the batch, this will wait to retry with random
        exponential backoff. Backoff is randomized to mitigate starvation.

        See push_batch() for details on passing a negative limit to avoid
        starvation of larger batches when pushing from multiple processes.

        Unlike push_batch(), the ids returned by this method will never be
        None, since all tasks must be pushed.
        """
        assert isinstance(
            contents, (list, tuple)
        ), f"expected a list of task contents, got object of type {type(contents)}"
        assert (
            init_wait_time <= self.max_timeout
        ), f"wait time {init_wait_time=} should not be larger than {self.max_timeout=}"
        assert limit < 0 or limit >= len(contents)

        cur_wait = init_wait_time
        while True:
            ids = self.push_batch(contents, limit=limit)
            if ids is not None:
                return ids
            timeout = cur_wait * random.random()
            time.sleep(timeout)
            # Use summation instead of doubling to prevent really rapid
            # growth of cur_wait with low probability.
            cur_wait = min(cur_wait + timeout, self.max_timeout)

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
            supports_timeout=True,
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
            supports_timeout=True,
        )

        if response["done"]:
            return [], None

        retry = float(response["retry"]) if "retry" in response else None

        if len(response["tasks"]):
            return [Task(id=x["id"], contents=x["contents"]) for x in response["tasks"]], retry
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
        self._post_form("/task/keepalive", dict(id=id), supports_timeout=True)

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
                time.sleep(min(timeout, self.max_timeout))
            else:
                yield None
                return

    def counts(self, rate_window: int = 0) -> QueueCounts:
        """Get the number of tasks in each state within the queue."""
        data = self._get(
            f"/counts?window={rate_window}&includeModtime=1",
            {
                "pending": int,
                "running": int,
                "expired": int,
                "completed": int,
                OptionalKey("minute_rate"): float,
            },
        )
        return QueueCounts(**data)

    def __getstate__(
        self,
    ):
        res = self.__dict__.copy()
        del res["session"]
        return res

    def __setstate__(self, state: Dict[str, Any]):
        self.__dict__ = state
        self.session = requests.Session()
        self._configure_session()

    def _configure_session(self):
        if self.username is not None or self.password is not None:
            assert self.username is not None and self.password is not None
            self.session.auth = (self.username, self.password)
        if self.retry_server_errors:
            retries = Retry(
                total=10,
                backoff_factor=1.0,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=False,
            )
            for schema in ("http://", "https://"):
                self.session.mount(schema, HTTPAdapter(max_retries=retries))

    def _get(
        self, path: str, type_template: Optional[Any] = None, supports_timeout: bool = False
    ) -> Any:
        return _process_response(
            self.session.get(self._url_for_path(path, supports_timeout)), type_template
        )

    def _post_form(
        self,
        path: str,
        args: Dict[str, str],
        type_template: Optional[Any] = None,
        supports_timeout: bool = False,
    ) -> Any:
        return _process_response(
            self.session.post(self._url_for_path(path, supports_timeout), data=args), type_template
        )

    def _post_json(
        self,
        path: str,
        data: Any,
        type_template: Optional[Any] = None,
        supports_timeout: bool = False,
    ) -> Any:
        return _process_response(
            self.session.post(self._url_for_path(path, supports_timeout), json=data), type_template
        )

    def _url_for_path(self, path: str, supports_timeout: bool) -> str:
        separator = "?" if "?" not in path else "&"
        result = self.base_url + path + separator + "context=" + urllib.parse.quote(self.context)
        if supports_timeout and self.task_timeout is not None:
            result += "&timeout=" + urllib.parse.quote(f"{self.task_timeout:f}")
        return result


@dataclass
class RunningTask(Task):
    """
    A task object that periodically sends keepalives in the background until
    cancel() or completed() is called.
    """

    def __init__(self, client: TasqClient, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self._kill_queue = Queue()
        self._thread = Thread(
            target=RunningTask._keepalive_worker,
            name="tasq-keepalive-worker",
            args=(
                self._kill_queue,
                client,
                self.id,
            ),
            daemon=True,
        )
        self._thread.start()

    def cancel(self):
        if self._thread is None:
            return
        self._kill_queue.put(None)
        self._thread.join()
        self._thread = None

    def completed(self):
        self.cancel()
        self.client.completed(self.id)

    @staticmethod
    def _keepalive_worker(
        kill_queue: Queue,
        client: TasqClient,
        task_id: str,
    ):
        while True:
            try:
                client.keepalive(task_id)
            except Exception as exc:  # pylint: disable=broad-except
                # Ignore the error if we killed the thread during the
                # keepalive call.
                try:
                    kill_queue.get(block=False)
                    return
                except Empty:
                    pass
                print(f"exception in tasq keepalive worker: {exc}", file=sys.stderr)
            try:
                kill_queue.get(timeout=client.keepalive_interval)
                return
            except Empty:
                pass


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
        raise TasqMisbehavingServerError(f"invalid response object: {exc}") from exc

    if "error" in parsed:
        raise TasqRemoteError(parsed["error"])
    elif "data" in parsed:
        return parsed["data"]
    else:
        raise TasqMisbehavingServerError("missing error or data fields in response")
