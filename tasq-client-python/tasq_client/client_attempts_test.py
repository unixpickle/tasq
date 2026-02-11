import pytest

from .client import TasqClient, Task


@pytest.mark.parametrize(
    ["response", "expected_num_previous_attempts"],
    [
        ({"id": "task-1", "contents": "a"}, None),
        ({"id": "task-1", "contents": "a", "numPreviousAttempts": 2}, 2),
    ],
)
def test_pop_include_attempts(
    monkeypatch,
    response,
    expected_num_previous_attempts,
):
    client = TasqClient("http://example.com")
    called = {}

    def fake_get(path, type_template=None, supports_timeout=False):
        called["path"] = path
        called["supports_timeout"] = supports_timeout
        return response

    monkeypatch.setattr(client, "_get", fake_get)
    task, retry = client.pop()

    assert called["path"] == "/task/pop?includePreviousAttempts=1"
    assert called["supports_timeout"]
    assert task is not None
    assert retry is None
    assert task.id == "task-1"
    assert task.contents == "a"
    assert task.num_previous_attempts == expected_num_previous_attempts


@pytest.mark.parametrize(
    ["response_task", "expected_num_previous_attempts"],
    [
        ({"id": "task-1", "contents": "a"}, None),
        ({"id": "task-1", "contents": "a", "numPreviousAttempts": 3}, 3),
    ],
)
def test_pop_batch_include_attempts(
    monkeypatch,
    response_task,
    expected_num_previous_attempts,
):
    client = TasqClient("http://example.com")
    called = {}

    def fake_post_form(path, args, type_template=None, supports_timeout=False):
        called["path"] = path
        called["args"] = args
        called["supports_timeout"] = supports_timeout
        return {"done": False, "tasks": [response_task], "retry": 0}

    monkeypatch.setattr(client, "_post_form", fake_post_form)
    tasks, retry = client.pop_batch(1)

    assert called["path"] == "/task/pop_batch?includePreviousAttempts=1"
    assert called["args"] == {"count": 1}
    assert called["supports_timeout"]
    assert retry == 0.0
    assert len(tasks) == 1
    assert tasks[0].id == "task-1"
    assert tasks[0].contents == "a"
    assert tasks[0].num_previous_attempts == expected_num_previous_attempts


def test_pop_running_task_preserves_attempts(monkeypatch):
    client = TasqClient("http://example.com")
    monkeypatch.setattr(
        client,
        "pop",
        lambda: (Task(id="task-1", contents="a", num_previous_attempts=4), None),
    )
    monkeypatch.setattr(client, "keepalive", lambda _id: None)
    completed = []
    monkeypatch.setattr(client, "completed", lambda _id: completed.append(_id))

    with client.pop_running_task() as task:
        assert task is not None
        assert task.num_previous_attempts == 4

    assert completed == ["task-1"]
