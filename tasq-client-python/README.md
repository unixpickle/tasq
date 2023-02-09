## Tasq python client

### Initialization:

```python
from tasq_client import TasqClient
TasqClient(base_url="<server>",
           username="<username>",
           password="<password>",
           context="<context>")
```

### Sample enqueue usage:

```python
client.push(json.dumps({'key': 'value'}))
```

### Sample worker:

```python
while True:
  with client.pop_running_task() as task:
    if task is None:
      break
    d = json.loads(task.contents)
    assert d['key'] == 'value'
```

## Troubleshooting

If you get the following error:
```requests.exceptions.SSLError: HTTPSConnectionPool(host='<redacted>', port=443): Max retries exceeded with url: /task/completed?context=test (Caused by SSLError(SSLError(1, '[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] decryption failed or bad record mac (_ssl.c:2633)')))```

Try setting the following option at the start of your program:
```python
import multiprocessing
multiprocessing.set_start_method('spawn')
```
