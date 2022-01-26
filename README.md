# tasq

**tasq** is a simple HTTP-based task queue. Each task is represented as a string (it could be anything). 

Tasks are pushed to the queue via an HTTP endpoint, and popped using another endpoint. The worker performing the popped task can signal its completion using a third endpoint. If tasks aren't completed after a timeout, they will be re-added to the queue.

 * `/task/push` - add a task to the queue. Simply provide a `?contents=X` query argument.
 * `/task/push_batch` - POST to this endpoint with a JSON array of tasks. For example, `["hi", "test"]`.
 * `/task/pop` - pop a task from the queue. If no tasks are available, this may indicate a timeout after which the longest-running task would timeout.
   * On normal response, will return something like `{"data": {"id": "...", "contents": "..."}}`.
   * If queue is empty, will return something like `{"data": {"done": false, "retry": 3.14}}`, where `retry` is the number of seconds after which to try popping again, and `done` is `true` if no tasks are pending or running.
 * `/task/completed` - indicate that the task is completed. Simply provide a `?id=X` query argument.
