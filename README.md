# tasq

**tasq** is a simple HTTP-based task queue. Each task is represented as a string (it could be anything). 

Tasks are pushed to the queue via an HTTP endpoint, and popped using another endpoint. The worker performing the popped task can signal its completion using a third endpoint. If tasks aren't completed after a timeout, they can be popped again once all other pending tasks have been popped.

# Protocol

Here are endpoints for pushing and popping tasks:

 * `/task/push` - add a task to the queue. Simply provide a `?contents=X` query argument.
 * `/task/push_batch` - POST to this endpoint with a JSON array of tasks. For example, `["hi", "test"]`.
 * `/task/pop` - pop a task from the queue. If no tasks are available, this may indicate a timeout after which the longest-running task would timeout.
   * On normal response, will return something like `{"data": {"id": "...", "contents": "..."}}`.
   * If queue is empty, will return something like `{"data": {"done": false, "retry": 3.14}}`, where `retry` is the number of seconds after which to try popping again, and `done` is `true` if no tasks are pending or running.
 * `/task/completed` - indicate that the task is completed. Simply provide a `?id=X` query argument.

Additionally, these are some endpoints that may be helpful for maintaining a running queue in practice:
 * `/counts` - get a dictionary containing sizes of queues. Has keys `pending`, `running`, and `completed`.
 * `/task/peek` - look at the next task that would be returned by `/task/pop`. When the queue is empty but tasks are still in progress (but not timed out), this returns extra information. In addition to `done` and `retry` fields, this will return a `next` field containing a dictionary with `id` and `contents` of the next task that will expire. This can make it easier for a human to see which tasks are repeatedly failing or timing out.
 * `/task/clear` - delete all pending and running tasks in the queue.
 * `/task/expire_all` - set all currently running tasks as expired so that they can be re-popped immediately.
 * `/task/queue_expired` - move all expired tasks from the `in-progress` queue to the `pending` queue. This is mostly helpful to get more accurate information in the `/counts` endpoint, but it will also have an effect on prematurely expired tasks: if any worker was still working on an expired task and calls `/task/completed`, a task in the `pending` queue will not be successfully marked as completed.
