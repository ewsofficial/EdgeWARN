def queue_log(log_queue, message):
    log_queue.put(str(message))


import queue


def drain_log_queue(log_queue):
    while True:
        try:
            print(log_queue.get_nowait())
        except queue.Empty:
            break
