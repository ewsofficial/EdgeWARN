def queue_log(log_queue, message):
    log_queue.put(str(message))


def drain_log_queue(log_queue):
    while not log_queue.empty():
        print(log_queue.get())
