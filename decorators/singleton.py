from threading import Lock


def singleton(cls):

    instances = dict()
    lock = Lock()

    def wrapper(*args, **kwargs):

        if cls not in instances:
            with lock:
                if cls not in instances:
                    instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper