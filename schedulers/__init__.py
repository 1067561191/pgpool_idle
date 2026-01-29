from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler


executors = {"default": APThreadPoolExecutor(max_workers=4)}
background_scheduler = BackgroundScheduler(executors=executors, timezone="Asia/Shanghai")


__all__ = ["background_scheduler"]