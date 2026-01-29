import os
import pytz
import datetime as dt
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from pools import ReadOnlyPGPool
from schedulers import background_scheduler
from tasks import timed_close_idle_timeout_connections

bj_tz = pytz.timezone("Asia/Shanghai")


os.environ["DB_PG_NAME"] = "nlyc"
os.environ["DB_PG_USER"] = "postgres"
os.environ["DB_PG_PASSWORD"] = "123456"
os.environ["DB_PG_PORT"] = "15432"
os.environ["DB_PG_HOST"] = "127.0.0.1"

logger = logging.getLogger("")
logger.setLevel(logging.DEBUG)

log_format = logging.Formatter(
    "%(levelname)s - %(asctime)s - [%(threadName)s] - %(funcName)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"  # 自定义时间格式
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # 控制台只输出INFO及以上级别
console_handler.setFormatter(log_format)

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "app.log")
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5,
    encoding="utf-8"  # 确保中文日志正常显示
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(log_format)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


def job(idx: int):
    logger.info("任务开始")
    pool = ReadOnlyPGPool(2, 4, 60)
    conn = pool._get_conn()
    with conn.cursor() as cursor:
        cursor.execute("select true")
        res = cursor.fetchone()
        logger.info(f"查询到：{res}")
        sleep(30)
    pool._put_conn(conn)
    logger.info("任务结束")
    return res


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == "__main__":
    background_scheduler.add_job(
        func=timed_close_idle_timeout_connections,
        args=[background_scheduler],
        id="timed_close_idle_timeout_connections",
        next_run_time=dt.datetime.now(bj_tz) + dt.timedelta(seconds=20),
        replace_existing=True
    )
    background_scheduler.start()
    background_scheduler.print_jobs()
    with ThreadPoolExecutor(max_workers=5, thread_name_prefix="test_conn_pool") as executor:
        results = executor.map(job, range(10))
    sleep(120)
    background_scheduler.shutdown()
    for res in results:
        logger.info(res)

