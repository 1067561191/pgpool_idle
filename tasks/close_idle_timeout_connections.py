from apscheduler.schedulers.background import BackgroundScheduler
import logging
import pytz
from queue import Empty
import datetime as dt
from time import perf_counter

from pools import ReadOnlyPGPool


bj_tz = pytz.timezone("Asia/Shanghai")
logger = logging.getLogger("pgpool")


def timed_close_idle_timeout_connections(scheduler: BackgroundScheduler, *args, **kwargs):
    try:
        logger.info("定时任务 start")
        pool = ReadOnlyPGPool()
        idle_timeout = pool.idle_timeout
        if not idle_timeout:
            return
        logger.info("定时任务 尝试获取get锁")
        with pool._get_lock:
            logger.info("定时任务 尝试获取put锁")
            with pool._put_lock:
                logger.info("定时任务 双锁获取成功")
                idle_timeout_connections = []
                health_connections = []
                current_time = perf_counter()
                using_count = len(pool.using_connections)
                logger.info(f"正在使用的连接有 {using_count} 个")
                while True:
                    try:
                        conn = pool.idle_connections.get_nowait()
                    except Empty as e:
                        logger.info("定时任务 没有空闲的连接")
                        break
                    else:
                        logger.info(f"定时任务 发现空闲: {id(conn)}")
                        return_time = pool.return_times.get(conn)
                        if not pool._verify_conn(conn):
                            # 连接不可用 丢弃
                            logger.info(f"定时任务 不可用: {id(conn)}")
                            del conn
                            continue
                        if return_time < current_time - idle_timeout:
                            logger.info(f"定时任务 超时: {id(conn)}")
                            idle_timeout_connections.append(
                                (current_time - return_time, conn)
                            )
                        else:
                            logger.info(f"定时任务 健康: {id(conn)}")
                            health_connections.append((current_time - return_time, conn))
                if using_count >= pool.min_conn:
                    # 足够了 直接把多的丢掉
                    logger.info(f"定时任务 正在使用的连接足够多了 直接删掉空闲的连接")
                    for _, conn in idle_timeout_connections:
                        logger.info(f"删掉 {id(conn)}")
                        conn.close()
                        del conn
                    for _,conn in health_connections:
                        logger.info(f"删掉 {id(conn)}")
                        conn.close()
                        del conn
                else:
                    health_connections.sort(key=lambda x: x[0])
                    idle_timeout_connections.sort(key=lambda x: x[0])
                    logger.info(f"定时任务 正在使用的连接不够")
                    if len(health_connections) + using_count >= pool.min_conn:
                        # 活跃的不够 但是加上健康的够了 只要最近使用的那几个
                        logger.info(f"定时任务 但是加上没超时的够了")
                        for _, conn in health_connections[: pool.min_conn - using_count]:
                            logger.info(f"定时任务 从没超时的集合里拿 {id(conn)}")
                            pool.idle_connections.put_nowait(conn)
                        for _, conn in health_connections[pool.min_conn - using_count:]:
                            logger.info(f"定时任务 这个不要了删掉 {id(conn)}")
                            conn.close()
                            pool.return_times.pop(conn)
                    else:
                        # 不够，从超时的里面拿几个凑一下
                        logger.info(f"定时任务 但是加上没超时和超时的够了")
                        for _, conn in health_connections:
                            logger.info(f"定时任务 从没超时的集合里拿 {id(conn)}")
                            pool.idle_connections.put_nowait(conn)
                        for _, conn in idle_timeout_connections[
                            : pool.min_conn - using_count - len(health_connections)
                        ]:
                            logger.info(f"定时任务 从超时的集合里拿 {id(conn)}")
                            pool.idle_connections.put_nowait(conn)
                        for _, conn in idle_timeout_connections[pool.min_conn - using_count - len(health_connections): ]:
                            logger.info(f"定时任务 这个不要了删掉 {id(conn)}")
                            conn.close()
                            pool.return_times.pop(conn)
                logger.info("定时任务 释放put锁")
            logger.info("定时任务 释放get锁")
    finally:
        scheduler.print_jobs()
        next_run_time = dt.datetime.now(bj_tz) + dt.timedelta(seconds=20)
        scheduler.add_job(func=timed_close_idle_timeout_connections, args=[scheduler], id="timed_close_idle_timeout_connections", next_run_time=next_run_time, replace_existing=True)
        scheduler.print_jobs()