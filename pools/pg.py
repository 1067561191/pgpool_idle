from queue import Queue, Empty, Full
from weakref import WeakSet
from psycopg2._psycopg import connection
from psycopg2 import connect
from decorators import singleton
from threading import Lock
import logging
import os
from time import perf_counter

logger = logging.getLogger("pgpool")


@singleton
class ReadOnlyPGPool(object):
    """针对pg库的只读的连接池"""

    def __init__(self, min_conn: int = 4, max_conn: int = 12, idle_timeout: int = 360):
        self.min_conn = min_conn
        self.max_conn = max_conn
        self.idle_timeout = idle_timeout
        self.idle_connections = Queue(maxsize=self.max_conn)
        self.using_connections = WeakSet()
        self.return_times = dict()
        self._get_lock = Lock()  # 获取连接时的锁
        self._put_lock = Lock()  # 释放连接时的所

    def _connect(self, *args, **kwargs) -> connection:
        """创建连接 并丢到正在使用集合里"""
        logger.info("开始创建连接")
        conn = connect(
            dbname=os.getenv("DB_PG_NAME"),
            user=os.getenv("DB_PG_USER"),
            password=os.getenv("DB_PG_PASSWORD"),
            host=os.getenv("DB_PG_HOST"),
            port=int(os.getenv("DB_PG_PORT")),
        )
        conn.readonly = True
        self.using_connections.add(conn)
        logger.info(f"连接创建成功: {id(conn)}")
        return conn

    def _verify_conn(self, conn: connection) -> bool:
        """验证连接可用"""
        logger.info(f"验证是否可用：{id(conn)}")
        if conn.closed:
            logger.info(f"已关闭不可用： {id(conn)}")
            return False
        try:
            with conn.cursor() as c:
                c.execute("SELECT true")
                c.fetchone()
                logger.info(f"可查询可用：{id(conn)}")
                return True
        except:
            logger.info(f"查询失败不可用：{id(conn)}")
            return False

    def _get_conn(self, *args, **kwargs) -> connection:
        """如果池子里有就直接拿，没有就看满没满，没满就创建再拿，否则就阻塞"""
        logger.info(f"尝试获取锁")
        with self._get_lock:
            logger.info("获取锁成功")
            while True:
                try:
                    conn = self.idle_connections.get_nowait()
                    logger.info(f"直接拿到空闲conn：{id(conn)}")
                    self.using_connections.add(conn)
                except Empty as e:
                    # 没有可用的了
                    logger.info("没有可以用的了")
                    if len(self.using_connections) >= self.max_conn:
                        # 超出最大连接数了
                        logger.info("超出最大连接数了 阻塞一下吧")
                        conn = self.idle_connections.get(True)
                        logger.info(f"超出最大连接数了 阻塞之后拿到conn：{id(conn)}")
                        self.using_connections.add(conn)
                    else:
                        # 自己创建吧
                        logger.info(f"没有超出最大连接数 自己创建")
                        conn = self._connect()
                        logger.info(f"没有超出最大连接数 自己创建：{id(conn)}")
                if not self._verify_conn(conn):
                    logger.info(f"验证失败 删除: {id(conn)}")
                    del conn
                else:
                    break
            logger.info(f"拿到了 {id(conn)} 释放锁")
            return conn

    def _put_conn(self, conn: connection, *args, **kwargs):
        """连接放回池子"""
        logger.info("尝试获取锁")
        with self._put_lock:
            logger.info("获取到锁了")
            self.using_connections.discard(conn)
            current_time = perf_counter()
            try:
                self.return_times[conn] = current_time
                self.idle_connections.put_nowait(conn)
                logger.info("放回池子里了")
            except Full as e:
                # 满了
                logger.info("池子满了直接关掉丢弃")
                conn.close()
                self.return_times.pop(conn)
            logger.info("释放锁")

    def clear(self):
        while True:
            try:
                conn = self.idle_connections.get_nowait()
            except Empty as e:
                break
            else:
                self.return_times.pop(conn)
                conn.close()