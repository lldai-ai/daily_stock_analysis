# -*- coding: utf-8 -*-
"""
===================================
定时调度模块（已修复 14:30 触发问题）
===================================
"""

import logging
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """优雅退出处理器"""

    def __init__(self):
        self.shutdown_requested = False
        self._lock = threading.Lock()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        with self._lock:
            if not self.shutdown_requested:
                logger.info(f"收到退出信号 ({signum})，等待当前任务完成...")
                self.shutdown_requested = True

    @property
    def should_shutdown(self) -> bool:
        with self._lock:
            return self.shutdown_requested


class Scheduler:
    """定时任务调度器"""

    def __init__(self, schedule_time: str = "14:30"):  # ✅ 修正默认值为 14:30
        try:
            import schedule
            self.schedule = schedule
        except ImportError:
            logger.error("schedule 库未安装，请执行: pip install schedule")
            raise ImportError("请安装 schedule 库: pip install schedule")

        self.schedule_time = schedule_time
        self.shutdown_handler = GracefulShutdown()
        self._task_callback: Optional[Callable] = None
        self._running = False

        # ⚠️ 关键：打印当前系统时区，避免时区误解
        logger.info(f"【时区检查】系统当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            import time
            logger.info(f"【时区检查】系统时区: {time.tzname}")
        except:
            pass

    def set_daily_task(self, task: Callable, run_immediately: bool = True):
        self._task_callback = task
        self.schedule.every().day.at(self.schedule_time).do(self._safe_run_task)
        logger.info(f"✅ 已设置每日定时任务，执行时间: {self.schedule_time}（系统本地时间）")
        
        if run_immediately:
            logger.info("⚡ 立即执行一次任务...")
            self._safe_run_task()

    def _safe_run_task(self):
        if self._task_callback is None:
            return
        try:
            logger.info("=" * 50)
            logger.info(f"⏰ 定时任务开始执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 50)
            self._task_callback()
            logger.info(f"✅ 定时任务执行完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logger.exception(f"❌ 定时任务执行失败: {e}")

    def run(self):
        self._running = True
        logger.info("🔄 调度器开始运行...")
        logger.info(f"📅 下次执行时间: {self._get_next_run_time()}")

        while self._running and not self.shutdown_handler.should_shutdown:
            self.schedule.run_pending()
            time.sleep(30)

            # 每小时打印心跳
            if datetime.now().minute == 0 and datetime.now().second < 30:
                logger.info(f"🔄 调度器运行中... 下次执行: {self._get_next_run_time()}")

        logger.info("⏹️ 调度器已停止")

    def _get_next_run_time(self) -> str:
        jobs = self.schedule.get_jobs()
        if jobs:
            next_run = min(job.next_run for job in jobs)
            return next_run.strftime('%Y-%m-%d %H:%M:%S')
        return "未设置"

    def stop(self):
        self._running = False


def run_with_schedule(
    task: Callable,
    schedule_time: str = "14:30",  # ✅ 仅保留一个参数，且默认 14:30
    run_immediately: bool = True
):
    """
    便捷函数：使用定时调度运行任务
    
    ⚠️ 重要提示：
    - schedule 库使用系统本地时间，不支持时区转换
    - 请确保服务器/本机时区为 **北京时间 (Asia/Shanghai, UTC+8)**
    - Linux 设置时区: sudo timedatectl set-timezone Asia/Shanghai
    - Windows: 设置 → 时间和语言 → 时区 → 选择"(UTC+08:00) 北京，重庆..."
    """
    scheduler = Scheduler(schedule_time=schedule_time)
    scheduler.set_daily_task(task, run_immediately=run_immediately)
    scheduler.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
    )

    def test_task():
        print(f"✅ 任务执行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print("🧪 启动测试调度器（按 Ctrl+C 退出）")
    run_with_schedule(test_task, schedule_time="14:30", run_immediately=True)
