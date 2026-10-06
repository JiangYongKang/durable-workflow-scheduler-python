"""测试公共配置：统一日志，并在每条用例前后打印其覆盖的业务场景。

运行 ``python3 -m pytest -s`` 即可在日志中看到本轮覆盖了哪些业务。
"""
from __future__ import annotations

import logging
import time

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
logger = logging.getLogger("tests")


@pytest.fixture(autouse=True)
def _log_case_banner(request):
    start = time.perf_counter()
    logger.info("=" * 72)
    logger.info("用例开始: %s", request.node.nodeid)
    doc = (request.function.__doc__ or "").strip().splitlines()
    if doc:
        logger.info("覆盖业务: %s", " ".join(s.strip() for s in doc))
    yield
    logger.info("用例结束: %s（耗时 %.3fs）",
                request.node.nodeid, time.perf_counter() - start)
