# services/utils.py
import tenacity
import logging

logger = logging.getLogger(__name__)

retry_policy = tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
    stop=tenacity.stop_after_attempt(5),
    after=tenacity.after_log(logger, logging.INFO),
)
