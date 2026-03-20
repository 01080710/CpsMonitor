import logging, json, sys, time, functools, traceback


class JsonFormatter(logging.Formatter):
    RESERVED_ATTRS = {
        "args","msg","levelname","levelno","pathname","filename",
        "module","exc_info","exc_text","stack_info","lineno",
        "funcName","created","msecs","relativeCreated","thread",
        "threadName","processName","process","name"
    }

    def format(self, record):
        # 基本 log 結構
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "service": getattr(record, "service", "unknown"),
            "logger": record.name,
            "stage": getattr(record, "stage", "unknown"),
            "status": getattr(record, "status", "unknown"),
            "event": getattr(record, "event", "unknown"),
            "message": record.getMessage(),
        }

        # 處理自訂屬性（排除保留字與空值）
        for key, value in record.__dict__.items():
            if key not in log_record and key not in self.RESERVED_ATTRS:
                if value not in (None, "", []):
                    log_record[key] = value

        return json.dumps(log_record, ensure_ascii=False)


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        merged = {**self.extra, **extra}  # log-specific takes precedence
        kwargs["extra"] = merged
        return msg, kwargs


def get_logger(service: str = "etl", logger_name: str = "etl_logger", stage: str = "local"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return ContextLoggerAdapter(
        logger,
        {
            "service": service,
            "stage": stage,
            "status": "ok",
        },
    )


# logging decorator
def log_step(stage: str = None):
    """
    Decorator for automatically logging function execution:
    - start / end events
    - duration
    - exception handling
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = kwargs.get("logger")
            if logger is None:
                raise ValueError("logger keyword argument required")

            # start log
            start_time = time.time()
            extra_start = {
                "stage": stage or getattr(logger, "stage", "unknown"),
                "event": "start",
                "status": "ok",
                "function": func.__name__,
            }
            logger.info(f"start {func.__name__}", extra=extra_start)

            try:
                result = func(*args, **kwargs)
                # end log
                duration = round(time.time() - start_time, 3)
                extra_end = {
                    "stage": stage or getattr(logger, "stage", "unknown"),
                    "event": "end",
                    "status": "ok",
                    "function": func.__name__,
                    "duration": duration
                }
                logger.info(f"{func.__name__} completed", extra=extra_end)
                return result

            except Exception as e:
                duration = round(time.time() - start_time, 3)
                extra_err = {
                    "stage": stage or getattr(logger, "stage", "unknown"),
                    "event": "end",
                    "status": "error",
                    "function": func.__name__,
                    "duration": duration,
                    "error_type": type(e).__name__,
                    "error_msg": str(e),
                    "traceback": traceback.format_exc()
                }
                logger.exception(f"{func.__name__} failed", extra=extra_err)
                raise
        return wrapper
    return decorator