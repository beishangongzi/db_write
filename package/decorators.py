## 计时装饰器
import time


def cal_time(file):
    def decorator(func):
        def wrapper(*args, **kw):
            start = time.time()
            res = func(*args, **kw)
            end = time.time()
            with open(file, "a+", encoding="utf-8") as f:
                f.write(res)
                f.write("-----" + (end-start).__str__() + "\n")
            return
        return wrapper
    return decorator