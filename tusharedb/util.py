import logging
import time


def speed_fun(minutes=1, max_cnt=200):
    cnt = 0
    start = None
    sum_ = 0

    def cal():
        sec = minutes * 60
        nonlocal cnt, start, sum_
        if start is None:
            start = time.time()
            return
        end = time.time()
        cnt += 1
        sum_ += (end - start)

        if cnt >= max_cnt:
            logging.debug('Start sleep... sleep time: %s', sec-sum_+1)
            time.sleep(sec-sum_ + 1)
            cnt = 0
            sum_ = 0
            start = None
            logging.debug('Stop sleep...')
        else:
            start = end

        if sum_ > sec:
            logging.debug('Reset without sleep')
            cnt = 0
            sum_ = 0
            start = None

        logging.debug('cnt: %s, sum_: %s', cnt, sum_)

    return cal


speed_it = speed_fun()

speed_20_per_min = speed_fun(max_cnt=20)
