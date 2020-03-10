import logging

# 日志配置，输出日志同时写入文件
logging.basicConfig(
    #level=logging.DEBUG,
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        #logging.FileHandler("{}.log".format('toutiao_log')),
        logging.StreamHandler()
    ])

logger = logging.getLogger()
