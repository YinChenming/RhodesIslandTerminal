from terminal import Terminal, LOG_FILE
import logging

logging.basicConfig(filename=LOG_FILE, filemode="a", encoding="utf-8", datefmt="%y.%m.%d %H:%M:%S",
                    level=logging.DEBUG,
                    format="%(lineno)d|%(asctime)s|%(levelname)s|%(name)s-%(threadName)s: %(message)s")
Terminal().menu_index()
