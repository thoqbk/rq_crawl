import services
import time

PERIOD = 10

while True:
    links = services.count_links()
    time.sleep(PERIOD)
    new_links = services.count_links()
    print '%s link(s) per %s seconds' % (new_links - links, PERIOD)
