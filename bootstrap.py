from redis import Redis
from rq import Queue
from task import crawl
import services
from redis import Redis
import time
import sys
from optparse import OptionParser


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-u", "--url", dest="root_url", help="Specify the URL")
    (options, args) = parser.parse_args()
    if options.root_url is not None:
        rq = Queue(connection=Redis())
        root_url = options.root_url
        link_id = services.create_link(-1, root_url)
        rq.enqueue(crawl, link_id)
