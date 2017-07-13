from redis import Redis
from rq import Queue
from task import crawl
import services
from redis import Redis
import time

rq = Queue(connection=Redis())

root_url = 'http://edition.cnn.com/'

link_id = services.create_link(-1, root_url)

rq.enqueue(crawl, link_id)
