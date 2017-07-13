from redis import Redis
from rq import Queue
import services
from lxml.html import parse
import logging
import traceback
import re

logging.basicConfig(filename='./log/rq.log', level=logging.INFO)

rq = Queue(connection=Redis())


def crawl(id):
    link = services.get_link(id)
    logging.info('Crawling %s|%s' % (id, link))
    if link is None or link['status'] == services.DONE:
        return
    # Else:
    try:
        dom = parse(link['url']).getroot()
        new_urls = _get_new_urls(dom)
        logging.info('Found %s new url(s) in %s' % (len(new_urls), link['url']))
        for url in new_urls:
            try:
                link_id = services.create_link(id, url)
                rq.enqueue(crawl, link_id)
            except Exception as e:
                logging.error('Create link failed %s. %s, %s' % (url, str(e), traceback.format_exc()))

        # Update:
        services.update_link(id, _get_title(dom), services.DONE)
    except Exception as e:
        services.update_link(id, '', services.FAIL)
        logging.error('Crawl link fail %s|%s. Reason: %s, %s' % (
            id, link['url'], str(e), traceback.format_exc()
        ))


# Utils ------------------------------------------------------------------------
def _get_title(dom):
    titles = dom.cssselect('title')
    if titles is None or len(titles) == 0:
        return None
    # Else:
    title = titles[0]
    return title.text


def _get_new_urls(dom):
    links = dom.cssselect('a')
    urls = []
    for link in links:
        url = link.get('href', None)
        if _is_valid_http_url(url):
            urls.append(url)

    if len(urls) == 0:
        return []

    # Else:
    return services.filter_new_urls(urls)


def _is_valid_http_url(url):
    if url is None or len(url) == 0:
        return False
    match = re.match(r'^(http|https)\:', url.lower())
    if match is None or len(match.groups()) == 0:
        return False
    return True
