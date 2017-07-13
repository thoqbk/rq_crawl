import MySQLdb
from datetime import datetime
import logging
import traceback

logging.basicConfig(filename='./log/rq.log', level=logging.INFO)

HOST = '127.0.0.1'
PORT = 3306
USERNAME = 'root'
PASSWORD = 'root'
DB = 'rq'

FAIL = 'fail'
DONE = 'done'
PENDING = 'pending'


def create_link(parent_id, url):
    ret_val = -1
    connection = _get_connection()
    try:
        insert_string = 'INSERT INTO rq_link(parent_id, url, status, create_time) VALUES(%s, %s, %s, %s)'
        cursor = connection.cursor()
        cursor.execute(insert_string, (parent_id, url, PENDING, datetime.now()))
        ret_val = cursor.lastrowid
        connection.commit()
    except Exception as e:
        connection.rollback()
        logging.error('Create link failed %s|%s|%s|%s' % (
            parent_id,
            url,
            str(e),
            traceback.format_exc()
        ))
    finally:
        connection.close()

    return ret_val


def get_link(id):
    """
    :return dict of {
        id,
        url,
        status
    }
    """
    ret_val = None
    connection = _get_connection()
    try:
        select_string = 'SELECT id, url, status FROM rq_link WHERE id = %s'
        cursor = connection.cursor()
        cursor.execute(select_string, (id,))
        rows = cursor.fetchall()
        if rows is not None and len(rows) > 0:
            row = rows[0]
            ret_val = {
                'id': row[0],
                'url': row[1],
                'status': row[2]
            }
    finally:
        connection.close()

    # return
    return ret_val


def update_link(id, title, status):
    connection = _get_connection()
    try:
        update_string = 'UPDATE rq_link set status = %s, title = %s WHERE id = %s'
        cursor = connection.cursor()
        cursor.execute(update_string, (status, title, str(id)))
        connection.commit()
    except Exception as e:
        connection.rollback()
        logging.error('Update link failed %s|%s' % (
            str(e), traceback.format_exc()
        ))
    finally:
        connection.close()


def count_links():
    """
    :return number of 'done' links
    """
    connection = _get_connection()
    try:
        select_string = 'SELECT COUNT(*) as COUNT FROM rq_link WHERE status = %s'
        cursor = connection.cursor()
        cursor.execute(select_string, (DONE, ))
        rows = cursor.fetchall()
        if rows is not None and len(rows) > 0:
            row = rows[0]
            return int(row[0])
    finally:
        connection.close()


def filter_new_urls(urls):
    """
    :return list of not exist urls
    """
    ret_val = []
    old_urls = []
    connection = _get_connection()
    cursor = None
    try:
        select_string = 'SELECT url FROM rq_link WHERE url IN (%s)' % ', '.join(list(map(lambda x: '%s', urls)))
        cursor = connection.cursor()
        cursor.execute(select_string, urls)
        rows = cursor.fetchall()
        for row in rows:
            old_urls.append(row[0].lower())
        for url in urls:
            lower_url = url.lower()
            if lower_url not in old_urls and lower_url not in ret_val:
                ret_val.append(lower_url)
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()

    # return
    return ret_val


# Utils ------------------------------------------------------------------------
def _get_connection():
    return MySQLdb.connect(
        host=HOST, user=USERNAME,
        port=PORT, passwd=PASSWORD, db=DB)
