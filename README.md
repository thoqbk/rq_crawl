# [RQ Crawl]

Using RQ (Redis Queue) to crawl links and titles

## Dependencies

* **rq:** `pip install rq`
* **lxml:** `pip install lxml`
* **cssselect:** `pip install cssselect`

## Get started

* **Step 1:** Create mysql database using schema.sql
* **Step 2:** Update DB config in `services.py` and `root_url` in `bootstrap.py`
* **Step 3:** Run `bootstrap.py` to initialize crawling job: `python bootstrap.py`
* **Step 4:** Start one or more workers by running `rq worker` in `rq_crawl` directory
* **Step 5:** Run `python count.py` to view crawling speed
