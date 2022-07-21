import datetime
import json
import logging
import os
import sys
import warnings
from urllib.parse import urlparse

import requests
import yaml
from bs4 import BeautifulSoup
from newsplease import NewsPlease
from playhouse.db_url import connect
from playhouse.migrate import *
from rake_new2 import Rake

# https://github.com/fhamborg/news-please
# python -m nltk.downloader stopwords
# python -m nltk.downloader punkt

logging.basicConfig(format="[%(asctime)s][%(levelname)s][%(filename)s] %(message)s", level=logging.INFO)

warnings.filterwarnings("ignore")
db = connect("sqlite:///news_spider.db")
db.close()


class BaseModel(Model):
    class Meta:
        database = db


class Link(BaseModel):
    link = CharField(max_length=1024, null=False)
    date = DateTimeField()
    tags = CharField(max_length=4096, null=False)


def load_config() -> dict:
    """
    Load config and return it as a dict
    :return: dict
    """

    with open("config.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as ex:
            logging.error(ex)
            sys.exit(1)


def fetch(site_url):
    """
    Fetch all news from the url page
    :param site_url: 'https://www.cbc.ca/news/canada/montreal' for example
    :return:
    """

    url_parse = urlparse(site_url)
    url_path = url_parse.path
    url_title = url_parse.netloc.rstrip("/")
    url_site = "{}://{}".format(url_parse.scheme, url_parse.netloc.rstrip("/"))
    logging.info("Processing site: {}".format(url_site))

    try:
        page = requests.get(site_url)
    except Exception as ex:
        logging.error(ex)
        sys.exit(1)

    page_parsed = BeautifulSoup(page.text)
    res = page_parsed.find_all("a")

    # we will use Rake to get keywords from the article for the ML thingy someday
    rk = Rake()

    links = []
    for item in res:
        href = item.get("href")
        if url_path in href:
            if href != url_path and href not in links:
                links.append(href)

    # old news are on the bottom of the page
    for link in reversed(links):
        site_link = "{}/{}".format(url_site, str(link).lstrip("/"))

        # let's check the history
        history = Link.get_or_none(link=site_link)
        if history:
            logging.info("Site link was already downloaded: {}".format(site_link))
            continue

        try:
            page = NewsPlease.from_url(site_link)
        except Exception as e:
            logging.error(e)
            continue

        date_utc_now = datetime.datetime.utcnow()
        date_delta = date_utc_now - page.date_publish

        if date_delta.days > 1:
            Link.create(link=site_link, date=page.date_publish, tags="to_old")
            logging.info("Site link is too old: {}".format(site_link))
            continue

        if len(page.maintext) < 512:
            Link.create(link=site_link, date=page.date_publish, tags="to_small")
            logging.info("Site link is too small: {}".format(site_link))
            continue

        logging.info("Processing the link: {}".format(site_link))
        rk.get_keywords_from_raw_text(page.maintext)
        kw_s = rk.get_kw_degree()

        keywords = []
        for k, v in kw_s.items():
            if v > 3 and len(k) >= 5:
                keywords.append("{}_{}".format(k, v))

        # save to the database
        Link.create(link=site_link, date=page.date_publish, tags=",".join(keywords))
        logging.info("Site was saved to the database: {}".format(site_link))

        push_news(
            news_url=site_link,
            date=page.date_publish,
            title=page.title,
            description=page.description,
            url_title=url_title,
        )


def push_news(news_url: str, date: datetime, title: str, description: str, url_title: str):
    """
    Push news to the telegram bot

    :param news_url: link to the news
    :param date: date of the news
    :param title: title
    :param description: short description
    :param url_title: the link title
    :return:
    """

    bot_token = os.environ.get("TELEGRAM_TOKEN", None)
    bot_chat_id = os.environ.get("TELEGRAM_CHAT", 0)

    if not bot_token or bot_chat_id == 0:
        logging.error("No telegram token or chat ID found")
        return

    message = """<a href="{}">{}</a>: <strong>{}</strong>
    
<i>{}</i>

date: {}
    """.format(
        news_url, url_title, title, description, str(date)
    )

    params = {"chat_id": bot_chat_id, "text": message, "parse_mode": "HTML"}

    telegram_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    result = requests.post(url=telegram_url, data=json.dumps(params), headers={"Content-Type": "application/json"})
    logging.info("response: {}".format(result.json()))


def init_database():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        db.connect()
        db.create_tables(
            [
                Link,
            ]
        )
        db.close()


if __name__ == "__main__":
    init_database()
    cfg = load_config()
    urls = cfg.get("news_spider", {}).get("urls", [])

    for url in urls:
        fetch(url)
