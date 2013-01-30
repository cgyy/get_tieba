# -*- coding: utf-8 -*- 

import sys
import re
import json
import urllib
import urlparse
import traceback
from tornado import httpclient, ioloop, database
from lxml import etree
from lxml.cssselect import CSSSelector as css
from lxml.html import tostring
#from database_sqlite import Connection

class handle_request(object):
    def __init__(self, func_parser, exit_flag=False):
        self.func_parser = func_parser
        self.exit_flag = exit_flag

    def __call__(self, response):
        if response.error:
            print "Error:", response.error
        else:
            content_type = response.headers["Content-Type"]
            charset = re.match(".+charset=(.+)", content_type).group(1)

            try:
                html = response.body.decode(charset)
            except UnicodeDecodeError:
                if charset.lower() == "gbk":
                    try:
                        html = response.body.decode("gb18030", "ignore")
                    except:
                        print "Decode error"
                        html = None

            if self.exit_flag:
                ioloop.IOLoop.instance().stop()

            if html:
                return self.func_parser(html)


class PageItem(object):
    regex_num = re.compile(".+/p/(\d+)")

    def __init__(self, url, reply_date):
        self.url = url
        self.reply_date = reply_date
        self.page_num = self._page_num()

    def parse_detail_page(self, html):
        core_selector = css("div.core")
        doc = etree.HTML(html)

        core_div = core_selector(doc)
        if core_div:
            content_list = core_div[0]
        else:
            return

        post_contents = css("div.l_post")(content_list)
        main_author = None
        contents = []
        created_at = None
        for div in post_contents:
            dumped_data = div.get("data-field")
            try:
                data = json.loads(dumped_data)
            except:
                print "Parse json error"
                continue

            _content = data["content"]
            _author = data["author"]
            user_id = _author.get("id") or _author.get("name")
            main_author = user_id if main_author is None else main_author
            created_at = _content["date"]

            if main_author != user_id:
                break

            post_content = css("cc div.d_post_content")(div)[0]
            current_content = tostring(post_content, encoding="UTF-8", method="text")
            contents.append(current_content)

        self.title = css(".core_title_txt")(content_list)[0].text.encode("UTF-8")
        self.content = "\n\n".join(contents)
        self.created_at = created_at
        self.save_to_sqlite()

    def save_to_sqlite(self):
        print self.title
        PageItem.db.execute("insert into page_items(page_num, title, content, created_at) values(%s,%s,%s,%s)", 
                             self.page_num, self.title, self.content, self.created_at)

    def _page_num(self):
        return int(PageItem.regex_num.match(self.url).group(1))


    @classmethod 
    def init_sqlite(cls):
        cls.db = database.Connection("127.0.0.1", "test")
        try:
            migrate_str = """
CREATE TABLE page_items (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `keyword` varchar(255) DEFAULT NULL,
    `page_num` int(11) DEFAULT NUll,
    `title` text DEFAULT NULL,
    `content` text DEFAULT NUll,
    `created_at` DATETIME NULL,
    `reply_date` DATETIME NULL,
    `active` tinyint(1) DEFAULT '0',
    PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=12 DEFAULT CHARSET=utf8;
            """
            cls.db.execute(migrate_str)
        except:
            pass
            #traceback.print_exc()
    

def parse_summary_page(html):
    doc = etree.HTML(html)
    li_elms = css("li.j_thread_list")(doc)


    base_url = "http://tieba.baidu.com%s" 
    link_items = []
    for li in li_elms:
        link = css("a.j_th_tit")(li)[0]
        href = base_url % link.get('href')
        reply_date = css("span.j_reply_data")(li)[0]
        link_items.append(PageItem(href, reply_date))

    next_page_links = css("a.next")(doc)
    if next_page_links:
        next_link = base_url % next_page_links[0].get("href")
    else:
        next_link = None

    return (next_link, link_items)


def get_keyword(raw_word):
    gbk_str = raw_word.decode("UTF-8").encode("GBK")
    return urllib.quote(gbk_str)

def async_fetch(page_items):
    if not page_items: return

    async_client = httpclient.AsyncHTTPClient()
    for idx, page_item in enumerate(page_items):
        exit_flag = len(page_items) == idx + 1
        try:
            async_client.fetch(page_item.url, handle_request(page_item.parse_detail_page, exit_flag))
        except:
            print "fetch error"

    ioloop.IOLoop.instance().start()



def fetch_summary_page(page_url):
    sync_client = httpclient.HTTPClient()
    response = sync_client.fetch(page_url)

    hander = handle_request(parse_summary_page)
    return hander(response)


def main():
    args = sys.argv
    if len(args) > 1:
        keyword = args[1]
    else:
        print "Keyword is blank!!"
        keyword = "java"
        #sys.exit(1)

    PageItem.init_sqlite()
    main_url = "http://tieba.baidu.com/f/good?kw=%s" % get_keyword(keyword)

    next_link, detail_items = fetch_summary_page(main_url)
    while True:
        async_fetch(detail_items)
        print next_link

        if next_link:
            next_link, detail_items = fetch_summary_page(next_link)
        else:
            break
     


if __name__ == "__main__":
    main()
