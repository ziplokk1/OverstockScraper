__author__ = 'Mark'

from threading import Thread
import time
import logging
from random import uniform
from decimal import Decimal
from datetime import datetime, timedelta
from Queue import Queue
import argparse
import json
import re
from copy import deepcopy
import json

import requests
from BeautifulSoup import BeautifulSoup

from Database import DBInterface



MAX_OVERSTOCK_THREADS = 1


class SessionHandler(object):
    """
    Used to distribute the same session throughout multiple threads.
    """

    def __init__(self, sams_login=False):
        self.session = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_new_session()

    def request_new_session(self):
        self.logger.info('Requesting New Session')
        new_session = requests.Session()
        # spoof the user agent
        new_session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; WOW64) ' \
                                            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
        new_session.headers['Connection'] = 'keep-alive'
        new_session.headers['Accept'] = '*/*'
        self.session = new_session


class ScrapedItem(object):

    def __init__(self, sku, vendor):
        self.sku = sku
        self.price = Decimal(0)
        self.shipping = Decimal(0)
        self.product_id = None
        self.status = 'Out Of Stock'
        self.store = vendor
        self.option = None
        self.date_modified = datetime.now()

    def for_db(self):
        return self.sku, self.price, self.status, self.option, self.store, self.shipping, self.date_modified

    def __repr__(self):
        return '<%r Vendor: %r Sku: %r Option: %r>' % (self.__class__.__name__, self.store, self.sku, self.option)

    def __str__(self):
        return str(self.__repr__())

    def __dict__(self):
        return dict(
            sku=self.sku,
            price=float(self.price),
            shipping=float(self.shipping),
            product_id=self.product_id,
            status=self.status,
            store=self.store,
            option=self.option,
            date_modified=self.date_modified.strftime('%Y-%m-%dT%H:%M:%S')
            )

    def to_json(self):
        return json.dumps(self.__dict__())


class BaseScraper(Thread):

    def __init__(self, task_queue, end_queue, vendor, thread_id=0):
        """

        Base scraper thread class

        :param task_queue_dict: A dictionary with the vendor code as the key and a Queue as the value.
        :param end_queue: A queue to put data ready to be written to the database
        :param vendor: The vendor code (i.e. b, c, d, e, f, s, t2)

        :type task_queue_dict: dict[str, Queue]
        :type end_queue: Queue
        :type vendor: str
        :return:
        """
        Thread.__init__(self)
        self.queue = task_queue
        self.end_queue = end_queue
        self.vendor = vendor
        self.logger = logging.getLogger(self.__class__.__name__)
        self.thread_id = thread_id

    def run(self):
        while True:
            if DBInterface.STOP_ALL_THREADS:
                self.logger.error('Shutting Down Thread (%s-%d)...' % (self.vendor, self.thread_id))
                with self.queue.mutex:
                    self.queue.queue.clear()
                    break
            if self.queue.unfinished_tasks:
                task = self.queue.get()
                self.logger.debug('Dict(%s-%d) RemainingSize: %d CurTask: %s' % (self.vendor, self.thread_id,
                                                                                self.queue.qsize(), task))
                parsed_data_list = self.get_vendor_data(task)
                self.queue.task_done()
                for parsed_data in parsed_data_list:
                    self.end_queue.put(parsed_data)
                time.sleep(uniform(0.1, 3.5))  # Sleep for a random interval of time. Ideally you'd change this
                                               # to be a solid 3-5 seconds.
            else:
                break

    def get_vendor_data(self, task):
        """
        Override in subclass to process data. This method should only return the sku, price, status, option, store,
            shipping cost, and the date modified in a list format so that we can iterate through the list and
            put the parsed data into the finished queue to be written to the database
        :param task: The current task from the initial queue
        :return:
        """
        sku = '000000'
        price = Decimal(12.34)
        status = 'In Stock'  # / Out Of Stock
        option = '0'  # Sams 12, overstock 15, walmart 18, target 13, drugstore 15, costco 15
        store = self.vendor
        shipping = Decimal(0.00)
        date_modified = datetime.now()
        return [(sku, price, status, option, store, shipping, date_modified)]


class OverstockScraper(BaseScraper):

    OVERSTOCK_SEARCH_URL = 'http://www.overstock.com/search?keywords=%s&SearchType=Header'
    NO_RESULTS_PAGE = 'no-results-page'
    SEARCH_RESULTS_PAGE = 'search-nav-page'
    PRODUCT_PAGE = 'product-page'

    def __init__(self, start_queue, end_queue, thread_id=0):
        BaseScraper.__init__(self, start_queue, end_queue, 'o', thread_id)

    def get_vendor_data(self, task):
        item = ScrapedItem(task, 'o')

        # Continue to request a new session until the response code is 200
        while True:
            try:
                response = session_handler.session.get(self.OVERSTOCK_SEARCH_URL % task)
                if not response.status_code == 200:
                    self.logger.warn('Connection Status Code %d' % response.status_code)
                    session_handler.request_new_session()
                else:
                    break
            except requests.ConnectionError:
                self.logger.warn('Connection Error... Requesting New Session')
                session_handler.request_new_session()

        soup = BeautifulSoup(response.content)

        # Search for the page id
        # Only parse products if the page id is product page
        page_id = soup.find('div', {'class': 'page-wrapper'}).get('id')
        if page_id == self.NO_RESULTS_PAGE:
            self.logger.info('PageType: No Results - %s' % item)
            return [item]
        if page_id == self.SEARCH_RESULTS_PAGE:
            item.status = 'Check'
            self.logger.info('PageType: Search Results - %s' % item)
            return [item]

        price = self.get_price(soup)

        prod_id_pattern = re.compile('.*/([\d]+)/product\.html')
        item.product_id = prod_id_pattern.findall(response.url)[0]

        item.status = 'In Stock'

        dropdown_values = self.get_options(soup)

        return_vals = []

        # If the item has dropdown values then copy the default product and
        # set the option to the dropdown option and the price to the dropdown
        # price and append it to the db_products list for writing to the db
        if dropdown_values:
            self.logger.info('Dropdowns Found - %s' % item)
            # key as option and price as dropdown price
            for dropdown in dropdown_values:
                option_item = deepcopy(item)
                option_item.price = dropdown['price']
                option_item.option = dropdown['option']
                self.logger.info('Dropdown Product - %s' % item)
                return_vals.append(option_item)
        else:
            item.price = price
            return_vals.append(item)

        for product in return_vals:
            if product.price in ['PriceinCart', '']:
                self.logger.warn('No Valid Price For %s' % product)
                product.status = 'Check'
                product.price = Decimal(9999.99)

        return return_vals

    def get_options(self, soup):
        """
        Parse options if available
        :param soup: BeautifulSoup object of the product page
        :type soup: BeautifulSoup
        :return: Dictionary containing all options and prices
        :rtype: dict
        """

        # Find any tags that with the id attr like %addid%
        # The only tags with that attribute are the select options element
        option_dropdown = soup.find('select', {'id': re.compile(r'addid')})
        if not option_dropdown:
            logging.debug('No Dropdowns Found')
            return

        logging.debug('Dropdowns Found')

        # Get option tag values from the found select tag
        # and replace anything that's not whitespace, a period, or hyphen
        dropdown_values = [re.sub('[^\w^\.^-]', '', x.text)
                           for x in option_dropdown.findAll('option') if x.get('value') != ""]

        # Separate the option value at the rightmost hyphen, the right is the option value, and the left is the price
        # Merge them into a dictionary in the format {option: optionvalue, price: optionprice}
        return [dict(zip(('option', 'price'), (re.sub('\W', '', x.rpartition('-')[0])[:15],
                                               x.rpartition('-')[2]))) for x in dropdown_values]

    def get_price(self, soup):
        """
        Parse the product price from the supplied soup object
        :param soup: BeautifulSoup object of product page
        :type soup: BeautifulSoup
        :return: Item price
        :rtype: Decimal
        """
        price_element = soup.find('span', {'itemprop': 'price'})
        """:type price_element: BeautifulSoup"""

        # price element is normally none because it doesn't show the price in the HTML tag,
        # though the price can be parsed from a css class in the source, which is what the
        # following json parser is for
        if not price_element:
            pattern = re.compile(',defaultOption:\s+(\{.*?\})\s+,', re.DOTALL)

            # All of the following re.sub[s] are to make the javascript dictionary parsable with json
            price_content = pattern.findall(soup.prettify())[0]
            price_content = re.sub('\s+', '', price_content)
            price_content = re.sub('\{', '{"', price_content)
            price_content = re.sub(',', ', "', price_content)
            price_content = re.sub(':', '":', price_content)
            try:
                price = json.loads(price_content)['sellingPrice']
                price = re.sub('[^\d^\.]', '', price)  # Replace anything in the price text that's not a digit or period
                return Decimal(price)
            except (ValueError, KeyError) as f:
                self.logger.error(str(repr(f.args)))
                return Decimal(9999.99)

        price_text = price_element.text.replace('$', '').replace(',', '').strip()
        price = re.sub('[^\d^\.]', '', price_text)  # Replace anything in the price text that's not a digit or period
        return Decimal(price)


session_handler = SessionHandler()


if __name__ == "__main__":

    import sys

    start_time = datetime.now()

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--fout', help='File output mode', action='store_true')
    arg_parser.add_argument('--itemnumbers', nargs='+', help='Target specific item numbers')
    args = arg_parser.parse_args()

    #print args.store
    print 'Scraper Lib - Version 1.0'
    print 'Fileoutput: %s' % str(args.fout)

    logging.basicConfig(level=logging.ERROR)
    sq = Queue()
    for item_number in args.itemnumbers:
        sq.put(item_number)
    """:type queue_dict: dict[str, Queue]"""
    eq = Queue()

    t = DBInterface(sq, eq, args.fout)
    t.setDaemon(True)
    t.start()

    while not t.all_queues_populated:
        pass

    threads = []

    for i in range(MAX_OVERSTOCK_THREADS):
        o_thread = OverstockScraper(sq, eq, thread_id=i)
        o_thread.setDaemon(True)
        threads.append(o_thread)
        o_thread.start()

    while not t.STOP_ALL_THREADS:
        try:
            #sys.stdout.write('\r%s' % ' - '.join([': '.join((store, str(queue.unfinished_tasks))) for store, queue in queue_dict.iteritems()]))
            if not sq.unfinished_tasks:
                DBInterface.STOP_ALL_THREADS = True
            time.sleep(0.5)
        except KeyboardInterrupt:
            sys.stdout.write('\r\n')
            print 'SIGINT Received...'
            logging.info('SIGINT Received... Shutting Down...')
            DBInterface.STOP_ALL_THREADS = True

    for thread in threads:
        thread.join()
        sq.join()

    t.logger.info('Shutting Down...')
    print ('Shutting Down...')
    t.join()
    t.out_file.flush()
    t.out_file.close()
    logging.info('Done')
    print 'Done'

    end_time = datetime.now()

    time_diff = (end_time - start_time)
    seconds = time_diff.seconds
    hours = seconds // 3600
    seconds -= (hours * 3600)
    minutes = seconds // 60
    seconds -= (minutes * 60)

    logging.info('Delta Time: %s:%s:%s' % (hours, minutes, seconds))
    print 'Delta Time: %s:%s:%s' % (hours, minutes, seconds)

