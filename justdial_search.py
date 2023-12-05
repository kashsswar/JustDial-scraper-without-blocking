import time
import logging
from scrapy import Spider, Request, signals
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import os
import signal

class JustDialSpider(Spider):
    name = 'justdial'
    download_delay = 2
    crawler_process = None
    start_urls = ['https://www.justdial.com/Mumbai/Restaurants']
    scrapeops_headers = {
        'x-api-key': 'b497a621-cb51-4f88-9115-b89f6d0dd79d',
    }

    custom_settings = {
        'FEEDS': {
            'output.json': {
                'format': 'json',
                'overwrite': True,
            },
        },
    }

    def __init__(self, *args, **kwargs):
        super(JustDialSpider, self).__init__(*args, **kwargs)
        self.driver = self.create_webdriver()
        self.scraped_elements = set()
        self.max_items = 100  # Set the maximum number of items you want to scrape

    def create_webdriver(self):
        edge_options = webdriver.EdgeOptions()
        edge_options.use_chromium = True
        driver = webdriver.Edge(options=edge_options)
        return driver

    def start_requests(self):
        user_agent = UserAgent()
        headers = {
            'User-Agent': user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        headers.update(self.scrapeops_headers)

        for url in self.start_urls:
            yield Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        try:
            self.driver.get(response.url)
            self.handle_popups()
            max_scroll_attempts = 5

            while max_scroll_attempts > 0 and len(self.scraped_elements) < self.max_items:
                services = self.driver.find_elements(
                    By.CSS_SELECTOR, 'div.jsx-3349e7cd87e12d75.resultbox_info'
                )

                for service in services:
                    if service in self.scraped_elements:
                        continue

                    try:
                        item = {
                            'Name': self.get_text(service, 'div.jsx-3349e7cd87e12d75.resultbox_title_anchor.line_clamp_1'),
                            'Phone': self.get_phone_number(service),
                            'Rating': self.get_rating(service),
                            'Rating Count': self.get_rating_count(service),
                            'Address': self.get_text(service, 'div.jsx-3349e7cd87e12d75.font15.fw400.color111')
                        }

                        yield item
                        self.scraped_elements.add(service)

                    except Exception as e:
                        service_name = self.get_text(
                            service, 'div.jsx-3349e7cd87e12d75.resultbox_title_anchor.line_clamp_1')
                        logging.error(
                            f"Error processing service {service_name}: {e}")
                        continue

                self.scroll_down()
                new_elements_loaded = self.wait_for_new_elements()
                self.handle_popups()

                if new_elements_loaded:
                    max_scroll_attempts = 5
                else:
                    max_scroll_attempts -= 1

            # Stop the spider if the maximum number of items is reached
            if len(self.scraped_elements) >= self.max_items:
                self.crawler_process.stop()

        except Exception as e:
            logging.error(f"Error in parse function: {e}")

    def scroll_down(self):
        try:
            last_element = self.driver.find_elements(By.CSS_SELECTOR, 'div.jsx-3349e7cd87e12d75.resultbox_info')[-1]
            self.driver.execute_script("arguments[0].scrollIntoView();", last_element)
            time.sleep(2)
        except Exception as e:
            logging.error(f"Error while scrolling: {e}")

    def wait_for_new_elements(self):
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.jsx-3349e7cd87e12d75.resultbox_info'))
            )
            return True
        except Exception as e:
            logging.error(f"Error waiting for new elements: {e}")
            return False

    def handle_popups(self):
        try:
            popup_element = self.driver.find_element(By.CSS_SELECTOR, 'div.jsx-3eb89d42b60d6e6b.jd_modal_content.jd_modal_medium.modal_animation.fadeInUpTop')

            if popup_element.is_displayed():
                close_button = popup_element.find_element(By.CSS_SELECTOR, 'div.jsx-3eb89d42b60d6e6b.jd_modal_content.jd_modal_medium.modal_animation.fadeInUpTop span.jsx-3eb89d42b60d6e6b.jd_modal_close.jdicon')
                close_button.click()
                time.sleep(2)
        except Exception as e:
            pass

    def get_phone_number(self, service):
        try:
            phone_number_element = service.find_elements(
                By.CSS_SELECTOR, 'span.jsx-3349e7cd87e12d75.callcontent.callNowAnchor')

            if phone_number_element:
                phone_number = phone_number_element[0].text
            else:
                show_number_button = service.find_elements(
                    By.CSS_SELECTOR, 'span.jsx-3349e7cd87e12d75.callcontent')

                if show_number_button:
                    show_number_button[0].click()
                    phone_number_element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'ul.jsx-d17cc062da6c7a17 li.jsx-d17cc062da6c7a17 a.jsx-d17cc062da6c7a17.color111'))
                    )
                    phone_number = phone_number_element.text
                else:
                    phone_number = "N/A"

            return phone_number.strip() if phone_number else "N/A"

        except Exception as e:
            logging.error(f"Error in get_phone_number: {e}")
            return "N/A"

    def get_rating(self, service):
        try:
            rating_element = service.find_element(
                By.CSS_SELECTOR, 'div.jsx-3349e7cd87e12d75.resultbox_totalrate')
            rating = float(rating_element.text) if rating_element.text else 0.0
            return rating

        except Exception as e:
            logging.error(f"Error in get_rating: {e}")
            return 0.0

    def get_rating_count(self, service):
        try:
            count_element = service.find_element(
                By.CSS_SELECTOR, 'div.jsx-3349e7cd87e12d75.resultbox_countrate')
            count = int(count_element.text) if count_element.text else 0
            return count

        except Exception as e:
            logging.error(f"Error in get_rating_count: {e}")
            return 0

    def closed(self, reason):
        self.driver.quit()

    def close_spider(self, spider, reason):
        if self.crawler_process:
            self.crawler_process.stop()


    def get_text(self, parent_element, selector):
        try:
            element = parent_element.find_element(By.CSS_SELECTOR, selector)
            return element.text.strip() if element.text else "N/A"

        except Exception as e:
            logging.error(f"Error in get_text: {e}")
            return "N/A"

JustDialSpider.crawler_process = CrawlerProcess()

# Connect the spider to the CrawlerProcess
JustDialSpider.crawler_process.crawl(JustDialSpider)

# Connect the close_spider method to the spider_closed signal
# Connect the close_spider method to the spider_closed signal
crawler_instance = JustDialSpider.crawler_process.crawlers.pop()
crawler_instance.signals.connect(
    JustDialSpider.close_spider, signal=signals.spider_closed
)



# Start the CrawlerProcess
JustDialSpider.crawler_process.start()

# This blocks the script until the spider is finished
try:
    JustDialSpider.crawler_process.join()
except KeyboardInterrupt:
    # Handle keyboard interrupt (Ctrl+C) to stop the spider gracefully
    JustDialSpider.close_spider(None, "KeyboardInterrupt")

# Additional code to stop the script gracefully after spider execution
time.sleep(5)  # Wait for a short duration to allow the spider to finish

# Send a signal to gracefully stop the script
os.kill(os.getpid(), signal.SIGINT)