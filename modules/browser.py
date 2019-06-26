"""
This module is meant to provide a basic chrome web browser in selenium.
Look for more reference in JD's bi-module project, and my work drive Scripts, Auto Updates directory.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from os import path
from os import environ as e
import json


class BrowserBase:
    delay = 20
    websites = {
        # todo: Map the most commonly used selectors to navigate the individual websites.
        "Gmail": {
            "Start_address": "https://mail.google.com/mail/u/0/#inbox",
            "Inbox_css_selector": "xyz"
        },
        "Mercury Account List": {
            "Start_address": "https://mercury.vivintsolar.com/#!/accounts/Account-List",
            "First_Account_css_selector": "xyz"
        },
        "Luna": {
            "Start_address": "http://luna.vivintsolar.com/calculators/system_performance_calculator/",
            "Sidebar_css_selector": "xyz"
        },
        "Holds Report": {
            "Start_address": "https://vivintsolar.my.salesforce.com/00O41000008r5c1?cancelURL=%2F00O41000008r5c1",
            "Export_css_selector": "xyz"
        },
        "TAT Report": {
            "Start_address": "https://vivintsolar.my.salesforce.com/00O1M000007qNaJ?cancelURL=%2F00O1M000007qNaJ",
            "Export_css_selector": "xyz"
        },
        "Mercury Query": {
            "Start_address": "https://mercury.vivintsolar.com/#!/analytics/performance/query/dialog",
            "Account_population_css_selector": "xyz"
        },
        "ShareFile": {
            "Start_address": "https://leasedimensions.sharefile.com",
            "Filings_css_selector": "xyz"
        },
        "Uberconference": {
            "Start_address": "https://uberconference.com/tyleranderson1",
            "Share_screen_css_selector": "xyz"
        }
    }

    def __init__(self,
                 driver_type='chrome',
                 download_directory=path.join(e['userprofile'], 'downloads'),
                 headless=False):
        self.skip = False
        self.headless = headless
        self.driver_type = driver_type.lower()
        self.download_directory = download_directory
        self.console_output = False
        self.driver = None
        self.options = None
        self.active = False
        # List of values dependant on system environment variables
        self.chrome_profile_path = ''
        self.chromedriver_exe = ''
        self.chrome_exe = ''

    def enable_console_output(self):
        self.console_output = True

    def _start_driver(self):
        if self.driver_type == 'chrome':
            self.create_chrome_driver()

    def _populate_system_envs(self):
        selenium_json = json.loads(e['SELENIUM'])
        self.chrome_exe = selenium_json['canary_exe']
        self.chrome_profile_path = selenium_json['canary_profile']
        self.chromedriver_exe = selenium_json['chromedriver_exe']

    def new_tab(self):
        if self.driver:
            self.driver.execute_script('''window.open("about:blank", "_blank");''')

    def focus_window_number(self, num):
        if self.driver:
            num = int(num)
            self.driver.switch_to.window(self.driver.window_handles[num])
            bod = self.driver.find_element_by_tag_name('body')
            bod.click()

    def create_chrome_driver(self):
        self._populate_system_envs()
        self.options = Options()

        preferences = {
            'profile.default_content_settings.popups': 0,
            'download.prompt_for_download': False
        }

        if self.download_directory:
            preferences['download.default_directory'] = self.download_directory + '\\'
            preferences['download.directory_upgrade'] = True

        self.options.add_experimental_option('prefs', preferences)
        self.options.add_argument('disable-infobars')
        # Force chromedriver to run canary
        self.options.binary_location = self.chrome_exe

        if self.headless:
            self.options.add_argument('headless')

        self.driver = webdriver.Chrome(executable_path=self.chromedriver_exe, options=self.options)

    def delete_cookies(self):
        # Create the Driver and Delete all cookies
        self.driver.delete_all_cookies()

    def end_crawl(self):
        if self.console_output:
            print('Driver Tasks Complete')
        self.driver.quit()
        self.active = False
