import platform
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Used to get the Selenium Web Driver Object based on the environment
class SeleniumWebDriver():

    def __init__(self, debugFlag):
        self.debugFlag = debugFlag
        self.driver = None

    def getWebDriverObj(self):

        platformType = platform.system()
        architecture = platform.architecture()[0]
        filepath = os.path.dirname(os.path.abspath(__file__))


        if platformType == "Windows":
            driverLocation = "C:/"
            execpath = driverLocation + "chromedriver/chromedriver.exe"
            prefs = {"download.default_directory": "C:/Users/Kumar.DESKTOP-Q2F04SK/Downloads"}
            options = Options()
            options.add_experimental_option("prefs", prefs)
            if self.debugFlag is False:
                options.add_argument('--headless')
            self.driver = webdriver.Chrome(execpath,chrome_options=options)
        elif platformType == "Darwin":
            driverLocation = "/users/"
            execpath = driverLocation + "chromedriver/chromedriver"
            prefs = {"download.default_directory": "/users/akaiyer/Downloads"}
            options = Options()
            options.add_experimental_option("prefs", prefs)
            if self.debugFlag is False:
                options.add_argument('--headless')
            self.driver = webdriver.Chrome(execpath, chrome_options=options)
        elif platformType == "Linux" and architecture == "64bit":
            driverLocation = "/tmp/"
            execpath = driverLocation + "chromedriver/chromedriver"
            prefs = {"download.default_directory": "/home/kaiyer/Downloads"}
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_experimental_option("prefs", prefs)
            if self.debugFlag is False:
                options.add_argument('--headless')
            self.driver = webdriver.Chrome(executable_path=execpath, chrome_options=options)
        elif platformType == "Linux" and architecture == "32bit":
            driverLocation = "/tmp/"
            execpath = driverLocation + "chromedriver/chromedriver"
            prefs = {"download.default_directory": "/home/kaiyer/Downloads"}
            options = Options()
            options.add_argument('--no-sandbox')
            options.add_experimental_option("prefs", prefs)
            if self.debugFlag is False:
                options.add_argument('--headless')
            self.driver = webdriver.Chrome(executable_path=execpath, chrome_options=options)
        return self.driver
