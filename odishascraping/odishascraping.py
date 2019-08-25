# -*- coding: utf-8 -*-
"""
Created on March 2019

@author: anurag aiyer
"""
import logging
import sys
from timeit import default_timer as timer
import time
from threading import Thread
from Queue import Queue
import argparse
import os
# import urllib
import time
import traceback
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
# from time import sleep
from bs4 import BeautifulSoup
import csv
import numpy as np
import pandas as pd
# import urllib2
# from bs4 import BeautifulSoup
import codecs
import re
import pandas as pd
from tabulate import tabulate

from SeleniumWebDriver import *
from helpers import *

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from collections import defaultdict

class OdishaRccmsScraper:
    def __init__(self, debugType=False, scrapeTopLevel=False, scrapeLowerLevel=False, threadsOn=False ):
        # lower level scraping
        self.threadsOn =threadsOn
        self.driverPool = None
        self.numThreads = 1
        self.debugType = debugType
        self.driver = None
        # set local directory
        platformType = platform.system()
        if platformType == "Windows":
            self.directory = "C:/Users/Kumar.DESKTOP-Q2F04SK/Downloads"
        elif platformType == "Darwin":
            self.directory = "/Users/anurag/Dropbox/UC Berkeley/IEOR/Spring 2019/URAP PoliSci Indian SocioEconomic Status"
        elif platformType == "Linux":
            self.directory = "/home/kaiyer/Downloads"
        os.chdir(self.directory)
        # website wait timing limits
        self.waitSecs1 = 1
        self.waitSecs2 = 20
        self.waitSecs3 = 5

        # website to scrape
        self.url = "http://bhulekh.ori.nic.in/rccms/Dashboard.aspx"
        if (scrapeTopLevel==True):
            self.runTopLevelScraping()
        if (scrapeLowerLevel==True):
            self.runLowerLevelScraping()
        return


    def __del__(self):
        if (scrapeTopLevel == True):
            if self.driver is not None:
                self.driver.close()
        if (scrapeLowerLevel == True):
            for driver in self.driverPool:
                driver.close()

    @retry(TimeoutException, tries=3, delay=2)
    def waitForMonthYearCaseAndCourtLabel(self, driver, mnth, yr, caseName, courtName, currMonth, currDay):
        if (mnth == currMonth):
            monthAndYear = currDay + " / " + currMonth + " / " + yr
        else:
            monthAndYear = mnth + " " + yr
        lbl = caseName + " cases in the court of " + courtName + " as on " + monthAndYear
        #logging.info("waiting for " + lbl)
        WebDriverWait(driver, self.waitSecs2).until(
            EC.text_to_be_present_in_element((By.XPATH,
                                              '//*[@id="ctl00_ContentPlaceHolder1_lblheadertext"]'),
                                             lbl))

    @retry(TimeoutException, tries=3, delay=10)
    def waitForDashboard(self, driver):
        WebDriverWait(driver, self.waitSecs2).until(
            EC.visibility_of_element_located((By.XPATH, "//*[@id='ctl00_ContentPlaceHolder1_grddashboard']")))

    def courtSelect(self, driver):
        driver.get(self.url)
        ####select court type
        court_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$drpCourtType"))
        court_list = []
        court_names = []

        # Get the court names and values
        for y in court_select.options:
            court_list.append(str(y.get_attribute("value")))
            court_names.append(str(y.get_attribute("text")))
        ##set court to tehsildar
        try:
            ndx = court_names.index("Tahasildar")
            court_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$drpCourtType"))
            court_select.select_by_value(court_list[ndx])
        except StaleElementReferenceException:
            print("Court_Exception1")
            pass
        except WebDriverException:
            print("Court_Exception2")
            pass

        return court_list, court_names, ndx

    def runTopLevelScraping(self):
        # get selenium driver
        selenium = SeleniumWebDriver(debugFlag=self.debugType)
        self.driver = selenium.getWebDriverObj()
        ##Get all the months from the website and then for 2018
        ##press View for every month and then
        ##get all the links (for all the districts)
        ##press each district and download Mutation data for each tehsil
        ##repeat this process for OLR8a and OLR 19(1)c
        # select Tahashildar as courtType

        self.court_list, self.court_names, self.court_ndx = self.courtSelect(self.driver)
        self.waitForDashboard(self.driver)
        self.currMonthLbl, self.currDay, self.currMonth = self.getLatestMonthYear(self.driver)
        self.case_list, self.case_names = self.initCaseType(self.driver)
        self.topdf, self.topSummaryDf = self.initYearMoCase(self.driver, self.court_ndx, self.case_list, self.case_names, self.court_names,
                            saveData=True,
                            currMonth=self.currMonth, currDay=self.currDay)
        # write top level results
        self.topdf = self.topdf.apply(lambda x: x.replace(',', ''))
        self.topdf.to_csv(self.directory + "/RCCMS_Overall.csv",
                          header=True, index=False, encoding='utf-8')
        # now save the urllinks
        self.topSummaryDf = self.topSummaryDf.apply(lambda x: x.replace(',', ''))
        self.topSummaryDf.drop_duplicates(inplace=True)
        self.topSummaryDf.to_csv(self.directory + '/rccms_url.csv',
                          header=True, index=False, encoding='utf-8')



    def runLowerLevelScraping(self):
        self.district_stack = []
        self.readUrlLinks()
        if (self.threadsOn):
            self.numThreads = 16
        else:
            self.numThreads = 1
            # thread specific web processors
        numthreads = min(self.numThreads, len(self.district_stack))
        self.driverPool = [SeleniumWebDriver(debugFlag=self.debugType).getWebDriverObj()
                           for _ in range(numthreads)]
        self.q = Queue(maxsize=0)
        i = 0
        while len(self.district_stack) > 0:
            self.q.put((i, self.district_stack.pop()))
            i = i + 1
        # now start threads and process
        for j in range(numthreads):
            logging.info('Starting thread %d', j)
            worker = Thread(target=self.crawl, args=(self.q, j))
            worker.setDaemon(True)  # setting threads as "daemon" allows main program to
            # exit eventually even if these dont finish
            # correctly.
            worker.start()
            # now we wait until the queue has been processed
        self.q.join()

        logging.info('All tasks completed.')


        return

    def getMoList(self,driver):
        month_list = []
        month_names = []
        month_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$dllmonth"))
        for y in month_select.options:
            month_list.append(str(y.get_attribute("value")))
            month_names.append(str(y.get_attribute("text")))
        return [month_list, month_names]

    def crawl(self, q, threadno):
        while not q.empty():
            try:
                # get selenium driver handle
                driver = self.driverPool[threadno]
                driver.get(self.url)
                work = q.get()  # fetch new work from the Queue
                itemnum = work[0]
                #unpack list
                logging.info(work[1])
                court_name, district_name = work[1]
                court_list, court_names, court_ndx = self.courtSelect(driver)
                self.waitForDashboard(driver)
                currMonthLbl, currDay, currMonth = self.getLatestMonthYear(driver)
                case_list, case_names = self.initCaseType(driver)
                # then activate link to tashil portal
                # get current window handle
                # store current window for backup to switch back
                xpath1 = "//a[contains(., " + '"' + district_name + '")]'
                elem = driver.find_element_by_xpath(xpath1)
                elem.click()
                cnt = 0
                while True:
                    time.sleep(self.waitSecs1)
                    numWindows = len(driver.window_handles)
                    cnt = cnt + 1
                    if (numWindows == 2 or cnt == 10):
                        break
                # now switch to window
                driver.switch_to.window(driver.window_handles[1])
                # now wait for page
                self.waitForDashboard(driver)
                resultsDf, summaryDf = self.initYearMoCase(driver, court_ndx, case_list,
                                                                    case_names, court_names,
                                                                    saveData=True,
                                                                    currMonth=currMonth, currDay=currDay, district_name=district_name)
                # write results
                resultsDf = resultsDf.apply(lambda x: x.replace(',', ''))
                resultsDf.to_csv(self.directory + "/RCCMS_" + district_name + ".csv",
                                  header=True, index=False, encoding='utf-8')
                # close window
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

            except TimeoutException as e:
                # logging.error(str(e))
                # traceback.print_exc()
                sys.exc_clear()
                pass
            except Exception as e:
                # dont make thread fail
                # log exception and continue on
                logging.error(str(e))
                traceback.print_exc()
                sys.exc_clear()
                pass
            q.task_done()


    def initCaseType(self,driver):
        case_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$drpCaseType"))
        case_list = []
        case_names = []

        # Get the month names and values
        for y in case_select.options:
            case_list.append(str(y.get_attribute("value")))
            case_names.append(str(y.get_attribute("text")))
        return case_list, case_names

    def loadYearList(self,driver):
        year_list = []
        year_names = []
        year_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$ddlYear"))
        # Get the month names and values
        for y in year_select.options:
            year_list.append(str(y.get_attribute("value")))
            year_names.append(str(y.get_attribute("text")))
        return year_list, year_names

    def initYearMoCase(self, driver, court_ndx, case_list, case_names, court_names, saveData=False,
                    currMonth=None, currDay=None, district_name=None):
        resultDf = self.initResultsDataFrame()
        summaryDf = self.initSummaryDataFrame()
        ####select year
        year_list = []
        year_names = []

        year_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$ddlYear"))
        # Get the month names and values
        for y in year_select.options:
            year_list.append(str(y.get_attribute("value")))
            year_names.append(str(y.get_attribute("text")))
        # if year_name is given use that
        # redo year_list based on new year_names

        try:
            for yr in year_list:
                yr_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$ddlYear"))
                yr_select.select_by_value(yr)
                time.sleep(self.waitSecs2)
                ####select month for given year
                month_list, month_names = self.loadMonthList(driver)
                movalue = month_list[0]
                casevalue = case_list[0]
                # now load January to trigger
                self.chooseMonthSelect(driver,movalue)
                # now handle the ALL case and wait
                self.waitForResultsToLoad(driver, yr, movalue, casevalue, court_ndx, year_list, month_list, case_list,
                                          year_names, month_names, case_names, court_names, currMonth, currDay)
                # now reload list
                month_list, month_names = self.loadMonthList(driver)
                for mo in month_list:
                    self.chooseMonthSelect(driver, mo)
                    for cs in case_list:
                        case_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$drpCaseType"))
                        case_select.select_by_value(cs)
                        # wait for response from webserver and examine the label
                        self.waitForResultsToLoad(driver, yr, mo, cs, court_ndx, year_list, month_list, case_list,
                                                  year_names, month_names, case_names, court_names, currMonth, currDay)
                        html = driver.page_source
                        soup = BeautifulSoup(html, 'lxml')

                        table = soup.find('table', {'id': 'ctl00_ContentPlaceHolder1_grddashboard'})
                        self.rows = table.findAll('tr')
                        if saveData:
                            summaryDf = self.updateSummaryDf(court_ndx, court_names, summaryDf)
                            # now extract base data
                            # read and print the entire table
                            dfs = pd.read_html(str(table))
                            # print(tabulate(dfs[0],headers='keys',tablefmt='psql'))
                            resultDf = self.add2ResultsDf(dfs[0], yr, mo, cs, court_ndx, year_list, month_list, case_list,
                                                     year_names, month_names, case_names, court_names, resultDf, district_name)
            return resultDf, summaryDf
        except TimeoutException as e:
            # logging.error(str(e))
            # traceback.print_exc()
            pass
        except Exception as e:
            logging.error(str(e))
            traceback.print_exc()
            raise

    def getLatestMonthYear(self, driver):
        # latest month for which data is available in current year shows a date format that in not Month Year
        lblElmnt = driver.find_element_by_xpath(
            '//*[@id="ctl00_ContentPlaceHolder1_lblheadertext"]')
        lbl = lblElmnt.text.split(" ")
        lbl = lbl[-5:]
        currMonthLbl = str(" ".join(lbl))
        currDay = str(lbl[0])
        currMonth = str(lbl[2])
        return currMonthLbl, currDay, currMonth

    def waitForResultsToLoad(self, driver, yr, mo, cs, court_ndx, year_list, month_list, case_list,
                             year_names, month_names, case_names, court_names, currMonth, currDay):
        try:
            ndx2 = year_list.index(yr)
            ndx3 = month_list.index(mo)
            ndx4 = case_list.index(cs)
            caseName = case_names[ndx4]
            courtName = court_names[court_ndx]
            mnth = month_names[ndx3]
            yrs = year_names[ndx2]
            self.waitForMonthYearCaseAndCourtLabel(driver, mnth, yr, caseName, courtName, currMonth, currDay)
        except TimeoutException as e:
            #logging.error(str(e))
            #traceback.print_exc()
            pass
        return

    @retry(StaleElementReferenceException, tries=3, delay=10)
    def chooseMonthSelect(self, driver, movalue):
        # Get the month names and values
        #month_select = Select(self.driver.find_element_by_name("ctl00$ContentPlaceHolder1$dllmonth"))
        month_select = Select(driver.find_element_by_xpath('// *[ @ id = "ctl00_ContentPlaceHolder1_dllmonth"]'))
        month_select.select_by_value(movalue)

    def loadMonthList(self, driver):
        month_list = []
        month_names = []
        month_select = Select(driver.find_element_by_name("ctl00$ContentPlaceHolder1$dllmonth"))
        for y in month_select.options:
            month_list.append(str(y.get_attribute("value")))
            month_names.append(str(y.get_attribute("text")))
        return month_list, month_names

    def updateSummaryDf(self, court_ndx, court_names, summaryDf):
        try:
            for row in self.rows:
                for tag in row.find_all(True, {'id': True}):
                    tempDf = pd.DataFrame(data=[ (court_names[court_ndx], tag.text) ],
                                        columns = ["court","district"])
                    summaryDf = summaryDf.append(tempDf)
        except Exception as e:
            logging.error(str(e))
            traceback.print_exc()
            pass
        return summaryDf

    def initResultsDataFrame(self):
        # init empty data frame with just the column names
        df = pd.DataFrame(columns=['district','courtName','caseName','year','month','Slno', 'Tehsil_Name', 'Cases pending till end of Previous Month',
                                       'Total Cases instituted during the Month','Total Cases for Disposal', 'Total cases Disposed during the Month',
                                       'Total Cases pending', 'Percent of Disposal'])
        return df

    def initSummaryDataFrame(self):
        df = pd.DataFrame(columns=["court", "district"])
        return df

    def add2ResultsDf(self, df, yr, mo, cs, court_ndx, year_list, month_list, case_list,
                      year_names, month_names, case_names, court_names,
                      resultDf, district_name = None):
        # blow away the top row and the total row
        dfdim = df.shape
        rightdf = df.drop(df.index[[0, dfdim[0] - 1]])
        rightdfdim = rightdf.shape
        rightdf = rightdf.reset_index(drop=True)
        # if 9 columns then need to drop first column
        if rightdfdim[1] == 9:
            # need to drop a column that is the 7th from a 0 to 8 range
            rightdf = rightdf.drop([7],axis=1)
            pass
        rightdf.columns = ['Slno', 'Tehsil_Name', 'Cases pending till end of Previous Month',
                           'Total Cases instituted during the Month',
                           'Total Cases for Disposal', 'Total cases Disposed during the Month',
                           'Total Cases pending', 'Percent of Disposal']
        rightdf = rightdf.reset_index(drop=True)
        rightdfdim = rightdf.shape
        # now form the leftdf
        ndx2 = year_list.index(yr)
        ndx3 = month_list.index(mo)
        ndx4 = case_list.index(cs)
        if district_name == None:
            district_name = "All"
        logging.info("Executed: district - %s, court - %s, case - %s, year - %s, month - %s" % (
            district_name, court_names[court_ndx],case_names[ndx4],year_names[ndx2], month_names[ndx3] ))
        leftdf = pd.DataFrame(data = [(district_name,court_names[court_ndx], case_names[ndx4],year_names[ndx2], month_names[ndx3] ) ],
                              columns=['district','courtName','caseName','year','month'],)
        newdf = pd.DataFrame(np.repeat(leftdf.values, rightdfdim[0], axis=0))
        newdf.columns = leftdf.columns
        newdf = newdf.reset_index(drop=True)
        newdf = newdf.join(rightdf)
        resultDf = resultDf.append(newdf)
        return resultDf


    def readUrlLinks(self):
        readheader = False
        with open(self.directory + '/rccms_url.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if readheader == True:
                    self.district_stack.append(row)
                else:
                    readheader = True
                print ','.join(row)
        return



if __name__ == "__main__":
    try:
        logging.getLogger().setLevel(logging.INFO)
        logging.info("Starting OdishaRccmsScraper")
        parser = argparse.ArgumentParser(description='OdishaRccmsScraper')
        parser.add_argument('--noThreads', help='--noThreads', action='store_true')
        parser.add_argument('--debug', help='--debug', action='store_true')
        parser.add_argument('--scrapeTopLevel', help='--scrapeTopLevel', action='store_true')
        parser.add_argument('--scrapeLowerLevel', help='--scrapeLowerLevel', action='store_true')
        args = parser.parse_args()
        threadsOn = not args.noThreads
        scrapeTopLevel = args.scrapeTopLevel
        scrapeLowerLevel = args.scrapeLowerLevel
        debugType = args.debug
        runstart = timer()
        obj = OdishaRccmsScraper(debugType=debugType,
                                 scrapeTopLevel=scrapeTopLevel,
                                 scrapeLowerLevel=scrapeLowerLevel,
                                 threadsOn = threadsOn)
        #obj.processDistrictLevelLinks()
        runend = timer()
        runtime = runend - runstart
        logging.info("OdishaRccmsScraper runtime took %f min" % (runtime / 60))
        pass
    except RuntimeError as e:
        logging.error("RuntimeError ({0}): {1}".format(e.errno, e.strerror))
        traceback.print_exc()
        e = sys.exc_info()[0]
        logging.error("Error: %s" % e)
        exit(1)
    except Exception as e:
        logging.error(str(e))
        traceback.print_exc()
        exit(1)
    finally:
        logging.info("Completed OdishaScraping...")
        logging.shutdown()
