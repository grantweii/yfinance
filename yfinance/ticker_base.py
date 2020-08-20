#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import time as _time
import datetime as _datetime
import requests as _requests
import pandas as _pd
import numpy as _np
import csv
import json

try:
    from urllib.parse import quote as urlencode
except ImportError:
    from urllib import quote as urlencode

from . import utils

import json as _json
# import re as _re
# import sys as _sys

from . import shared


class TickerBase():
    def __init__(self, ticker, driver=None, session=None, **kwargs):
        self.ticker = ticker.upper()
        self.driver = driver
        self._history = None
        self._base_url = 'https://query1.finance.yahoo.com'
        self._scrape_url = 'https://finance.yahoo.com/quote'

        self.session = session

        self._fundamentals = False
        self._info = None
        self._sustainability = None
        self._recommendations = None
        self._major_holders = None
        self._institutional_holders = None
        self._isin = None

        self._calendar = None
        self._expirations = {}

        self._earnings = {
            "yearly": utils.empty_df(),
            "quarterly": utils.empty_df()}
        self._financials = {
            "yearly": utils.empty_df(),
            "quarterly": utils.empty_df()}
        self._balancesheet = {
            "yearly": utils.empty_df(),
            "quarterly": utils.empty_df()}
        self._cashflow = {
            "yearly": utils.empty_df(),
            "quarterly": utils.empty_df()}


    def history(self, period="1mo", interval="1d",
                start=None, end=None, prepost=False, actions=True,
                auto_adjust=True, back_adjust=False,
                proxy=None, rounding=True, tz=None, **kwargs):
        """
        :Parameters:
            period : str
                Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
                Either Use period parameter or use start and end
            interval : str
                Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
                Intraday data cannot extend last 60 days
            start: str
                Download start date string (YYYY-MM-DD) or _datetime.
                Default is 1900-01-01
            end: str
                Download end date string (YYYY-MM-DD) or _datetime.
                Default is now
            prepost : bool
                Include Pre and Post market data in results?
                Default is False
            auto_adjust: bool
                Adjust all OHLC automatically? Default is True
            back_adjust: bool
                Back-adjusted data to mimic true historical prices
            proxy: str
                Optional. Proxy server URL scheme. Default is None
            rounding: bool
                Round values to 2 decimal places?
                Optional. Default is False = precision suggested by Yahoo!
            tz: str
                Optional timezone locale for dates.
                (default data is returned as non-localized dates)
            **kwargs: dict
                debug: bool
                    Optional. If passed as False, will suppress
                    error message printing to console.
        """

        if start or period is None or period.lower() == "max":
            if start is None:
                start = -2208988800
            elif isinstance(start, _datetime.datetime):
                start = int(_time.mktime(start.timetuple()))
            else:
                start = int(_time.mktime(
                    _time.strptime(str(start), '%Y-%m-%d')))
            if end is None:
                end = int(_time.time())
            elif isinstance(end, _datetime.datetime):
                end = int(_time.mktime(end.timetuple()))
            else:
                end = int(_time.mktime(_time.strptime(str(end), '%Y-%m-%d')))

            params = {"period1": start, "period2": end}
        else:
            period = period.lower()
            params = {"range": period}

        params["interval"] = interval.lower()
        params["includePrePost"] = prepost
        params["events"] = "div,splits"

        # 1) fix weired bug with Yahoo! - returning 60m for 30m bars
        if params["interval"] == "30m":
            params["interval"] = "15m"

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        # Getting data from json
        url = "{}/v8/finance/chart/{}".format(self._base_url, self.ticker)
        data = _requests.get(url=url, params=params, proxies=proxy)
        if "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        data = data.json()

        # Work with errors
        debug_mode = True
        if "debug" in kwargs and isinstance(kwargs["debug"], bool):
            debug_mode = kwargs["debug"]

        err_msg = "No data found for this date range, symbol may be delisted"
        if "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return shared._DFS[self.ticker]

        elif "chart" not in data or data["chart"]["result"] is None or \
                not data["chart"]["result"]:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return shared._DFS[self.ticker]

        # parse quotes
        try:
            quotes = utils.parse_quotes(data["chart"]["result"][0], tz)
        except Exception:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return shared._DFS[self.ticker]

        # 2) fix weired bug with Yahoo! - returning 60m for 30m bars
        if interval.lower() == "30m":
            quotes2 = quotes.resample('30T')
            quotes = _pd.DataFrame(index=quotes2.last().index, data={
                'Open': quotes2['Open'].first(),
                'High': quotes2['High'].max(),
                'Low': quotes2['Low'].min(),
                'Close': quotes2['Close'].last(),
                'Adj Close': quotes2['Adj Close'].last(),
                'Volume': quotes2['Volume'].sum()
            })
            try:
                quotes['Dividends'] = quotes2['Dividends'].max()
            except Exception:
                pass
            try:
                quotes['Stock Splits'] = quotes2['Dividends'].max()
            except Exception:
                pass

        if auto_adjust:
            quotes = utils.auto_adjust(quotes)
        elif back_adjust:
            quotes = utils.back_adjust(quotes)

        if rounding:
            quotes = _np.round(quotes, data[
                "chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(_np.int64)

        quotes.dropna(inplace=True)

        # actions
        dividends, splits = utils.parse_actions(data["chart"]["result"][0], tz)

        # combine
        df = _pd.concat([quotes, dividends, splits], axis=1, sort=True)
        df["Dividends"].fillna(0, inplace=True)
        df["Stock Splits"].fillna(0, inplace=True)

        # index eod/intraday
        df.index = df.index.tz_localize("UTC").tz_convert(
            data["chart"]["result"][0]["meta"]["exchangeTimezoneName"])

        if params["interval"][-1] == "m":
            df.index.name = "Datetime"
        else:
            df.index = _pd.to_datetime(df.index.date)
            if tz is not None:
                df.index = df.index.tz_localize(tz)
            df.index.name = "Date"

        self._history = df.copy()

        if not actions:
            df.drop(columns=["Dividends", "Stock Splits"], inplace=True)

        return df

    # ------------------------

    def setup_proxy(self, proxy=None):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}
        return proxy

    
    def _get_financials(self, kind=None, proxy=None):
        def cleanup(data):
            # data is a list of income statement objects 
            # eg. { "researchDevelopment": 4887000000, "effectOfAccountingCharges": None, } for every year
            df = _pd.DataFrame(data).drop(columns=['maxAge'])
            for col in df.columns:
                df[col] = _np.where(
                    df[col].astype(str) == '-', _np.nan, df[col])

            df.set_index('endDate', inplace=True)
            try:
                df.index = _pd.to_datetime(df.index, unit='s')
            except ValueError:
                df.index = _pd.to_datetime(df.index)
            df = df.T
            df.columns.name = ''
            df.index.name = 'Breakdown'
            df.index = utils.camel2title(df.index)
            return df

        proxy = self.setup_proxy(proxy)

        # get financials
        url = '%s/%s' % (self._scrape_url, self.ticker)
        financialsData = utils.get_json(url+'/financials', 'financials', proxy)
        financialsSummaryData = financialsData['QuoteSummaryStore']

        # num shares API call
        url = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%spadTimeSeries=true&type=annualDilutedAverageShares,annualBasicAverageShares,quarterlyDilutedAverageShares,quarterlyBasicAverageShares&merge=false&period1=493590046&period2=%s&corsDomain=au.finance.yahoo.com' % (self.ticker, self.ticker, int(_time.time()))
        financialsTimeSeriesData = _json.loads(_requests.get(url=url).text)['timeseries']['result']

        # currently used to retrieve the provided key from the QuoteTimeSeriesStore and append the data to the 
        # current QuoteSummaryStore
        def appendExtraDetails(key, financialsSummaryData, financialsTimeSeriesData, timeSeries):
            summaryList = financialsSummaryData[timeSeries]['incomeStatementHistory']
            numSharesDict = next(filter(lambda x: x['meta']['type'][0] == key[0], financialsTimeSeriesData), None)
            for statement in summaryList:
                endDate = statement['endDate']
                if isinstance(numSharesDict.get('timestamp'), dict):
                    index = numSharesDict['timestamp'].index(endDate)

                    if index is None:
                        raise ValueError('No matching end date for %s' % (endDate))
                    
                    value = numSharesDict[key[0]][index]['reportedValue']['raw'] # value in scientific notation
                    statement[key[1]] = value
                    netIncome = statement['netIncome']
                    eps = netIncome/value
                    statement[key[2]] = eps

        if isinstance(financialsSummaryData.get('incomeStatementHistory'), dict):
            for key in (
                ('annualBasicAverageShares', 'basicAverageShares', 'basicEPS'),
                ('annualDilutedAverageShares', 'dilutedAverageShares', 'dilutedEPS')
            ):
                appendExtraDetails(key, financialsSummaryData, financialsTimeSeriesData, 'incomeStatementHistory')

        if isinstance(financialsSummaryData.get('incomeStatementHistoryQuarterly'), dict):
            for key in (
                ('quarterlyBasicAverageShares', 'basicAverageShares', 'basicEPS'),
                ('quarterlyDilutedAverageShares', 'dilutedAverageShares', 'dilutedEPS')
            ):
                appendExtraDetails(key, financialsSummaryData, financialsTimeSeriesData, 'incomeStatementHistoryQuarterly')

        # generic patterns
        for key in (
            (self._cashflow, 'cashflowStatement', 'cashflowStatements'),
            (self._balancesheet, 'balanceSheet', 'balanceSheetStatements'),
            (self._financials, 'incomeStatement', 'incomeStatementHistory')
        ):
            item = key[1] + 'History'
            if isinstance(financialsSummaryData.get(item), dict):
                key[0]['yearly'] = cleanup(financialsSummaryData[item][key[2]])

            item = key[1]+'HistoryQuarterly'
            if isinstance(financialsSummaryData.get(item), dict):
                key[0]['quarterly'] = cleanup(financialsSummaryData[item][key[2]])

        # earnings
        if isinstance(financialsSummaryData.get('earnings'), dict):
            earnings = financialsSummaryData['earnings']['financialsChart']
            df = _pd.DataFrame(earnings['yearly']).set_index('date')
            df.columns = utils.camel2title(df.columns)
            df.index.name = 'Year'
            self._earnings['yearly'] = df

            df = _pd.DataFrame(earnings['quarterly']).set_index('date')
            df.columns = utils.camel2title(df.columns)
            df.index.name = 'Quarter'
            self._earnings['quarterly'] = df

        self._fundamentals = True

    # requires yahoo premium account and public IP located in the US
    def _store_premium_financials(self, freq, proxy=None, errorWriter=None, successWriter=None):
        def store(data):
            print('Storing annual %s' % self.ticker)
            jsonData = json.loads(data)
            timeseries = jsonData.get('timeseries').get('result')
            with open('../datafiles/fundamentals/nasdaq/annual/{}.csv'.format(self.ticker), 'w', newline='') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(['date', 'entry', 'figure'])
                i = 0
                for item in timeseries:
                    key = item.get('meta').get('type')[0]
                    # this timestamps logic is to check if yahoo actually returns any data for this ticker
                    # ensure this only runs for annual entries, not trailing entries
                    if key.startswith('annual'):
                        timestamps = item.get('timestamp')
                        if timestamps is not None and len(timestamps) > 0: 
                            i += 1
                    values = item.get(key)
                    if values is not None and key.startswith('annual') and len(values) > 0:
                        for entry in values:
                            if entry is not None:
                                entryDate = entry.get('asOfDate')
                                entryFigure = entry.get('reportedValue').get('raw')
                                writer.writerow([entryDate, key, entryFigure])
                if i == 0:
                    with open('../datafiles/tickeroutcome/nasdaq/annual_nodata.csv', 'a', newline='') as nodataCsv:
                        print('%s has no data' % self.ticker)
                        nodataWriter = csv.writer(nodataCsv)
                        nodataWriter.writerow([self.ticker])
                else:
                    successWriter.writerow([self.ticker])

        try:
            proxy = self.setup_proxy(proxy)
            annual_url = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/premium/timeseries/%s?lang=en-US&region=US&symbol=%s&padTimeSeries=true&type=annualTaxEffectOfUnusualItems,trailingTaxEffectOfUnusualItems,annualTaxRateForCalcs,trailingTaxRateForCalcs,annualNormalizedEBITDA,trailingNormalizedEBITDA,annualNormalizedDilutedEPS,trailingNormalizedDilutedEPS,annualNormalizedBasicEPS,trailingNormalizedBasicEPS,annualTotalUnusualItems,trailingTotalUnusualItems,annualTotalUnusualItemsExcludingGoodwill,trailingTotalUnusualItemsExcludingGoodwill,annualNetIncomeFromContinuingOperationNetMinorityInterest,trailingNetIncomeFromContinuingOperationNetMinorityInterest,annualReconciledDepreciation,trailingReconciledDepreciation,annualReconciledCostOfRevenue,trailingReconciledCostOfRevenue,annualEBITDA,trailingEBITDA,annualEBIT,trailingEBIT,annualNetInterestIncome,trailingNetInterestIncome,annualInterestExpense,trailingInterestExpense,annualInterestIncome,trailingInterestIncome,annualContinuingAndDiscontinuedDilutedEPS,trailingContinuingAndDiscontinuedDilutedEPS,annualContinuingAndDiscontinuedBasicEPS,trailingContinuingAndDiscontinuedBasicEPS,annualNormalizedIncome,trailingNormalizedIncome,annualNetIncomeFromContinuingAndDiscontinuedOperation,trailingNetIncomeFromContinuingAndDiscontinuedOperation,annualTotalExpenses,trailingTotalExpenses,annualRentExpenseSupplemental,trailingRentExpenseSupplemental,annualReportedNormalizedDilutedEPS,trailingReportedNormalizedDilutedEPS,annualReportedNormalizedBasicEPS,trailingReportedNormalizedBasicEPS,annualTotalOperatingIncomeAsReported,trailingTotalOperatingIncomeAsReported,annualDividendPerShare,trailingDividendPerShare,annualDilutedAverageShares,trailingDilutedAverageShares,annualBasicAverageShares,trailingBasicAverageShares,annualDilutedEPS,trailingDilutedEPS,annualDilutedEPSOtherGainsLosses,trailingDilutedEPSOtherGainsLosses,annualTaxLossCarryforwardDilutedEPS,trailingTaxLossCarryforwardDilutedEPS,annualDilutedAccountingChange,trailingDilutedAccountingChange,annualDilutedExtraordinary,trailingDilutedExtraordinary,annualDilutedDiscontinuousOperations,trailingDilutedDiscontinuousOperations,annualDilutedContinuousOperations,trailingDilutedContinuousOperations,annualBasicEPS,trailingBasicEPS,annualBasicEPSOtherGainsLosses,trailingBasicEPSOtherGainsLosses,annualTaxLossCarryforwardBasicEPS,trailingTaxLossCarryforwardBasicEPS,annualBasicAccountingChange,trailingBasicAccountingChange,annualBasicExtraordinary,trailingBasicExtraordinary,annualBasicDiscontinuousOperations,trailingBasicDiscontinuousOperations,annualBasicContinuousOperations,trailingBasicContinuousOperations,annualDilutedNIAvailtoComStockholders,trailingDilutedNIAvailtoComStockholders,annualAverageDilutionEarnings,trailingAverageDilutionEarnings,annualNetIncomeCommonStockholders,trailingNetIncomeCommonStockholders,annualOtherunderPreferredStockDividend,trailingOtherunderPreferredStockDividend,annualPreferredStockDividends,trailingPreferredStockDividends,annualNetIncome,trailingNetIncome,annualMinorityInterests,trailingMinorityInterests,annualNetIncomeIncludingNoncontrollingInterests,trailingNetIncomeIncludingNoncontrollingInterests,annualNetIncomeFromTaxLossCarryforward,trailingNetIncomeFromTaxLossCarryforward,annualNetIncomeExtraordinary,trailingNetIncomeExtraordinary,annualNetIncomeDiscontinuousOperations,trailingNetIncomeDiscontinuousOperations,annualNetIncomeContinuousOperations,trailingNetIncomeContinuousOperations,annualEarningsFromEquityInterestNetOfTax,trailingEarningsFromEquityInterestNetOfTax,annualTaxProvision,trailingTaxProvision,annualPretaxIncome,trailingPretaxIncome,annualOtherIncomeExpense,trailingOtherIncomeExpense,annualOtherNonOperatingIncomeExpenses,trailingOtherNonOperatingIncomeExpenses,annualSpecialIncomeCharges,trailingSpecialIncomeCharges,annualGainOnSaleOfPPE,trailingGainOnSaleOfPPE,annualGainOnSaleOfBusiness,trailingGainOnSaleOfBusiness,annualOtherSpecialCharges,trailingOtherSpecialCharges,annualWriteOff,trailingWriteOff,annualImpairmentOfCapitalAssets,trailingImpairmentOfCapitalAssets,annualRestructuringAndMergernAcquisition,trailingRestructuringAndMergernAcquisition,annualSecuritiesAmortization,trailingSecuritiesAmortization,annualEarningsFromEquityInterest,trailingEarningsFromEquityInterest,annualGainOnSaleOfSecurity,trailingGainOnSaleOfSecurity,annualNetNonOperatingInterestIncomeExpense,trailingNetNonOperatingInterestIncomeExpense,annualTotalOtherFinanceCost,trailingTotalOtherFinanceCost,annualInterestExpenseNonOperating,trailingInterestExpenseNonOperating,annualInterestIncomeNonOperating,trailingInterestIncomeNonOperating,annualOperatingIncome,trailingOperatingIncome,annualOperatingExpense,trailingOperatingExpense,annualOtherOperatingExpenses,trailingOtherOperatingExpenses,annualOtherTaxes,trailingOtherTaxes,annualProvisionForDoubtfulAccounts,trailingProvisionForDoubtfulAccounts,annualDepreciationAmortizationDepletionIncomeStatement,trailingDepreciationAmortizationDepletionIncomeStatement,annualDepletionIncomeStatement,trailingDepletionIncomeStatement,annualDepreciationAndAmortizationInIncomeStatement,trailingDepreciationAndAmortizationInIncomeStatement,annualAmortization,trailingAmortization,annualAmortizationOfIntangiblesIncomeStatement,trailingAmortizationOfIntangiblesIncomeStatement,annualDepreciationIncomeStatement,trailingDepreciationIncomeStatement,annualResearchAndDevelopment,trailingResearchAndDevelopment,annualSellingGeneralAndAdministration,trailingSellingGeneralAndAdministration,annualSellingAndMarketingExpense,trailingSellingAndMarketingExpense,annualGeneralAndAdministrativeExpense,trailingGeneralAndAdministrativeExpense,annualOtherGandA,trailingOtherGandA,annualInsuranceAndClaims,trailingInsuranceAndClaims,annualRentAndLandingFees,trailingRentAndLandingFees,annualSalariesAndWages,trailingSalariesAndWages,annualGrossProfit,trailingGrossProfit,annualCostOfRevenue,trailingCostOfRevenue,annualTotalRevenue,trailingTotalRevenue,annualExciseTaxes,trailingExciseTaxes,annualOperatingRevenue,trailingOperatingRevenue&merge=false&period1=493590046&period2=1593637862&corsDomain=finance.yahoo.com' % (self.ticker, self.ticker)
            annualData = self.session.get(url=annual_url, proxies=proxy).text
            store(annualData)
            
        except Exception as e:
            print('An error has occurred: %s' % str(e))
            errorWriter.writerow([self.ticker, str(e)])
            raise
                

    # requires yahoo premium account and public IP located in the US
    def _store_premium_quarterly_financials(self, freq, proxy=None, errorWriter=None, successWriter=None):
        def store(data):
            print('Storing quarterly %s' % self.ticker)
            jsonData = json.loads(data)
            timeseries = jsonData.get('timeseries').get('result')
            with open('../datafiles/fundamentals/nasdaq/quarterly/{}.csv'.format(self.ticker), 'w', newline='') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(['date', 'entry', 'figure'])
                i = 0
                for item in timeseries:
                    key = item.get('meta').get('type')[0]
                    # this timestamps logic is to check if yahoo actually returns any data for this ticker
                    # ensure this only runs for annual entries, not trailing entries
                    if key.startswith('quarterly'):
                        timestamps = item.get('timestamp')
                        if timestamps is not None and len(timestamps) > 0: 
                            i += 1
                    key = item.get('meta').get('type')[0]
                    values = item.get(key)
                    if values is not None and key.startswith('quarterly') and len(values) > 0:
                        for entry in values:
                            if entry is not None:
                                entryDate = entry.get('asOfDate')
                                entryFigure = entry.get('reportedValue').get('raw')
                                writer.writerow([entryDate, key, entryFigure])
                if i == 0:
                    with open('../datafiles/tickeroutcome/nasdaq/quarterly_nodata.csv', 'a', newline='') as nodataCsv:
                        print('%s has no data' % self.ticker)
                        nodataWriter = csv.writer(nodataCsv)
                        nodataWriter.writerow([self.ticker])
                else:
                    successWriter.writerow([self.ticker])

        try:        
            # no quarterly data for ASX stonks
            if self.ticker.endswith('.AX'):
                return

            proxy = self.setup_proxy(proxy)
            quarterly_url = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/premium/timeseries/%s?lang=en-US&region=US&symbol=%s&padTimeSeries=true&type=quarterlyTaxEffectOfUnusualItems,trailingTaxEffectOfUnusualItems,quarterlyTaxRateForCalcs,trailingTaxRateForCalcs,quarterlyNormalizedEBITDA,trailingNormalizedEBITDA,quarterlyNormalizedDilutedEPS,trailingNormalizedDilutedEPS,quarterlyNormalizedBasicEPS,trailingNormalizedBasicEPS,quarterlyTotalUnusualItems,trailingTotalUnusualItems,quarterlyTotalUnusualItemsExcludingGoodwill,trailingTotalUnusualItemsExcludingGoodwill,quarterlyNetIncomeFromContinuingOperationNetMinorityInterest,trailingNetIncomeFromContinuingOperationNetMinorityInterest,quarterlyReconciledDepreciation,trailingReconciledDepreciation,quarterlyReconciledCostOfRevenue,trailingReconciledCostOfRevenue,quarterlyEBITDA,trailingEBITDA,quarterlyEBIT,trailingEBIT,quarterlyNetInterestIncome,trailingNetInterestIncome,quarterlyInterestExpense,trailingInterestExpense,quarterlyInterestIncome,trailingInterestIncome,quarterlyContinuingAndDiscontinuedDilutedEPS,trailingContinuingAndDiscontinuedDilutedEPS,quarterlyContinuingAndDiscontinuedBasicEPS,trailingContinuingAndDiscontinuedBasicEPS,quarterlyNormalizedIncome,trailingNormalizedIncome,quarterlyNetIncomeFromContinuingAndDiscontinuedOperation,trailingNetIncomeFromContinuingAndDiscontinuedOperation,quarterlyTotalExpenses,trailingTotalExpenses,quarterlyRentExpenseSupplemental,trailingRentExpenseSupplemental,quarterlyReportedNormalizedDilutedEPS,trailingReportedNormalizedDilutedEPS,quarterlyReportedNormalizedBasicEPS,trailingReportedNormalizedBasicEPS,quarterlyTotalOperatingIncomeAsReported,trailingTotalOperatingIncomeAsReported,quarterlyDividendPerShare,trailingDividendPerShare,quarterlyDilutedAverageShares,trailingDilutedAverageShares,quarterlyBasicAverageShares,trailingBasicAverageShares,quarterlyDilutedEPS,trailingDilutedEPS,quarterlyDilutedEPSOtherGainsLosses,trailingDilutedEPSOtherGainsLosses,quarterlyTaxLossCarryforwardDilutedEPS,trailingTaxLossCarryforwardDilutedEPS,quarterlyDilutedAccountingChange,trailingDilutedAccountingChange,quarterlyDilutedExtraordinary,trailingDilutedExtraordinary,quarterlyDilutedDiscontinuousOperations,trailingDilutedDiscontinuousOperations,quarterlyDilutedContinuousOperations,trailingDilutedContinuousOperations,quarterlyBasicEPS,trailingBasicEPS,quarterlyBasicEPSOtherGainsLosses,trailingBasicEPSOtherGainsLosses,quarterlyTaxLossCarryforwardBasicEPS,trailingTaxLossCarryforwardBasicEPS,quarterlyBasicAccountingChange,trailingBasicAccountingChange,quarterlyBasicExtraordinary,trailingBasicExtraordinary,quarterlyBasicDiscontinuousOperations,trailingBasicDiscontinuousOperations,quarterlyBasicContinuousOperations,trailingBasicContinuousOperations,quarterlyDilutedNIAvailtoComStockholders,trailingDilutedNIAvailtoComStockholders,quarterlyAverageDilutionEarnings,trailingAverageDilutionEarnings,quarterlyNetIncomeCommonStockholders,trailingNetIncomeCommonStockholders,quarterlyOtherunderPreferredStockDividend,trailingOtherunderPreferredStockDividend,quarterlyPreferredStockDividends,trailingPreferredStockDividends,quarterlyNetIncome,trailingNetIncome,quarterlyMinorityInterests,trailingMinorityInterests,quarterlyNetIncomeIncludingNoncontrollingInterests,trailingNetIncomeIncludingNoncontrollingInterests,quarterlyNetIncomeFromTaxLossCarryforward,trailingNetIncomeFromTaxLossCarryforward,quarterlyNetIncomeExtraordinary,trailingNetIncomeExtraordinary,quarterlyNetIncomeDiscontinuousOperations,trailingNetIncomeDiscontinuousOperations,quarterlyNetIncomeContinuousOperations,trailingNetIncomeContinuousOperations,quarterlyEarningsFromEquityInterestNetOfTax,trailingEarningsFromEquityInterestNetOfTax,quarterlyTaxProvision,trailingTaxProvision,quarterlyPretaxIncome,trailingPretaxIncome,quarterlyOtherIncomeExpense,trailingOtherIncomeExpense,quarterlyOtherNonOperatingIncomeExpenses,trailingOtherNonOperatingIncomeExpenses,quarterlySpecialIncomeCharges,trailingSpecialIncomeCharges,quarterlyGainOnSaleOfPPE,trailingGainOnSaleOfPPE,quarterlyGainOnSaleOfBusiness,trailingGainOnSaleOfBusiness,quarterlyOtherSpecialCharges,trailingOtherSpecialCharges,quarterlyWriteOff,trailingWriteOff,quarterlyImpairmentOfCapitalAssets,trailingImpairmentOfCapitalAssets,quarterlyRestructuringAndMergernAcquisition,trailingRestructuringAndMergernAcquisition,quarterlySecuritiesAmortization,trailingSecuritiesAmortization,quarterlyEarningsFromEquityInterest,trailingEarningsFromEquityInterest,quarterlyGainOnSaleOfSecurity,trailingGainOnSaleOfSecurity,quarterlyNetNonOperatingInterestIncomeExpense,trailingNetNonOperatingInterestIncomeExpense,quarterlyTotalOtherFinanceCost,trailingTotalOtherFinanceCost,quarterlyInterestExpenseNonOperating,trailingInterestExpenseNonOperating,quarterlyInterestIncomeNonOperating,trailingInterestIncomeNonOperating,quarterlyOperatingIncome,trailingOperatingIncome,quarterlyOperatingExpense,trailingOperatingExpense,quarterlyOtherOperatingExpenses,trailingOtherOperatingExpenses,quarterlyOtherTaxes,trailingOtherTaxes,quarterlyProvisionForDoubtfulAccounts,trailingProvisionForDoubtfulAccounts,quarterlyDepreciationAmortizationDepletionIncomeStatement,trailingDepreciationAmortizationDepletionIncomeStatement,quarterlyDepletionIncomeStatement,trailingDepletionIncomeStatement,quarterlyDepreciationAndAmortizationInIncomeStatement,trailingDepreciationAndAmortizationInIncomeStatement,quarterlyAmortization,trailingAmortization,quarterlyAmortizationOfIntangiblesIncomeStatement,trailingAmortizationOfIntangiblesIncomeStatement,quarterlyDepreciationIncomeStatement,trailingDepreciationIncomeStatement,quarterlyResearchAndDevelopment,trailingResearchAndDevelopment,quarterlySellingGeneralAndAdministration,trailingSellingGeneralAndAdministration,quarterlySellingAndMarketingExpense,trailingSellingAndMarketingExpense,quarterlyGeneralAndAdministrativeExpense,trailingGeneralAndAdministrativeExpense,quarterlyOtherGandA,trailingOtherGandA,quarterlyInsuranceAndClaims,trailingInsuranceAndClaims,quarterlyRentAndLandingFees,trailingRentAndLandingFees,quarterlySalariesAndWages,trailingSalariesAndWages,quarterlyGrossProfit,trailingGrossProfit,quarterlyCostOfRevenue,trailingCostOfRevenue,quarterlyTotalRevenue,trailingTotalRevenue,quarterlyExciseTaxes,trailingExciseTaxes,quarterlyOperatingRevenue,trailingOperatingRevenue&merge=false&period1=493590046&period2=1593466751&corsDomain=finance.yahoo.com' % (self.ticker, self.ticker)
            quarterlyData = self.session.get(url=quarterly_url, proxies=proxy).text
            store(quarterlyData)
            
        except Exception as e:
            print('An error has occurred: %s' % str(e))
            errorWriter.writerow([self.ticker, str(e)])
            raise


    def _get_holders(self, kind=None, proxy=None):
        proxy = self.setup_proxy(proxy)

        # holders
        url = "{}/{}".format(self._scrape_url, self.ticker)
        holders = _pd.read_html(url+'/holders')
        self._major_holders = holders[0]
        if (len(holders) > 1):
            self._institutional_holders = holders[1]
            if 'Date Reported' in self._institutional_holders:
                self._institutional_holders['Date Reported'] = _pd.to_datetime(
                    self._institutional_holders['Date Reported'])
            if '% Out' in self._institutional_holders:
                self._institutional_holders['% Out'] = self._institutional_holders[
                    '% Out'].str.replace('%', '').astype(float)/100

    # returns cashflows as json
    # Refactored completely by Grant
    def _get_cashflow(self, proxy=None, freq='yearly'):
        def cleanData(data):
            jsonData = json.loads(data)
            timeseries = jsonData.get('timeseries').get('result')
            dict = {}
            for item in timeseries:
                key = item.get('meta').get('type')[0]
                values = item.get(key)
                if values is not None:
                    for entry in values:
                        if entry is not None:
                            entryDate = entry.get('asOfDate')
                            entryFigure = entry.get('reportedValue').get('raw')
                            if key not in dict.keys():
                                dict[key] = {}
                            dict[key][entryDate] = entryFigure
            return dict

        proxy = self.setup_proxy(proxy)

        if freq == 'yearly':
            cashflowUrl = 'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=annualFreeCashFlow,trailingFreeCashFlow,annualCapitalExpenditure,trailingCapitalExpenditure,annualOperatingCashFlow,trailingOperatingCashFlow,annualEndCashPosition,trailingEndCashPosition,annualBeginningCashPosition,trailingBeginningCashPosition,annualChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,annualCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,annualNetOtherFinancingCharges,trailingNetOtherFinancingCharges,annualCashDividendsPaid,trailingCashDividendsPaid,annualRepurchaseOfCapitalStock,trailingRepurchaseOfCapitalStock,annualCommonStockIssuance,trailingCommonStockIssuance,annualRepaymentOfDebt,trailingRepaymentOfDebt,annualInvestingCashFlow,trailingInvestingCashFlow,annualNetOtherInvestingChanges,trailingNetOtherInvestingChanges,annualSaleOfInvestment,trailingSaleOfInvestment,annualPurchaseOfInvestment,trailingPurchaseOfInvestment,annualPurchaseOfBusiness,trailingPurchaseOfBusiness,annualOtherNonCashItems,trailingOtherNonCashItems,annualChangeInAccountPayable,trailingChangeInAccountPayable,annualChangeInInventory,trailingChangeInInventory,annualChangesInAccountReceivables,trailingChangesInAccountReceivables,annualChangeInWorkingCapital,trailingChangeInWorkingCapital,annualStockBasedCompensation,trailingStockBasedCompensation,annualDeferredIncomeTax,trailingDeferredIncomeTax,annualDepreciationAndAmortization,trailingDepreciationAndAmortization,annualNetIncome,trailingNetIncome&merge=false&period1=493590046&period2=1597921829&corsDomain=au.finance.yahoo.com' % (self.ticker, self.ticker)
        else:
            cashflowUrl = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=quarterlyFreeCashFlow,trailingFreeCashFlow,quarterlyCapitalExpenditure,trailingCapitalExpenditure,quarterlyOperatingCashFlow,trailingOperatingCashFlow,quarterlyEndCashPosition,trailingEndCashPosition,quarterlyBeginningCashPosition,trailingBeginningCashPosition,quarterlyChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,quarterlyCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,quarterlyNetOtherFinancingCharges,trailingNetOtherFinancingCharges,quarterlyCashDividendsPaid,trailingCashDividendsPaid,quarterlyRepurchaseOfCapitalStock,trailingRepurchaseOfCapitalStock,quarterlyCommonStockIssuance,trailingCommonStockIssuance,quarterlyRepaymentOfDebt,trailingRepaymentOfDebt,quarterlyInvestingCashFlow,trailingInvestingCashFlow,quarterlyNetOtherInvestingChanges,trailingNetOtherInvestingChanges,quarterlySaleOfInvestment,trailingSaleOfInvestment,quarterlyPurchaseOfInvestment,trailingPurchaseOfInvestment,quarterlyPurchaseOfBusiness,trailingPurchaseOfBusiness,quarterlyOtherNonCashItems,trailingOtherNonCashItems,quarterlyChangeInAccountPayable,trailingChangeInAccountPayable,quarterlyChangeInInventory,trailingChangeInInventory,quarterlyChangesInAccountReceivables,trailingChangesInAccountReceivables,quarterlyChangeInWorkingCapital,trailingChangeInWorkingCapital,quarterlyStockBasedCompensation,trailingStockBasedCompensation,quarterlyDeferredIncomeTax,trailingDeferredIncomeTax,quarterlyDepreciationAndAmortization,trailingDepreciationAndAmortization,quarterlyNetIncome,trailingNetIncome&merge=false&period1=493590046&period2=1597926642&corsDomain=au.finance.yahoo.com' % (self.ticker, self.ticker)
        data = _requests.get(url=cashflowUrl, proxies=proxy).text

        cashflows = cleanData(data)
        return cashflows

    def _get_sustainability(self, kind=None, proxy=None):
        proxy = self.setup_proxy(proxy)

        # get sustainability
        url = '%s/%s' % (self._scrape_url, self.ticker)
        infoData = utils.get_json(url, 'info', proxy)['QuoteSummaryStore']

        # sustainability
        d = {}
        if isinstance(infoData.get('esgScores'), dict):
            for item in infoData['esgScores']:
                if not isinstance(infoData['esgScores'][item], (dict, list)):
                    d[item] = infoData['esgScores'][item]

            s = _pd.DataFrame(index=[0], data=d)[-1:].T
            s.columns = ['Value']
            s.index.name = '%.f-%.f' % (
                s[s.index == 'ratingYear']['Value'].values[0],
                s[s.index == 'ratingMonth']['Value'].values[0])

            self._sustainability = s[~s.index.isin(
                ['maxAge', 'ratingYear', 'ratingMonth'])]

    def _get_info(self, kind=None, proxy=None):
        proxy = self.setup_proxy(proxy)

        # get info
        url = '%s/%s' % (self._scrape_url, self.ticker)
        infoData = utils.get_json(url, 'info', proxy)['QuoteSummaryStore']

        # info (be nice to python 2)
        self._info = {}
        items = ['summaryProfile', 'summaryDetail', 'quoteType',
                 'defaultKeyStatistics', 'assetProfile', 'summaryDetail']
        for item in items:
            if isinstance(infoData.get(item), dict):
                self._info.update(infoData[item])

        self._info['regularMarketPrice'] = self._info['regularMarketOpen']
        self._info['logo_url'] = ""
        try:
            domain = self._info['website'].split(
                '://')[1].split('/')[0].replace('www.', '')
            self._info['logo_url'] = 'https://logo.clearbit.com/%s' % domain
        except Exception:
            pass

    def _get_events(self, kind=None, proxy=None):
        proxy = self.setup_proxy(proxy)

        # get events
        url = '%s/%s' % (self._scrape_url, self.ticker)
        infoData = utils.get_json(url, 'info', proxy)['QuoteSummaryStore']

         # events
        try:
            cal = _pd.DataFrame(
                infoData['calendarEvents']['earnings'])
            cal['earningsDate'] = _pd.to_datetime(
                cal['earningsDate'], unit='s')
            self._calendar = cal.T
            self._calendar.index = utils.camel2title(self._calendar.index)
            self._calendar.columns = ['Value']
        except Exception:
            pass

    def _get_recommendations(self, kind=None, proxy=None):
        proxy = self.setup_proxy(proxy)

        # get recommendations
        url = '%s/%s' % (self._scrape_url, self.ticker)
        infoData = utils.get_json(url, 'info', proxy)['QuoteSummaryStore']

        # analyst recommendations
        try:
            rec = _pd.DataFrame(
                infoData['upgradeDowngradeHistory']['history'])
            rec['earningsDate'] = _pd.to_datetime(
                rec['epochGradeDate'], unit='s')
            rec.set_index('earningsDate', inplace=True)
            rec.index.name = 'Date'
            rec.columns = utils.camel2title(rec.columns)
            self._recommendations = rec[[
                'Firm', 'To Grade', 'From Grade', 'Action']].sort_index()
        except Exception:
            pass

    def get_recommendations(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_recommendations(proxy)
        data = self._recommendations
        if as_dict:
            return data.to_dict()
        return data

    def get_calendar(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_events(proxy)
        data = self._calendar
        if as_dict:
            return data.to_dict()
        return data

    def get_major_holders(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_holders(proxy)
        data = self._major_holders
        if as_dict:
            return data.to_dict()
        return data

    def get_institutional_holders(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_holders(proxy)
        data = self._institutional_holders
        if as_dict:
            return data.to_dict()
        return data

    def get_info(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_info(proxy)
        data = self._info
        if as_dict:
            return data.to_dict()
        return data

    def get_sustainability(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_sustainability(proxy)
        data = self._sustainability
        if as_dict:
            return data.to_dict()
        return data

    def get_earnings(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_financials(proxy)
        data = self._earnings[freq]
        if as_dict:
            return data.to_dict()
        return data

    def get_financials(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_financials(proxy)
        data = self._financials[freq]
        if as_dict:
            return data.to_dict()
        return data

    # Created by Grant
    def get_premium_financials(self, proxy=None, as_dict=False, freq='yearly', errorWriter=None, successWriter=None):
        if freq == 'yearly':
            self._store_premium_financials(freq, proxy, errorWriter=errorWriter, successWriter=successWriter)
        else:
            self._store_premium_quarterly_financials(freq, proxy, errorWriter=errorWriter, successWriter=successWriter)

    def get_balancesheet(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_financials(proxy)
        data = self._balancesheet[freq]
        if as_dict:
            return data.to_dict()
        return data

    def get_balance_sheet(self, proxy=None, as_dict=False, freq="yearly"):
        return self.get_balancesheet(proxy, as_dict, freq)

    # Refactored completely by Grant
    def get_cashflow(self, proxy=None, as_dict=False, freq="yearly"):
        data = self._get_cashflow(proxy, freq)
        return data

    def get_dividends(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        dividends = self._history["Dividends"]
        return dividends[dividends != 0]

    def get_splits(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        splits = self._history["Stock Splits"]
        return splits[splits != 0]

    def get_actions(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        actions = self._history[["Dividends", "Stock Splits"]]
        return actions[actions != 0].dropna(how='all').fillna(0)

    def get_isin(self, proxy=None):
        # *** experimental ***
        if self._isin is not None:
            return self._isin

        ticker = self.ticker.upper()

        if "-" in ticker or "^" in ticker:
            self._isin = '-'
            return self._isin

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        q = ticker
        self.get_info(proxy=proxy)
        if "shortName" in self._info:
            q = self._info['shortName']

        url = 'https://markets.businessinsider.com/ajax/' \
              'SearchController_Suggest?max_results=25&query=%s' \
            % urlencode(q)
        data = _requests.get(url=url, proxies=proxy).text

        search_str = '"{}|'.format(ticker)
        if search_str not in data:
            if q.lower() in data.lower():
                search_str = '"|'
                if search_str not in data:
                    self._isin = '-'
                    return self._isin
            else:
                self._isin = '-'
                return self._isin

        self._isin = data.split(search_str)[1].split('"')[0].split('|')[0]
        return self._isin
