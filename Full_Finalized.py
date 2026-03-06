import yfinance as yf
import json
import os
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any
import requests
import importlib.metadata

    
class StockDataProcessor:
    """Class to handle stock data fetching and processing"""
    
    def __init__(self, output_dir: str = "All Symbols"):
        self.output_dir = output_dir
        self.logger = self._setup_logging()
        self.presentable_names = self._get_presentable_names()
        self._setup_output_directory()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        os.makedirs(self.output_dir, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(
                    os.path.join(self.output_dir, f"stock_data_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
                ),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def _setup_output_directory(self):
        """Create output directory if it doesn't exist"""
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.info(f"Output directory ready: {self.output_dir}")
    
    def _get_presentable_names(self) -> Dict[str, str]:
        """Return dictionary of field name mappings"""
        return {
            # Risk and Governance
            "auditRisk": "Audit Risk Level",
            "boardRisk": "Board Governance Risk Level",
            "compensationRisk": "Compensation Risk Level",
            "shareHolderRightsRisk": "Shareholder Rights Risk Level",
            "overallRisk": "Overall Governance Risk Level",
            "governanceEpochDate": "Governance Data Timestamp",
            "compensationAsOfEpochDate": "Compensation Data Timestamp",
            
            # Basic Info
            "symbol": "Stock Symbol",
            "shortName": "Short Company Name",
            "longName": "Full Company Name",
            "currency": "Currency",
            "quoteType": "Quote Type",
            
            # Price Data
            "currentPrice": "Current Stock Price",
            "previousClose": "Previous Closing Price",
            "open": "Opening Price",
            "dayLow": "Daily Low Price",
            "dayHigh": "Daily High Price",
            "regularMarketPreviousClose": "Regular Market Previous Closing Price",
            "regularMarketOpen": "Regular Market Opening Price",
            "regularMarketDayLow": "Regular Market Daily Low Price",
            "regularMarketDayHigh": "Regular Market Daily High Price",
            "regularMarketPrice": "Regular Market Price",
            "regularMarketChange": "Regular Market Price Change",
            "regularMarketChangePercent": "Regular Market Change Percentage",
            "regularMarketDayRange": "Regular Market Daily Price Range",
            
            # 52-Week Data
            "fiftyTwoWeekLow": "52-Week Low Price",
            "fiftyTwoWeekHigh": "52-Week High Price",
            "fiftyTwoWeekRange": "52-Week Price Range",
            "fiftyTwoWeekChange": "52-Week Price Change Percentage",
            "fiftyTwoWeekChangePercent": "52-Week Change Percentage",
            "fiftyTwoWeekLowChange": "Change from 52-Week Low",
            "fiftyTwoWeekLowChangePercent": "Percentage Change from 52-Week Low",
            "fiftyTwoWeekHighChange": "Change from 52-Week High",
            "fiftyTwoWeekHighChangePercent": "Percentage Change from 52-Week High",
            
            # Moving Averages
            "fiftyDayAverage": "50-Day Moving Average Price",
            "twoHundredDayAverage": "200-Day Moving Average Price",
            "fiftyDayAverageChange": "Change from 50-Day Average",
            "fiftyDayAverageChangePercent": "Percentage Change from 50-Day Average",
            "twoHundredDayAverageChange": "Change from 200-Day Average",
            "twoHundredDayAverageChangePercent": "Percentage Change from 200-Day Average",
            
            # Volume
            "volume": "Trading Volume",
            "regularMarketVolume": "Regular Market Trading Volume",
            "averageVolume": "Average Daily Trading Volume",
            "averageVolume10days": "10-Day Average Trading Volume",
            "averageDailyVolume10Day": "10-Day Average Daily Trading Volume",
            "averageDailyVolume3Month": "3-Month Average Daily Volume",
            
            # Bid/Ask
            "bid": "Bid Price",
            "ask": "Ask Price",
            "bidSize": "Bid Size (Shares)",
            "askSize": "Ask Size (Shares)",
            
            # Market Data
            "marketCap": "Market Capitalization",
            "enterpriseValue": "Enterprise Value",
            "floatShares": "Float Shares",
            "sharesOutstanding": "Shares Outstanding",
            "impliedSharesOutstanding": "Implied Shares Outstanding",
            
            # Dividends
            "dividendRate": "Annual Dividend Rate",
            "dividendYield": "Dividend Yield Percentage",
            "exDividendDate": "Ex-Dividend Date Timestamp",
            "dividendDate": "Dividend Payment Date Timestamp",
            "payoutRatio": "Dividend Payout Ratio",
            "fiveYearAvgDividendYield": "Five-Year Average Dividend Yield",
            "trailingAnnualDividendRate": "Trailing Annual Dividend Rate",
            "trailingAnnualDividendYield": "Trailing Annual Dividend Yield",
            "lastDividendValue": "Last Dividend Value",
            "lastDividendDate": "Last Dividend Date Timestamp",
            
            # Financial Ratios
            "beta": "Stock Beta (Volatility)",
            "trailingPE": "Trailing Price-to-Earnings Ratio",
            "forwardPE": "Forward Price-to-Earnings Ratio",
            "priceToSalesTrailing12Months": "Price-to-Sales Ratio (Trailing 12 Months)",
            "bookValue": "Book Value per Share",
            "priceToBook": "Price-to-Book Ratio",
            "trailingPegRatio": "Trailing PEG Ratio",
            "debtToEquity": "Debt-to-Equity Ratio",
            "quickRatio": "Quick Ratio",
            "currentRatio": "Current Ratio",
            "returnOnAssets": "Return on Assets",
            "returnOnEquity": "Return on Equity",
            
            # Earnings
            "trailingEps": "Trailing Earnings per Share",
            "forwardEps": "Forward Earnings per Share",
            "epsTrailingTwelveMonths": "EPS Trailing 12 Months",
            "epsForward": "Forward EPS",
            "epsCurrentYear": "Current Year EPS",
            "priceEpsCurrentYear": "Price-to-EPS Current Year Ratio",
            "netIncomeToCommon": "Net Income to Common Shareholders",
            
            # Revenue and Profitability
            "totalRevenue": "Total Revenue",
            "revenuePerShare": "Revenue per Share",
            "revenueGrowth": "Revenue Growth Rate",
            "grossProfits": "Gross Profits",
            "profitMargins": "Profit Margin Percentage",
            "grossMargins": "Gross Margin Percentage",
            "ebitdaMargins": "EBITDA Margin Percentage",
            "operatingMargins": "Operating Margin Percentage",
            
            # Cash Flow
            "freeCashflow": "Free Cash Flow",
            "operatingCashflow": "Operating Cash Flow",
            "totalCash": "Total Cash",
            "totalCashPerShare": "Total Cash per Share",
            "totalDebt": "Total Debt",
            "ebitda": "EBITDA",
            
            # Analyst Data
            "targetHighPrice": "Analyst Target High Price",
            "targetLowPrice": "Analyst Target Low Price",
            "targetMeanPrice": "Analyst Target Mean Price",
            "targetMedianPrice": "Analyst Target Median Price",
            "recommendationMean": "Analyst Recommendation Mean Score",
            "recommendationKey": "Analyst Recommendation Key",
            "numberOfAnalystOpinions": "Number of Analyst Opinions",
            "averageAnalystRating": "Average Analyst Rating",
            
            # Short Interest
            "sharesShort": "Short Interest (Shares)",
            "sharesShortPriorMonth": "Previous Month Short Interest (Shares)",
            "sharesShortPreviousMonthDate": "Previous Month Short Interest Date Timestamp",
            "dateShortInterest": "Short Interest Date Timestamp",
            "sharesPercentSharesOut": "Short Interest as Percentage of Shares Outstanding",
            "shortRatio": "Short Interest Ratio",
            "shortPercentOfFloat": "Short Interest as Percentage of Float",
            
            # Ownership
            "heldPercentInsiders": "Percentage of Shares Held by Insiders",
            "heldPercentInstitutions": "Percentage of Shares Held by Institutions",
            
            # Company Info
            "address1": "Company Address Line 1",
            "city": "Company City",
            "state": "Company State",
            "zip": "Company Zip Code",
            "country": "Company Country",
            "phone": "Company Phone Number",
            "website": "Company Website",
            "irWebsite": "Investor Relations Website",
            "industry": "Industry",
            "industryKey": "Industry Key",
            "industryDisp": "Industry Display Name",
            "sector": "Sector",
            "sectorKey": "Sector Key",
            "sectorDisp": "Sector Display Name",
            "longBusinessSummary": "Business Summary",
            "fullTimeEmployees": "Full-Time Employees",
            
            # Dates and Times
            "lastFiscalYearEnd": "Last Fiscal Year End Timestamp",
            "nextFiscalYearEnd": "Next Fiscal Year End Timestamp",
            "mostRecentQuarter": "Most Recent Quarter Timestamp",
            "earningsTimestamp": "Earnings Report Timestamp",
            "earningsTimestampStart": "Earnings Report Start Timestamp",
            "earningsTimestampEnd": "Earnings Report End Timestamp",
            "earningsCallTimestampStart": "Earnings Call Start Timestamp",
            "earningsCallTimestampEnd": "Earnings Call End Timestamp",
            "isEarningsDateEstimate": "Is Earnings Date Estimated",
            "lastSplitDate": "Last Stock Split Date Timestamp",
            "lastSplitFactor": "Last Stock Split Factor",
            
            # Technical Fields
            "maxAge": "Data Cache Duration (Seconds)",
            "priceHint": "Price Display Hint",
            "tradeable": "Tradeable Status",
            "cryptoTradeable": "Crypto Tradeable Status",
            "hasPrePostMarketData": "Has Pre/Post Market Data",
            "postMarketTime": "Post-Market Time Timestamp",
            "regularMarketTime": "Regular Market Time Timestamp",
            "postMarketChangePercent": "Post-Market Change Percentage",
            "postMarketPrice": "Post-Market Price",
            "postMarketChange": "Post-Market Price Change",
            "firstTradeDateMilliseconds": "First Trade Date Timestamp",
            "fullExchangeName": "Full Exchange Name",
            "exchange": "Exchange Code",
            "exchangeTimezoneName": "Exchange Timezone Name",
            "exchangeTimezoneShortName": "Exchange Timezone Short Name",
            "gmtOffSetMilliseconds": "GMT Offset (Milliseconds)",
            "market": "Market Type",
            "marketState": "Market State",
            "sourceInterval": "Data Source Interval (Minutes)",
            "exchangeDataDelayedBy": "Exchange Data Delay (Minutes)",
            
            # Other
            "SandP52WeekChange": "S&P 52-Week Change Percentage",
            "enterpriseToRevenue": "Enterprise Value to Revenue Ratio",
            "enterpriseToEbitda": "Enterprise Value to EBITDA Ratio",
            "financialCurrency": "Financial Currency",
            "language": "Language",
            "region": "Region",
            "typeDisp": "Type Display",
            "quoteSourceName": "Quote Source Name",
            "triggerable": "Triggerable Status",
            "customPriceAlertConfidence": "Custom Price Alert Confidence",
            "messageBoardId": "Message Board ID",
            "esgPopulated": "ESG Data Populated",
            "displayName": "Display Name",
            "executiveTeam": "Executive Team List",
            
            # Special List Fields
            "companyOfficers": "Company Officers List",
            "corporateActions": "Corporate Actions List",
            
            # Officer-specific fields
            "name": "Officer Name",
            "age": "Officer Age",
            "title": "Officer Title",
            "yearBorn": "Officer Year of Birth",
            "fiscalYear": "Fiscal Year",
            "totalPay": "Total Compensation",
            "exercisedValue": "Exercised Stock Options Value",
            
            # Corporate Actions fields
            "header": "Action Type",
            "message": "Action Description",
            "meta": "Action Metadata",
            "eventType": "Event Type",
            "dateEpochMs": "Event Date (Epoch Milliseconds)",
            "amount": "Amount"
        }
    

    
    def _rename_nested_fields(self, data: Any) -> Any:
        """Recursively rename fields in nested structures"""
        if isinstance(data, dict):
            renamed_dict = {}
            for key, value in data.items():
                new_key = self.presentable_names.get(key, key)
                renamed_dict[new_key] = self._rename_nested_fields(value)
            return renamed_dict
        elif isinstance(data, list):
            return [self._rename_nested_fields(item) for item in data]
        else:
            return data
    
    def _rename_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Rename fields using presentable names"""
        renamed_data = {}
        
        for key, value in data.items():
            new_key = self.presentable_names.get(key, key)
            
            # Handle special list fields that need nested renaming
            if key in ["companyOfficers", "corporateActions"] and isinstance(value, list):
                renamed_value = []
                for item in value:
                    if isinstance(item, dict):
                        renamed_item = self._rename_nested_fields(item)
                        renamed_value.append(renamed_item)
                    else:
                        renamed_value.append(item)

                renamed_data[new_key] = renamed_value
            else:
                renamed_data[new_key] = value
        
        return renamed_data
    
    def fetch_stock_data(self, ticker: str) -> Dict[str, Any]:
        """Fetch data for a single ticker"""
        self.logger.info(f"Fetching data for ticker: {ticker}")
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info:
                raise ValueError("No data returned from yfinance")
            
            self.logger.info(f"Successfully retrieved data for ticker: {ticker}")
            return self._rename_fields(info)
            
        except Exception as e:
            error_msg = f"Error fetching data for ticker {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return {"error": str(e)}
    
    def process_company_data(self, ticker: str, info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract company data (excluding officers and corporate actions) for Excel sheet"""
        company_data = {"Ticker": ticker}
        
        if "error" in info:
            company_data["Error"] = info["error"]
            return company_data
        
        # Exclude special list fields from company data
        excluded_fields = ["Company Officers List", "Corporate Actions List"]
        
        for key, value in info.items():
            if key not in excluded_fields:
                company_data[key] = value
        
        return company_data
    
    def process_officers_data(self, ticker: str, info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract officers data for Excel sheet"""
        officers_data = []
        
        if "error" in info:
            return []
        
        officers = info.get("Company Officers List", [])

        if not isinstance(officers, list):
            self.logger.warning(f"Company Officers data for {ticker} is not a list: {type(officers)}")
            return []
        
        for officer in officers:
            if isinstance(officer, dict):
                officer_data = {"Ticker": ticker}
                officer_data.update(officer)
                officers_data.append(officer_data)
            else:
                self.logger.warning(f"Officer data for {ticker} is not a dict: {type(officer)}")
        
        return officers_data
    
    def process_corporate_actions_data(self, ticker: str, info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract corporate actions data for Excel sheet"""
        actions_data = []
        
        if "error" in info:
            return []
        
        corporate_actions = info.get("Corporate Actions List", [])
        if not isinstance(corporate_actions, list):
            self.logger.warning(f"Corporate Actions data for {ticker} is not a list: {type(corporate_actions)}")
            return []
        
        for action in corporate_actions:
            if isinstance(action, dict):
                action_data = {"Ticker": ticker}
                
                # Handle the main fields
                for key, value in action.items():
                    if key == "Action Metadata" and isinstance(value, dict):
                        # Flatten metadata fields
                        for meta_key, meta_value in value.items():
                            action_data[meta_key] = meta_value
                    else:
                        action_data[key] = value
                
                actions_data.append(action_data)
            else:
                self.logger.warning(f"Corporate action data for {ticker} is not a dict: {type(action)}")
        
        return actions_data
    
    def save_data(self, all_data: Dict[str, Any], company_data_list: List[Dict], 
                  officers_data_list: List[Dict], actions_data_list: List[Dict]):
        """Save data to JSON and Excel files"""
        # Save JSON
        json_file = os.path.join(self.output_dir, "All_Tickers.json")
        self.logger.info(f"Saving data to JSON file: {json_file}")
        
        try:
            with open(json_file, "w") as f:
                json.dump(all_data, f, indent=2, default=str)
            self.logger.info(f"Successfully saved data to JSON file: {json_file}")
        except Exception as e:
            self.logger.error(f"Error saving JSON file: {str(e)}")
            raise
        
        # Save Excel
        excel_file = os.path.join(self.output_dir, "All_Tickers.xlsx")
        self.logger.info(f"Saving data to Excel file: {excel_file}")
        
        try:
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                # Company Data sheet
                if company_data_list:
                    company_df = pd.DataFrame(company_data_list)
                    company_df.to_excel(writer, sheet_name="Company Data", index=False)
                    self.logger.info("Saved Company Data sheet to Excel")
                
                # Company Officers sheet
                if officers_data_list:
                    officers_df = pd.DataFrame(officers_data_list)
                    officers_df.to_excel(writer, sheet_name="Company Officers", index=False)
                    self.logger.info("Saved Company Officers sheet to Excel")
                
                # Corporate Actions sheet
                if actions_data_list:
                    actions_df = pd.DataFrame(actions_data_list)
                    actions_df.to_excel(writer, sheet_name="Corporate Actions", index=False)
                    self.logger.info("Saved Corporate Actions sheet to Excel")
                else:
                    # Create empty sheet if no data
                    empty_df = pd.DataFrame({"Ticker": [], "Message": ["No corporate actions data available"]})
                    empty_df.to_excel(writer, sheet_name="Corporate Actions", index=False)
                    self.logger.info("Created empty Corporate Actions sheet")
            
            self.logger.info(f"Successfully saved data to Excel file: {excel_file}")
            return excel_file, json_file
            
        except Exception as e:
            self.logger.error(f"Error saving Excel file: {str(e)}")
            raise

    
    def process_tickers(self, tickers: List[str]) -> tuple:
        """Main method to process all tickers"""
        # Remove duplicates while preserving order
        unique_tickers = list(dict.fromkeys(tickers))
        
        if len(unique_tickers) != len(tickers):
            self.logger.warning(f"Removed {len(tickers) - len(unique_tickers)} duplicate tickers")
        
        all_data = {}
        company_data_list = []
        officers_data_list = []
        actions_data_list = []
        
        for ticker in unique_tickers:
            # Fetch and process data
            stock_data = self.fetch_stock_data(ticker)
            all_data[ticker] = stock_data
            
            # Process for Excel sheets
            company_data = self.process_company_data(ticker, stock_data)
            company_data_list.append(company_data)
            
            officers_data = self.process_officers_data(ticker, stock_data)
            officers_data_list.extend(officers_data)
            
            actions_data = self.process_corporate_actions_data(ticker, stock_data)
            actions_data_list.extend(actions_data)
        
        # Log summary
        self.logger.info(f"Processed {len(unique_tickers)} tickers")
        self.logger.info(f"Total officers records: {len(officers_data_list)}")
        self.logger.info(f"Total corporate actions records: {len(actions_data_list)}")
        
        # Save all data
        excel_file, json_file = self.save_data(all_data, company_data_list, officers_data_list, actions_data_list)
        
        self.logger.info(f"Data processing complete. Files saved to {excel_file} and {json_file}")
        print(f"Data saved to {excel_file} and {json_file}")
        
        return excel_file, json_file
    

    def check_update(self,package_name: str):
        try:
            # Get installed version
            installed_version = importlib.metadata.version(package_name)

            # Get latest version from PyPI
            url = f"https://pypi.org/pypi/{package_name}/json"
            response = requests.get(url, timeout=5)

            if response.status_code != 200:
                print(f"❌ Could not fetch info for {package_name}")
                return False

            latest_version = response.json()["info"]["version"]

            if installed_version != latest_version:
                print(f"🔔 Update available for {package_name}: "
                      f"{installed_version} → {latest_version}")
                return True
            else:
                print(f"✅ {package_name} is up to date ({installed_version})")
                return False
            
        except Exception as e:
            print(f"⚠️ Error checking for updates: {e}")
            return False



def main():
    """Main function to run the stock data processor"""
    # List of tickers (removed duplicates: "DECK", "M", "TOST")
    tickers = [
        "AEO", "UAA", "DECK", "ANF", "PVH", "RL", "VFC", "LEVI", "BROS", "KDP",
        "SBUX", "TOST", "DIN", "NDSN", "M", "AMC", "CCL", "STZ",
        "SAM", "DASH", "SQCC", "SPG", "O", "USFD", "SYY", "CMG", "DRI",
        "BLMN", "TGT", "PLTR", "KLAR", "SBUX", "KMX", "CVNA", "AN", "AFRM"
    ]

    # Print the tickers list to verify
    print("Tickers list:", tickers)

    # Create processor and run
    processor = StockDataProcessor()

    if processor.check_update('yfinance'):
        os.system("pip install --upgrade yfinance")
        print("Please restart the script to use the updated version of yfinance.")
        return

    # input('Press any key to continue.....')
    processor.process_tickers(tickers)


if __name__ == "__main__":
    main()