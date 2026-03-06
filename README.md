# Public Equity Screening

A Python-based toolkit for fetching, processing, and analyzing public equity data for a curated list of consumer, retail, and hospitality stocks. The project pulls data from Yahoo Finance, saves it in structured formats, and runs comprehensive financial and governance analysis with visualizations.

---

## Overview

This project consists of two main scripts:

- **`Full_Finalized.py`** — Fetches live stock data from Yahoo Finance for a predefined list of tickers, processes it into structured records (company info, executive officers, corporate actions), and exports everything to JSON and Excel.
- - **`Data_Analysis.py`** — Loads the generated Excel file and performs in-depth statistical analysis, sector comparisons, executive compensation breakdowns, corporate actions trend analysis, and produces rich visualizations and a written report.
  - - **`start.bat`** — A Windows batch file that launches `Full_Finalized.py` with a single double-click.
   
    - ---

    ## Features

    ### Data Collection (`Full_Finalized.py`)
    - Fetches comprehensive stock data via `yfinance` for a list of ~35 tickers
    - - Extracts and renames 60+ fields into human-readable column names, including:
      -   - Price data (current price, 52-week high/low, moving averages)
          -   - Valuation metrics (P/E, P/B, EV/EBITDA, PEG ratio)
              -   - Financials (revenue, EBITDA, earnings, profit margins, cash flow)
                  -   - Governance & risk scores (audit risk, board risk, compensation risk, shareholder rights risk)
                      -   - Dividend & share data (yield, payout ratio, shares outstanding, float)
                          -   - Analyst sentiment (recommendation, target price, earnings estimates)
                              - - Organizes data into three sheets: **Company Data**, **Company Officers**, **Corporate Actions**
                                - - Saves output to `All Symbols/All_Tickers.xlsx` and `All Symbols/All_Tickers.json`
                                  - - Auto-detects and upgrades outdated `yfinance` installations
                                    - - Logs all activity to a timestamped log file
                                     
                                      - ### Data Analysis (`Data_Analysis.py`)
                                      - - Loads the Excel output and cleans/standardizes all three data sheets
                                        - - Generates a full data overview (record counts, market cap stats, compensation stats, date ranges)
                                          - - **Financial metrics analysis**: P/E ratios, revenue, profit margins, EV/EBITDA
                                            - - **Sector performance analysis**: Comparative breakdowns across industries
                                              - - **Executive compensation analysis**: Salary distributions and officer-level breakdowns
                                                - - **Corporate actions trends**: Event frequency and timeline analysis
                                                  - - **Visualizations**: Interactive Plotly charts and Seaborn/Matplotlib static plots
                                                    - - **Automated report**: Generates a written summary with data-driven recommendations
                                                     
                                                      - ---

                                                      ## Tickers Covered

                                                      The default ticker list covers consumer discretionary, food & beverage, retail, hospitality, and fintech sectors:

                                                      `AEO`, `UAA`, `DECK`, `ANF`, `PVH`, `RL`, `VFC`, `LEVI`, `BROS`, `KDP`, `SBUX`, `TOST`, `DIN`, `NDSN`, `M`, `AMC`, `CCL`, `STZ`, `SAM`, `DASH`, `SPG`, `O`, `USFD`, `SYY`, `CMG`, `DRI`, `BLMN`, `TGT`, `PLTR`, `KMX`, `CVNA`, `AN`, `AFRM`

                                                      ---

                                                      ## Requirements

                                                      - Python 3.8+
                                                      - - Dependencies:
                                                       
                                                        - ```
                                                          yfinance
                                                          pandas
                                                          numpy
                                                          openpyxl
                                                          requests
                                                          matplotlib
                                                          seaborn
                                                          plotly
                                                          ```

                                                          Install all dependencies with:

                                                          ```bash
                                                          pip install yfinance pandas numpy openpyxl requests matplotlib seaborn plotly
                                                          ```

                                                          ---

                                                          ## Usage

                                                          ### Step 1 — Fetch Stock Data

                                                          **Windows (double-click):**
                                                          ```
                                                          start.bat
                                                          ```

                                                          **Command line:**
                                                          ```bash
                                                          python Full_Finalized.py
                                                          ```

                                                          This will create an `All Symbols/` folder containing:
                                                          - `All_Tickers.xlsx` — Excel workbook with three sheets
                                                          - - `All_Tickers.json` — Full raw data in JSON format
                                                            - - A timestamped `.log` file
                                                             
                                                              - ### Step 2 — Run Analysis
                                                             
                                                              - ```bash
                                                                python Data_Analysis.py
                                                                ```

                                                                Point the analyzer at the generated Excel file to produce statistical summaries, charts, and a written report.

                                                                ---

                                                                ## Output Structure

                                                                ```
                                                                All Symbols/
                                                                ├── All_Tickers.xlsx        # Excel workbook (Company Data, Officers, Corporate Actions)
                                                                ├── All_Tickers.json        # Raw JSON data for all tickers
                                                                └── stock_data_log_YYYYMMDD_HHMMSS.log
                                                                ```

                                                                The Excel workbook contains three sheets:

                                                                | Sheet | Contents |
                                                                |---|---|
                                                                | Company Data | One row per ticker — prices, valuation, financials, governance scores |
                                                                | Company Officers | Officer names, titles, and compensation per company |
                                                                | Corporate Actions | Dividends, splits, and other corporate events with dates |

                                                                ---

                                                                ## Customization

                                                                To screen a different set of stocks, edit the `tickers` list in the `main()` function of `Full_Finalized.py`:

                                                                ```python
                                                                tickers = [
                                                                    "AAPL", "MSFT", "GOOGL",  # replace with your desired tickers
                                                                ]
                                                                ```

                                                                ---

                                                                ## License

                                                                This project is for personal and educational use. Stock data is sourced from Yahoo Finance via the `yfinance` library and is subject to Yahoo's terms of service.
