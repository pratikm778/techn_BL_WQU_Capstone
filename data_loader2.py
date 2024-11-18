import pandas as pd
import vectorbt as vbt
from datetime import datetime, timedelta
import sys
from pandas.tseries.offsets import BDay

class SNP100DataLoader:
  """Data loader for S&P 100 stocks"""
  
  DEFAULT_TICKERS = [
      "NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "AVGO", 
      "COST", "NFLX", "ASML", "TMUS", "AMD", "CSCO", "PEP", "ADBE", "LIN", 
      "AZN", "TXN", "QCOM", "INTU", "ISRG", "AMGN", "CMCSA", "PDD", "BKNG", 
      "AMAT", "HON", "VRTX", "PANW", "ADP", "MU", "GILD", "ADI", "SBUX", 
      "MELI", "INTC", "LRCX", "KLAC", "MDLZ", "REGN", "CTAS", "SNPS", "CDNS", 
      "PYPL", "CRWD", "MRVL", "MAR", "CSX", "ORLY", "WDAY", "CHTR", "ADSK", 
      "FTNT", "TTD", "ROP", "PCAR", "NXPI", "TEAM", "FANG", "MNST", "CPRT", 
      "PAYX", "AEP", "ODFL", "ROST", "FAST", "KDP", "DDOG", "EA", "BKR", 
      "KHC", "MCHP", "VRSK", "CTSH", "LULU", "EXC", "XEL", "CCEP", "IDXX", 
      "ON", "CSGP", "ZS", "TTWO", "ANSS", "CDW", "DXCM", "BIIB", "ILMN", 
      "MDB", "WBD", "MRNA", "DLTR", "WBA"
  ]

  EXCLUDED_TICKERS = ["ARM", "ABNB", "CEG", "DASH", "GEHC", "GFS"]
  
  TREASURY_BOND = "^TYX"
  SP100_INDEX = "^OEX"
  END_DATE = pd.Timestamp('2024-11-16')

  def __init__(self):
      """Initialize the data loader"""
      self.tickers = [t.strip() for t in self.DEFAULT_TICKERS 
                     if t.strip() not in self.EXCLUDED_TICKERS]

  def get_start_date(self, start_input):
      """
      Get start date based on input type
      
      Parameters:
      -----------
      start_input : str, datetime, or int
          Can be:
          - datetime string ('2020-01-01')
          - datetime object
          - integer (number of days to look back from end date)
          
      Returns:
      --------
      datetime
          Calculated start date
      """
      if isinstance(start_input, (str, pd.Timestamp, datetime)):
          return pd.Timestamp(start_input)
      elif isinstance(start_input, int):
          return self.END_DATE - BDay(start_input)
      else:
          raise ValueError("start_input must be date string, datetime, or integer days")

  def download_single_ticker(self, ticker, start_date, interval='1d'):
      """Download data for a single ticker"""
      try:
          price_data = vbt.YFData.download(
              ticker,
              interval=interval,
              start=start_date,
              end=self.END_DATE,
              missing_index='drop'
          ).get("Close")

          df = pd.DataFrame(price_data)
          df.rename(columns={"Close": ticker}, inplace=True)
          df.index = pd.to_datetime(df.index.strftime("%Y-%m-%d"))
          return df

      except Exception as err:
          self.print_exception_detail(err, f"Error downloading {ticker}")
          return None

  def load_all_data(self, start_date, include_indices=True):
      """
      Load data for all stocks and indices
      
      Parameters:
      -----------
      start_date : datetime
          Start date for data download
      include_indices : bool
          Whether to include treasury bond and S&P 100 index
          
      Returns:
      --------
      pd.DataFrame
          DataFrame with closing prices for all securities
      """
      # Start with stock tickers
      df = pd.DataFrame()
      
      # Download stock data
      for ticker in self.tickers:
          try:
              if df.empty:
                  df = self.download_single_ticker(ticker, start_date)
              else:
                  ticker_data = self.download_single_ticker(ticker, start_date)
                  if ticker_data is not None:
                      df = df.join(ticker_data, how='outer')
          except Exception as err:
              self.print_exception_detail(err)

      if include_indices:
          # Download and merge Treasury Bond data
          treasury_data = self.download_single_ticker(self.TREASURY_BOND, start_date)
          if treasury_data is not None:
              df = df.join(treasury_data, how='inner')
              # Calculate 20-day percentage change for treasury
              df[f"{self.TREASURY_BOND}_pct_change_20"] = df[self.TREASURY_BOND].pct_change(20)

          # Download and merge S&P 100 data
          sp100_data = self.download_single_ticker(self.SP100_INDEX, start_date)
          if sp100_data is not None:
              df = df.join(sp100_data, how='inner')

      return df.sort_index()

  def get_data(self, start_input, include_indices=True):
      """
      Get data for specified date range
      
      Parameters:
      -----------
      start_input : str, datetime, or int
          Start date specification:
          - date string ('2020-01-01')
          - datetime object
          - number of trading days to look back from end date
      include_indices : bool
          Whether to include treasury bond and S&P 100 index data
          
      Returns:
      --------
      pd.DataFrame
          DataFrame with closing prices for specified date range
      """
      start_date = self.get_start_date(start_input)
      return self.load_all_data(start_date, include_indices)

  @staticmethod
  def print_exception_detail(err, *args):
      """Print detailed error information"""
      exc_type, exc_obj, exc_tb = sys.exc_info()
      if len(args):
          for _arg in args:
              print(_arg)
      print(f"Error Message: {err}")
      print(f"Error at line number: {exc_tb.tb_lineno}")

if __name__ == "__main__":
  
  
# Initialize the loader
    loader = SNP100DataLoader()

    # Get data using different start date specifications:

    # 1. Using date string
    # df = loader.get_data('2020-01-01')

    # 2. Using number of trading days
    df1 = loader.get_data(1298, include_indices=False)  # Last trading days from Nov 14, 2023

    # 3. Without indices if needed
    # df = loader.get_data('2019-01-01', include_indices=False)

    # print(f"Date range: {df.index.min()} to {df.index.max()}")
    # print(f"Columns included: {df.columns.tolist()}")

  
    print("\nData Summary:")
    print(f"Start Date: {df1.index.min()}")
    print(f"End Date: {df1.index.max()}")
    print(f"Number of trading days: {len(df1)}")
    print(f"Number of columns: {len(df1.columns)}")
    
    # Check if indices are present
    if '^TYX' in df1.columns:
        print("\nTreasury bond data included")
        print("20-day percentage change column present:", 
                f"{'^TYX_pct_change_20' in df1.columns}")
    
    if '^OEX' in df1.columns:
        print("S&P 100 index data included")