import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import json

class CryptoDataPipeline:
    """
    End-to-end data pipeline for fetching and processing cryptocurrency data
    from CoinGecko API.
    """
    
    def __init__(self, coins=['bitcoin', 'ethereum'], vs_currency='usd'):
        self.coins = coins
        self.vs_currency = vs_currency
        self.base_url = 'https://api.coingecko.com/api/v3'
        self.data_dir = 'crypto_data'
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)
    
    def fetch_historical_data(self, coin_id, days=365):
        """
        Fetch historical market data for a specific coin.
        
        Args:
            coin_id: CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')
            days: Number of days of historical data (max 365 for free tier)
        
        Returns:
            pandas DataFrame with price, volume, and market cap data
        """
        endpoint = f'{self.base_url}/coins/{coin_id}/market_chart'
        params = {
            'vs_currency': self.vs_currency,
            'days': days,
            'interval': 'daily'
        }
        
        try:
            print(f"Fetching {days} days of data for {coin_id}...")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Convert to DataFrame
            df = pd.DataFrame({
                'timestamp': [x[0] for x in data['prices']],
                'price': [x[1] for x in data['prices']],
                'volume': [x[1] for x in data['total_volumes']],
                'market_cap': [x[1] for x in data['market_caps']]
            })
            
            # Convert timestamp to datetime
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['coin'] = coin_id
            df = df[['date', 'coin', 'price', 'volume', 'market_cap']]
            
            print(f"Successfully fetched {len(df)} records for {coin_id}")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {coin_id}: {e}")
            return None
    
    def fetch_current_data(self, coin_id):
        """
        Fetch current market data for a specific coin.
        
        Args:
            coin_id: CoinGecko coin ID
        
        Returns:
            Dictionary with current market data
        """
        endpoint = f'{self.base_url}/coins/markets'
        params = {
            'vs_currency': self.vs_currency,
            'ids': coin_id,
            'order': 'market_cap_desc',
            'sparkline': False
        }
        
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()[0]
            
            return {
                'date': datetime.now(),
                'coin': coin_id,
                'price': data['current_price'],
                'volume': data['total_volume'],
                'market_cap': data['market_cap'],
                'price_change_24h': data['price_change_percentage_24h'],
                'high_24h': data['high_24h'],
                'low_24h': data['low_24h']
            }
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching current data for {coin_id}: {e}")
            return None
    
    def process_data(self, df):
        """
        Process and clean the fetched data.
        
        Args:
            df: Raw DataFrame
        
        Returns:
            Processed DataFrame with additional features
        """
        # Remove duplicates
        df = df.drop_duplicates(subset=['date', 'coin'])
        
        # Sort by date
        df = df.sort_values('date').reset_index(drop=True)
        
        # Calculate daily returns
        df['daily_return'] = df.groupby('coin')['price'].pct_change()
        
        # Calculate moving averages
        df['ma_7'] = df.groupby('coin')['price'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )
        df['ma_30'] = df.groupby('coin')['price'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        
        # Calculate volatility (30-day rolling standard deviation of returns)
        df['volatility_30d'] = df.groupby('coin')['daily_return'].transform(
            lambda x: x.rolling(window=30, min_periods=1).std()
        )
        
        # Calculate cumulative returns (for portfolio value tracking)
        df['cumulative_return'] = df.groupby('coin')['daily_return'].transform(
            lambda x: (1 + x).cumprod()
        )
        
        return df
    
    def save_data(self, df, filename=None):
        """
        Save processed data to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Output filename (default: auto-generated with timestamp)
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'crypto_data_{timestamp}.csv'
        
        filepath = os.path.join(self.data_dir, filename)
        df.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")
        return filepath
    
    def load_data(self, filename):
        """
        Load data from CSV file.
        
        Args:
            filename: Name of file to load
        
        Returns:
            DataFrame
        """
        filepath = os.path.join(self.data_dir, filename)
        df = pd.read_csv(filepath)
        df['date'] = pd.to_datetime(df['date'])
        return df
    
    def run_pipeline(self, days=365, save=True):
        """
        Run the complete data pipeline for all coins.
        
        Args:
            days: Number of days of historical data to fetch
            save: Whether to save the data to file
        
        Returns:
            Processed DataFrame
        """
        all_data = []
        
        for coin in self.coins:
            # Fetch historical data
            df = self.fetch_historical_data(coin, days)
            
            if df is not None:
                all_data.append(df)
            
            # Rate limiting to respect API limits
            time.sleep(1)
        
        if not all_data:
            print("No data fetched successfully")
            return None
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Process data
        processed_df = self.process_data(combined_df)
        
        # Save data if requested
        if save:
            self.save_data(processed_df)
        
        print(f"\nPipeline completed successfully!")
        print(f"Total records: {len(processed_df)}")
        print(f"Date range: {processed_df['date'].min()} to {processed_df['date'].max()}")
        
        return processed_df


# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = CryptoDataPipeline(coins=['bitcoin', 'ethereum'])
    
    # Run the pipeline to fetch and process data
    df = pipeline.run_pipeline(days=365, save=True)
    
    # Display first five rows
    if df is not None:
        print(df.head(10))