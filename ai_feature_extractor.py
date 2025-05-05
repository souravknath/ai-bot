import numpy as np
import pandas as pd
import logging

class AIFeatureExtractor:
    def __init__(self):
        """Initialize the AI Feature Extractor"""
        pass
        
    def extract_features(self, df):
        """Extract features for ML models"""
        if df is None or len(df) < 50:  # Require minimum amount of data
            return None
            
        features = pd.DataFrame(index=df.index)
        
        try:
            # Price-based features
            features['price_ma_ratio'] = df['close'] / df['SMA_50']
            features['price_volatility'] = df['close'].rolling(20).std() / df['close'].rolling(20).mean()
            
            # Volume features
            if 'volume' in df.columns:
                features['volume_ma_ratio'] = df['volume'] / df['volume'].rolling(20).mean()
                features['volume_price_corr'] = df['volume'].rolling(10).corr(df['close'])
            
            # Technical indicators features
            features['rsi'] = df['RSI']
            features['rsi_slope'] = df['RSI'].diff(5)
            
            # Add Bollinger Bands
            bollinger_window = 20
            features['bollinger_width'] = self._calculate_bollinger_width(df, bollinger_window)
            features['bollinger_pos'] = self._calculate_bollinger_position(df, bollinger_window)
            
            # Add MACD features
            features = self._add_macd_features(features, df)
            
            # Add momentum features
            features['momentum_1d'] = df['close'].pct_change(1)
            features['momentum_5d'] = df['close'].pct_change(5)
            features['momentum_10d'] = df['close'].pct_change(10)
            
            # Fill any NaN values
            features = features.fillna(0)
            
            return features
        except Exception as e:
            logging.error(f"Error extracting features: {str(e)}")
            return None

    def _calculate_bollinger_width(self, df, window=20):
        """Calculate Bollinger Band width"""
        try:
            std = df['close'].rolling(window).std()
            middle_band = df['close'].rolling(window).mean()
            upper_band = middle_band + 2 * std
            lower_band = middle_band - 2 * std
            return (upper_band - lower_band) / middle_band
        except Exception as e:
            logging.error(f"Error calculating Bollinger width: {str(e)}")
            return pd.Series(0, index=df.index)
        
    def _calculate_bollinger_position(self, df, window=20):
        """Calculate price position within Bollinger Bands (0 to 1)"""
        try:
            std = df['close'].rolling(window).std()
            middle_band = df['close'].rolling(window).mean()
            upper_band = middle_band + 2 * std
            lower_band = middle_band - 2 * std
            
            # Position from 0 (at or below lower band) to 1 (at or above upper band)
            position = (df['close'] - lower_band) / (upper_band - lower_band)
            return position.clip(0, 1)  # Limit to 0-1 range
        except Exception as e:
            logging.error(f"Error calculating Bollinger position: {str(e)}")
            return pd.Series(0.5, index=df.index)
        
    def _add_macd_features(self, features, df):
        """Add MACD indicator features"""
        try:
            # MACD Line = 12-period EMA - 26-period EMA
            ema12 = df['close'].ewm(span=12, adjust=False).mean()
            ema26 = df['close'].ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            
            # Signal Line = 9-period EMA of MACD Line
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            
            # MACD Histogram = MACD Line - Signal Line
            macd_histogram = macd_line - signal_line
            
            features['macd_line'] = macd_line
            features['macd_signal'] = signal_line
            features['macd_histogram'] = macd_histogram
            features['macd_divergence'] = self._calculate_macd_divergence(df, macd_line)
            
            return features
        except Exception as e:
            logging.error(f"Error adding MACD features: {str(e)}")
            return features
        
    def _calculate_macd_divergence(self, df, macd_line, window=10):
        """Calculate MACD divergence (bullish/bearish)"""
        # Calculate if price is making higher highs but MACD is making lower highs (bearish)
        # or if price is making lower lows but MACD is making higher lows (bullish)
        try:
            price_max = df['close'].rolling(window=window).max()
            price_min = df['close'].rolling(window=window).min()
            
            macd_max = macd_line.rolling(window=window).max()
            macd_min = macd_line.rolling(window=window).min()
            
            # Higher price but lower MACD (bearish divergence): -1
            # Lower price but higher MACD (bullish divergence): +1
            divergence = pd.Series(0, index=df.index)
            
            # Compare current with previous window
            for i in range(window*2, len(df)):
                prev_price_max = price_max.iloc[i-window]
                curr_price_max = price_max.iloc[i]
                
                prev_macd_max = macd_max.iloc[i-window]
                curr_macd_max = macd_max.iloc[i]
                
                prev_price_min = price_min.iloc[i-window]
                curr_price_min = price_min.iloc[i]
                
                prev_macd_min = macd_min.iloc[i-window]
                curr_macd_min = macd_min.iloc[i]
                
                # Bearish divergence
                if curr_price_max > prev_price_max and curr_macd_max < prev_macd_max:
                    divergence.iloc[i] = -1
                    
                # Bullish divergence
                elif curr_price_min < prev_price_min and curr_macd_min > prev_macd_min:
                    divergence.iloc[i] = 1
                    
            return divergence
        except Exception as e:
            logging.error(f"Error calculating MACD divergence: {str(e)}")
            return pd.Series(0, index=df.index)