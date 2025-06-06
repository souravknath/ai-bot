import numpy as np
import pandas as pd
import logging
import requests
import time
import re
from datetime import datetime, timedelta
import os
import json

class SentimentAnalyzer:
    def __init__(self, api_key=None):
        """Initialize Sentiment Analyzer"""
        self.api_key = api_key
        self.cache_dir = 'sentiment_cache'
        self.cache_duration = 24  # hours
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def fetch_sentiment_data(self, symbol, days=7):
        """Fetch sentiment data from news and social media for a given stock symbol"""
        # First check if we have cached data
        cached_data = self._check_cache(symbol)
        if cached_data:
            logging.info(f'Using cached sentiment data for {symbol}')
            return cached_data
            
        # If no API key, generate mock sentiment data for demo purposes
        if not self.api_key:
            logging.warning(f'No API key provided, using simulated sentiment data for {symbol}')
            return self._generate_mock_sentiment(symbol, days)
        
        try:
            # In a real system, this would call an API to fetch actual sentiment data
            # For demonstration purposes, we'll use simulated data too but pretend it's from API
            logging.info(f'Fetching sentiment data for {symbol}')
            time.sleep(0.5)  # Simulate API call delay
            
            sentiment_data = self._generate_mock_sentiment(symbol, days)
            
            # Cache the results
            self._cache_data(symbol, sentiment_data)
            
            return sentiment_data
            
        except Exception as e:
            logging.error(f'Error fetching sentiment data: {str(e)}')
            # Fall back to mock data on error
            return self._generate_mock_sentiment(symbol, days)
    
    def _check_cache(self, symbol):
        """Check if we have fresh cached sentiment data for this symbol"""
        cache_file = os.path.join(self.cache_dir, f'{symbol}_sentiment.json')
        
        if not os.path.exists(cache_file):
            return None
            
        try:
            # Check if cache is still fresh
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_mod_time > timedelta(hours=self.cache_duration):
                logging.info(f'Sentiment cache for {symbol} is outdated')
                return None
                
            # Read cached data
            with open(cache_file, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            logging.error(f'Error reading sentiment cache: {str(e)}')
            return None
    
    def _cache_data(self, symbol, data):
        """Cache sentiment data for future use"""
        cache_file = os.path.join(self.cache_dir, f'{symbol}_sentiment.json')
        
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
            logging.info(f'Cached sentiment data for {symbol}')
            
        except Exception as e:
            logging.error(f'Error caching sentiment data: {str(e)}')
    
    def _generate_mock_sentiment(self, symbol, days=7):
        """Generate mock sentiment data for demonstration purposes"""
        # Use symbol's characters to create a predictable but symbol-specific pattern
        # This makes it so each stock gets consistent but different sentiment
        seed = sum(ord(c) for c in symbol)
        np.random.seed(seed)
        
        end_date = datetime.now()
        date_range = [(end_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
        date_range.reverse()  # Order from oldest to newest
        
        # Generate random sentiment scores with a slight trend
        base = np.random.uniform(0.4, 0.6)  # Base sentiment
        trend = np.random.uniform(-0.01, 0.01)  # Trend direction
        volatility = np.random.uniform(0.05, 0.15)  # Volatility of sentiment
        
        sentiment_scores = []
        for i in range(days):
            # Calculate sentiment with trend and noise
            score = base + trend * i + np.random.normal(0, volatility)
            # Ensure score is between 0 and 1
            score = max(0, min(1, score))
            sentiment_scores.append(score)
        
        # Generate mock news and social media mentions
        news_count = [int(np.random.poisson(5) * (1 + sentiment_scores[i])) for i in range(days)]
        social_count = [int(np.random.poisson(20) * (1 + sentiment_scores[i])) for i in range(days)]
        
        # Create sample news headlines for demonstration
        news_samples = [
            f'{symbol} announces quarterly results',
            f'Analysts upgrade {symbol} rating',
            f'New products from {symbol} receive positive reviews',
            f'Market reacts to {symbol} earnings report',
            f'{symbol} expands into new markets',
            f'Regulatory concerns affect {symbol}',
            f'{symbol} partners with industry leader'
        ]
        
        # Construct the sentiment data object
        sentiment_data = {
            'symbol': symbol,
            'generated_at': datetime.now().isoformat(),
            'daily_data': []
        }
        
        # Add daily data
        for i in range(days):
            # Select random news items
            daily_news = np.random.choice(
                news_samples, 
                size=min(len(news_samples), news_count[i]), 
                replace=False
            ).tolist() if news_count[i] > 0 else []
            
            sentiment_data['daily_data'].append({
                'date': date_range[i],
                'sentiment_score': round(sentiment_scores[i], 2),
                'news_count': news_count[i],
                'social_count': social_count[i],
                'news_samples': daily_news
            })
        
        # Calculate aggregate sentiment
        sentiment_data['aggregate'] = {
            'average_sentiment': round(sum(sentiment_scores) / len(sentiment_scores), 2),
            'latest_sentiment': round(sentiment_scores[-1], 2),
            'sentiment_trend': 'bullish' if trend > 0 else 'bearish' if trend < 0 else 'neutral',
            'total_news': sum(news_count),
            'total_social': sum(social_count)
        }
        
        return sentiment_data
    
    def analyze_sentiment(self, sentiment_data):
        """Analyze sentiment data to produce a signal"""
        if not sentiment_data:
            return {
                'sentiment_score': 0.5,  # Neutral score
                'sentiment_signal': 0,    # Neutral signal
                'sentiment_desc': 'NEUTRAL',
                'confidence': 0           # Zero confidence
            }
            
        try:
            # Get latest and average sentiment
            latest_sentiment = sentiment_data['aggregate']['latest_sentiment']
            avg_sentiment = sentiment_data['aggregate']['average_sentiment']
            
            # Calculate trend by comparing latest to average
            trend = latest_sentiment - avg_sentiment
            
            # Calculate weighted sentiment score (emphasize latest sentiment)
            weighted_score = 0.7 * latest_sentiment + 0.3 * avg_sentiment
            
            # Calculate confidence based on amount of data
            total_mentions = sentiment_data['aggregate']['total_news'] + sentiment_data['aggregate']['total_social']
            confidence = min(1.0, total_mentions / 100)  # Cap at 1.0
            
            # Determine signal
            if weighted_score > 0.6:
                signal = 1
                desc = 'BULLISH'
            elif weighted_score < 0.4:
                signal = -1
                desc = 'BEARISH'
            else:
                signal = 0
                desc = 'NEUTRAL'
                
            # Enhance signal with trend
            if signal == 1 and trend > 0.05:
                desc = 'STRONGLY BULLISH'
            elif signal == -1 and trend < -0.05:
                desc = 'STRONGLY BEARISH'
                
            return {
                'sentiment_score': weighted_score,
                'sentiment_signal': signal,
                'sentiment_desc': desc,
                'confidence': confidence,
                'news_count': sentiment_data['aggregate']['total_news'],
                'social_count': sentiment_data['aggregate']['total_social'],
                'trend': trend
            }
            
        except Exception as e:
            logging.error(f'Error analyzing sentiment: {str(e)}')
            return {
                'sentiment_score': 0.5,
                'sentiment_signal': 0,
                'sentiment_desc': 'NEUTRAL',
                'confidence': 0
            }

