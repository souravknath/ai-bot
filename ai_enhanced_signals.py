import pandas as pd
import numpy as np
import logging
import os
import plotly.graph_objects as go
from datetime import datetime
from plotly.offline import plot

# Import components
from ai_feature_extractor import AIFeatureExtractor
from ai_signal_generator import AISignalGenerator
from sentiment_analyzer import SentimentAnalyzer

class AIEnhancedSignalGenerator:
    def __init__(self):
        """Initialize the AI Enhanced Signal Generator"""
        self.feature_extractor = AIFeatureExtractor()
        self.ai_model = AISignalGenerator()
        self.sentiment_analyzer = SentimentAnalyzer()
    
    def enhance_with_ai(self, df, symbol):
        """Enhance a stock dataframe with AI predictions"""
        if df is None or len(df) < 50:
            return df
        
        try:
            # Extract AI features
            features = self.feature_extractor.extract_features(df)
            
            if features is None:
                logging.warning(f'Failed to extract AI features for {symbol}')
                return df
            
            # Generate AI signals
            ai_predictions = self.ai_model.predict(features)
            
            if ai_predictions is None:
                logging.warning(f'Failed to generate AI predictions for {symbol}')
                return df
            
            # Add AI signals to the dataframe
            for i in range(len(df)):
                if i < len(ai_predictions['signal']):
                    df.loc[df.index[i], 'AI_Signal'] = ai_predictions['signal'][i]
                    df.loc[df.index[i], 'AI_Signal_Prob'] = ai_predictions['signal_prob'][i]
                    df.loc[df.index[i], 'AI_Signal_Desc'] = ai_predictions['signal_desc'][i]
            
            # Get the latest AI signal description to add to signals dict
            latest_ai_signal = df['AI_Signal_Desc'].iloc[-1] if 'AI_Signal_Desc' in df.columns else 'NEUTRAL'
            
            # Add AI signal description to the dataframe
            df.loc[df.index[-1], 'ai_signal_desc'] = latest_ai_signal
            
            logging.info(f'Enhanced {symbol} data with AI signals')
            return df
            
        except Exception as e:
            logging.error(f'Error enhancing signals with AI: {str(e)}')
            return df
    
    def add_sentiment_analysis(self, df, symbol):
        """Add sentiment analysis to the dataframe"""
        if df is None:
            return df
            
        try:
            # Fetch sentiment data
            sentiment_data = self.sentiment_analyzer.fetch_sentiment_data(symbol)
            
            if not sentiment_data:
                logging.warning(f'No sentiment data available for {symbol}')
                return df
            
            # Analyze sentiment
            sentiment_analysis = self.sentiment_analyzer.analyze_sentiment(sentiment_data)
            
            # Add sentiment score and signal to the dataframe
            df.loc[df.index[-1], 'sentiment_score'] = sentiment_analysis['sentiment_score']
            df.loc[df.index[-1], 'sentiment_signal'] = sentiment_analysis['sentiment_signal']
            df.loc[df.index[-1], 'sentiment_desc'] = sentiment_analysis['sentiment_desc']
            
            logging.info(f'Added sentiment analysis for {symbol}')
            return df, sentiment_analysis
            
        except Exception as e:
            logging.error(f'Error adding sentiment analysis: {str(e)}')
            return df, None
    
    def optimize_parameters(self, df):
        """Use ML to find optimal parameters for technical indicators"""
        if df is None or len(df) < 50:
            return {
                'ma_period': 50,
                'rsi_period': 14,
                'rsi_threshold': 50
            }
            
        try:
            # Extract features for parameter optimization
            features = self.feature_extractor.extract_features(df)
            
            if features is None:
                return {
                    'ma_period': 50,
                    'rsi_period': 14,
                    'rsi_threshold': 50
                }
                
            # Get optimal parameters for this stock
            optimal_params = self.ai_model.get_optimal_parameters(features)
            
            logging.info(f'Optimized parameters: MA={optimal_params["ma_period"]}, '
                        f'RSI period={optimal_params["rsi_period"]}, '
                        f'RSI threshold={optimal_params["rsi_threshold"]}')
            
            return optimal_params
            
        except Exception as e:
            logging.error(f'Error optimizing parameters: {str(e)}')
            return {
                'ma_period': 50,
                'rsi_period': 14,
                'rsi_threshold': 50
            }
    
    def create_ai_enhanced_chart(self, df, symbol, sentiment_data=None, output_dir='ai_signal_charts'):
        """Create chart with AI-enhanced signals"""
        if df is None or len(df) < 2:
            logging.error(f'Insufficient data for AI-enhanced chart for {symbol}')
            return None
            
        try:
            # Ensure output directory exists
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Convert date to datetime if it's not already
            if not pd.api.types.is_datetime64_dtype(df['date']):
                df['date'] = pd.to_datetime(df['date'])
                
            # Create figure with subplots: price chart, RSI, and AI confidence
            fig = go.Figure()
            
            # Add price candlestick chart
            fig.add_trace(
                go.Candlestick(
                    x=df['date'],
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close'],
                    name=symbol,
                    showlegend=False
                )
            )
            
            # Add 50-day SMA
            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df['SMA_50'],
                    line=dict(color='blue', width=1),
                    name='50-day MA'
                )
            )
            
            # Add AI Buy signals
            if 'AI_Signal' in df.columns:
                ai_buy = df[df['AI_Signal'] > 0]
                if not ai_buy.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=ai_buy['date'],
                            y=ai_buy['low'] * 0.97,  # Position below price
                            mode='markers',
                            marker=dict(symbol='star', size=16, color='lime'),
                            name='AI Buy Signal'
                        )
                    )
            
            # Add AI Sell signals
            if 'AI_Signal' in df.columns:
                ai_sell = df[(df['AI_Signal_Prob'] < 0.3) & (df['AI_Signal'] == 0)]
                if not ai_sell.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=ai_sell['date'],
                            y=ai_sell['high'] * 1.03,  # Position above price
                            mode='markers',
                            marker=dict(symbol='star', size=16, color='red'),
                            name='AI Sell Signal'
                        )
                    )
            
            # Add sentiment data if available
            if sentiment_data and 'sentiment_score' in df.columns:
                # We'll add the sentiment as a rectangle at the edge of the chart
                sentiment_score = df['sentiment_score'].iloc[-1]
                sentiment_desc = df['sentiment_desc'].iloc[-1] if 'sentiment_desc' in df.columns else 'NEUTRAL'
                
                # Choose color based on sentiment
                if sentiment_score > 0.6:
                    sentiment_color = 'rgba(0, 255, 0, 0.3)'  # Green for bullish
                elif sentiment_score < 0.4:
                    sentiment_color = 'rgba(255, 0, 0, 0.3)'  # Red for bearish
                else:
                    sentiment_color = 'rgba(211, 211, 211, 0.3)'  # Gray for neutral
                
                # Add sentiment annotation
                fig.add_annotation(
                    x=df['date'].iloc[-1],
                    y=df['high'].max(),
                    text=f'Sentiment: {sentiment_desc}<br>Score: {sentiment_score:.2f}',
                    showarrow=True,
                    arrowhead=1,
                    arrowsize=1,
                    arrowwidth=2,
                    bgcolor=sentiment_color,
                    opacity=0.8
                )
            
            # Update layout
            fig.update_layout(
                title=f'{symbol} - AI-Enhanced Technical Signals',
                xaxis_title='Date',
                yaxis_title='Price',
                height=800,
                xaxis_rangeslider_visible=False,
                template='plotly_white'
            )
            
            # Date for the filename
            current_date = datetime.now().strftime('%Y-%m-%d')
            filename = f'{output_dir}/{symbol}_ai_signals_{current_date}.html'
            
            # Save the chart
            plot(fig, filename=filename, auto_open=False)
            logging.info(f'AI-enhanced chart saved to {filename}')
            
            return filename
            
        except Exception as e:
            logging.error(f'Error creating AI-enhanced chart: {str(e)}')
            return None
    
    def get_latest_ai_enhanced_signals(self, df, sentiment_analysis=None):
        """Extract the latest signals including AI and sentiment data"""
        if df is None or len(df) < 2:
            return None
            
        # Get the most recent data point
        latest = df.iloc[-1]
        
        # Start with existing signals (assuming they're already in the dataframe)
        signals = {
            'close': latest['close'] if 'close' in latest else None,
            'ma_50': latest['SMA_50'] if 'SMA_50' in latest else None,
            'rsi': latest['RSI'] if 'RSI' in latest else None,
            'ma_signal_desc': latest.get('ma_signal_desc', 'NEUTRAL'),
            'rsi_signal_desc': latest.get('rsi_signal_desc', 'NEUTRAL'),
            'combined_signal_desc': latest.get('combined_signal_desc', 'NEUTRAL')
        }
        
        # Add AI signal if available
        if 'AI_Signal_Desc' in latest:
            signals['ai_signal_desc'] = latest['AI_Signal_Desc']
            signals['ai_signal_prob'] = latest.get('AI_Signal_Prob', 0.5)
        else:
            signals['ai_signal_desc'] = 'NEUTRAL'
            signals['ai_signal_prob'] = 0.5
            
        # Add sentiment data if available
        if sentiment_analysis:
            signals['sentiment_desc'] = sentiment_analysis['sentiment_desc']
            signals['sentiment_score'] = sentiment_analysis['sentiment_score']
            signals['sentiment_confidence'] = sentiment_analysis.get('confidence', 0)
        else:
            signals['sentiment_desc'] = 'NEUTRAL'
            signals['sentiment_score'] = 0.5
            signals['sentiment_confidence'] = 0
            
        # Generate comprehensive signal that combines traditional, AI, and sentiment
        self._add_comprehensive_signal(signals)
        
        return signals
        
    def _add_comprehensive_signal(self, signals):
        """Add a comprehensive signal that considers all signal sources"""
        # Assign scores to different signals
        traditional_score = 0
        if signals['combined_signal_desc'] == 'STRONG BUY':
            traditional_score = 1
        elif signals['combined_signal_desc'] == 'FRESH STRONG BUY':
            traditional_score = 2
        elif signals['combined_signal_desc'] == 'STRONG SELL':
            traditional_score = -1
        elif signals['combined_signal_desc'] == 'FRESH STRONG SELL':
            traditional_score = -2
            
        # AI score
        ai_score = 0
        ai_confidence = signals.get('ai_signal_prob', 0.5)
        
        if signals.get('ai_signal_desc') == 'AI BUY':
            ai_score = 1 + (ai_confidence - 0.7) * 3  # Scale from 1 to 2 based on confidence
        elif signals.get('ai_signal_desc') == 'AI SELL':
            ai_score = -1 - (0.3 - ai_confidence) * 3  # Scale from -1 to -2 based on confidence
            
        # Sentiment score
        sentiment_score = 0
        sentiment_confidence = signals.get('sentiment_confidence', 0)
        
        if signals.get('sentiment_desc') == 'BULLISH':
            sentiment_score = 0.5 * sentiment_confidence
        elif signals.get('sentiment_desc') == 'STRONGLY BULLISH':
            sentiment_score = 1 * sentiment_confidence
        elif signals.get('sentiment_desc') == 'BEARISH':
            sentiment_score = -0.5 * sentiment_confidence
        elif signals.get('sentiment_desc') == 'STRONGLY BEARISH':
            sentiment_score = -1 * sentiment_confidence
            
        # Calculate weighted final score
        # Traditional signals: 50%, AI signals: 30%, Sentiment: 20%
        final_score = 0.5 * traditional_score + 0.3 * ai_score + 0.2 * sentiment_score
        
        # Determine final signal
        if final_score >= 1.2:
            signals['ai_enhanced_signal'] = 'STRONG AI BUY'
        elif final_score >= 0.6:
            signals['ai_enhanced_signal'] = 'AI BUY'
        elif final_score <= -1.2:
            signals['ai_enhanced_signal'] = 'STRONG AI SELL'
        elif final_score <= -0.6:
            signals['ai_enhanced_signal'] = 'AI SELL'
        else:
            signals['ai_enhanced_signal'] = 'NEUTRAL'
            
        signals['ai_enhanced_score'] = final_score
        
        return signals