#!/usr/bin/env python
import logging
import argparse
from generate_signals import SignalGenerator
import pandas as pd
import os

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename='ai_signals_test.log'
    )
    # Also output to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def analyze_with_ai(symbol, days=100):
    """Analyze a stock with AI-enhanced signals"""
    signal_gen = SignalGenerator()
    
    try:
        # First analyze with traditional signals
        logging.info(f'Analyzing {symbol} with traditional signals...')
        signal_gen.use_ai = False  # Temporarily disable AI
        traditional_signals = signal_gen.analyze_stock(
            symbol=symbol, 
            days=days,
            show_chart=False
        )
        
        if not traditional_signals:
            logging.error(f'Could not generate traditional signals for {symbol}')
            return
            
        # Then analyze with AI-enhanced signals
        logging.info(f'Analyzing {symbol} with AI-enhanced signals...')
        signal_gen.use_ai = True  # Enable AI
        ai_signals = signal_gen.analyze_stock(
            symbol=symbol,
            days=days,
            show_chart=True
        )
        
        if not ai_signals:
            logging.error(f'Could not generate AI-enhanced signals for {symbol}')
            return
            
        # Print comparison of traditional vs AI signals
        print('\n' + '=' * 80)
        print(f'SIGNAL COMPARISON FOR {symbol}:')
        print('-' * 80)
        
        # Traditional signals
        print('Traditional Signals:')
        print(f"  Moving Average Signal: {traditional_signals.get('ma_signal_desc', 'N/A')}")
        print(f"  RSI Signal: {traditional_signals.get('rsi_signal_desc', 'N/A')}")
        print(f"  Combined Signal: {traditional_signals.get('combined_signal_desc', 'NEUTRAL')}")
        print()
        
        # AI-enhanced signals
        print('AI-Enhanced Signals:')
        print(f"  AI Signal: {ai_signals.get('ai_signal_desc', 'N/A')}")
        print(f"  AI Signal Probability: {ai_signals.get('ai_signal_prob', 0):.2f}")
        print(f"  Sentiment: {ai_signals.get('sentiment_desc', 'N/A')} (Score: {ai_signals.get('sentiment_score', 0):.2f})")
        print(f"  Final AI-Enhanced Signal: {ai_signals.get('ai_enhanced_signal', 'N/A')} (Score: {ai_signals.get('ai_enhanced_score', 0):.2f})")
        print()
        
        # Parameter optimization
        print('Parameter Optimization:')
        print(f"  MA Period: {ai_signals.get('ma_period', 50)}")
        print(f"  RSI Period: {ai_signals.get('rsi_period', 14)}")
        print(f"  RSI Threshold: {ai_signals.get('rsi_threshold', 50)}")
        print('=' * 80)
        
        return ai_signals
        
    except Exception as e:
        logging.error(f'Error analyzing {symbol}: {str(e)}')
    finally:
        signal_gen.close_db()

def analyze_multiple_stocks(symbols, days=100):
    """Analyze multiple stocks with AI-enhanced signals"""
    results = []
    
    for symbol in symbols:
        signals = analyze_with_ai(symbol, days)
        if signals:
            results.append(signals)
            
    # Create a summary of the analysis
    print('\n\nSUMMARY OF AI-ENHANCED SIGNALS:')
    print('-' * 80)
    
    # Create a table
    rows = []
    for signal in results:
        symbol = signal.get('symbol', 'Unknown')
        price = signal.get('close', 0)
        trad_signal = signal.get('combined_signal_desc', 'NEUTRAL')
        ai_signal = signal.get('ai_signal_desc', 'NEUTRAL')
        sentiment = signal.get('sentiment_desc', 'NEUTRAL')
        final_signal = signal.get('ai_enhanced_signal', 'NEUTRAL')
        score = signal.get('ai_enhanced_score', 0)
        
        rows.append({
            'Symbol': symbol,
            'Price': price,
            'Traditional': trad_signal,
            'AI Signal': ai_signal,
            'Sentiment': sentiment,
            'Final Signal': final_signal,
            'Score': score
        })
    
    if rows:
        df = pd.DataFrame(rows)
        print(df.to_string(index=False))
        
        # Save the results to CSV
        csv_path = 'ai_signals_summary.csv'
        df.to_csv(csv_path, index=False)
        print(f'\nResults saved to {csv_path}')
    else:
        print('No results to display')

def main():
    """Main function to run the script"""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Test AI-enhanced signal generation')
    parser.add_argument('--symbol', help='Stock symbol to analyze')
    parser.add_argument('--list', action='store_true', help='Analyze multiple top stocks')
    parser.add_argument('--days', type=int, default=100, help='Number of days of historical data')
    parser.add_argument('--symbols', nargs='+', help='A list of symbols to analyze')
    
    args = parser.parse_args()
    
    if args.symbol:
        # Single stock analysis
        analyze_with_ai(args.symbol, args.days)
    elif args.symbols:
        # Analyze multiple specified stocks
        analyze_multiple_stocks(args.symbols, args.days)
    elif args.list:
        # Use the SignalGenerator to get top stocks
        signal_gen = SignalGenerator()
        try:
            query = """
                SELECT DISTINCT s.symbol, s.security_id
                FROM stocks s
                JOIN history_data h ON s.id = h.stock_id
                GROUP BY s.id
                ORDER BY COUNT(h.id) DESC
                LIMIT 10
            """
            df_stocks = pd.read_sql_query(query, signal_gen.conn)
            symbols = df_stocks['symbol'].tolist()
            analyze_multiple_stocks(symbols, args.days)
        finally:
            signal_gen.close_db()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()

