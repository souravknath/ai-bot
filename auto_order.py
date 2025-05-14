#!/usr/bin/env python
import pandas as pd
import numpy as np
import sqlite3
import logging
import os
from datetime import datetime, timedelta
import json
import requests
from pathlib import Path
import dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f"auto_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

class AutoOrderPlacer:
    def __init__(self, db_path='stock_data.db', config_path='order_config.json'):
        """Initialize the automated order placer"""
        self.db_path = db_path
        self.config_path = config_path  # Keep for backward compatibility
        self.conn = None
        self.db = None
        self.config = {}  # Will be loaded from database
        self.order_history_file = "order_history.json"  # Still keep order history in JSON for now
        self.order_history = self.load_order_history()
        self.pending_signals = {}  # Stores stocks that need confirmation candle
        
        # Connect to database and load settings
        self.initialize_database_connection()
        
    def initialize_database_connection(self):
        """Initialize database connection and load settings"""
        from db_handler import DatabaseHandler
        
        self.db = DatabaseHandler(self.db_path)
        if self.db.connect():
            # Load configuration from database
            self.conn = self.db.conn  # For backward compatibility with existing code
            self.config = self.load_config()
            return True
        return False
        
    def load_config(self):
        """Load configuration from database or initialize defaults"""
        if not self.db:
            from db_handler import DatabaseHandler
            self.db = DatabaseHandler(self.db_path)
            self.db.connect()
            
        # Get all settings from database
        config = self.db.get_all_settings()
        
        # If no settings exist yet, initialize with defaults and save to database
        if not config:
            # Try to import from JSON first for backward compatibility
            config = self.import_config_from_json()
            if not config:
                config = self.create_default_config()
                
            # Save all default settings to database
            self.save_config_to_db(config)
        else:
            # Make sure api_secret is properly retrieved from database
            api_secret = self.db.get_setting('api_secret')
            if api_secret:
                config['api_secret'] = api_secret
                logging.info("Successfully loaded API secret from settings table")
                
        # Load watchlist
        config['enabled_symbols'] = self.db.get_watchlist()
        
        return config
    
    def import_config_from_json(self):
        """Import configuration from JSON file for backward compatibility"""
        import json
        from pathlib import Path
        
        config_file = Path(self.config_path)
        if config_file.exists():
            try:
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                logging.info(f"Imported configuration from {self.config_path}")
                return config
            except Exception as e:
                logging.error(f"Error importing configuration from JSON: {e}")
        return None
    
    def create_default_config(self):
        """Create default configuration"""
        logging.info("Creating default configuration")
        
        # Default configuration
        default_config = {
            "api_key": "",  # API key for broker integration
            "api_secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ4NDE5MTIyLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMTY1Nzk4NCJ9.mdqgKkG2brOROTZwbmjtgdMylrQ0xtLTKhA23RqrRVSlaiN9uc_VZTPt1Te5DPi7G2GT3QePBgr_kKYsVbbMhw",  # API secret for broker integration (was dhan_access_token)
            "broker": "demo",  # 'demo', 'dhan', or other broker name
            "capital_per_trade": 10000,  # Amount to invest per trade
            "max_positions": 5,  # Maximum number of concurrent positions
            "stop_loss_percent": 5,  # Stop loss percentage
            "target_percent": 10,  # Target profit percentage
            "enable_auto_orders": False,  # Auto orders disabled by default
            "confirmation_candles": 1,  # Number of confirmation candles required
            "order_type": "LIMIT",  # LIMIT or MARKET
            "limit_price_offset": 0.5,  # Percentage below current price for limit orders
            "time_in_force": "DAY",  # DAY, GTC, etc.
            "notification_email": "",  # Email for notifications
            # Dhan-specific settings
            "dhan_client_id": "1101657984",  # Dhan client ID
            "dhan_api_url": "https://api.dhan.co/v2",  # Dhan API URL
            "dhan_product_type": "CNC",  # CNC, INTRADAY, MARGIN, MTF
            "dhan_trailing_jump": 10  # Price jump for trailing stop loss
        }
        
        return default_config
    
    def save_config_to_db(self, config):
        """Save configuration to database"""
        if not self.db:
            self.initialize_database_connection()
            
        # Set descriptions for important settings
        descriptions = {
            "broker": "Broker to use for trading (demo, dhan, zerodha, etc)",
            "api_key": "API key for broker integration",
            "api_secret": "API secret for broker integration (Dhan access token)",
            "capital_per_trade": "Amount to invest per trade in currency units",
            "max_positions": "Maximum number of concurrent positions",
            "stop_loss_percent": "Stop loss percentage",
            "target_percent": "Target profit percentage",
            "enable_auto_orders": "Whether auto orders are enabled",
            "confirmation_candles": "Number of confirmation candles required",
            "order_type": "Order type (LIMIT or MARKET)",
            "limit_price_offset": "Percentage below current price for limit orders",
            "time_in_force": "Time in force for orders (DAY, GTC, etc)",
            "notification_email": "Email for notifications",
            "dhan_client_id": "Dhan client ID",
            "dhan_api_url": "Dhan API URL",
            "dhan_product_type": "Dhan product type (CNC, INTRADAY, MARGIN, MTF)",
            "dhan_trailing_jump": "Price jump for trailing stop loss"
        }
        
        # Save each setting to database
        for key, value in config.items():
            # Skip the enabled_symbols list - it's handled separately through watchlist
            if key == 'enabled_symbols':
                continue
                
            description = descriptions.get(key, "")
            self.db.set_setting(key, value, description)
            
        # Handle watchlist separately
        if 'enabled_symbols' in config and config['enabled_symbols']:
            # First clear the watchlist
            self.db.clear_watchlist()
            
            # Add each symbol to watchlist
            for symbol in config['enabled_symbols']:
                self.db.add_to_watchlist(symbol)
                
        logging.info("Configuration saved to database")
    
    def save_config(self):
        """Save configuration to database"""
        if not self.db:
            self.initialize_database_connection()
            
        # Save settings to database
        self.save_config_to_db(self.config)
        
    def update_setting(self, key, value):
        """Update a single setting"""
        if not self.db:
            self.initialize_database_connection()
            
        # Update in memory
        self.config[key] = value
        
        # Save to database
        self.db.set_setting(key, value)
        
    def add_to_watchlist(self, symbol):
        """Add a symbol to the watchlist"""
        if not self.db:
            self.initialize_database_connection()
            
        # Add to database
        result = self.db.add_to_watchlist(symbol)
        
        # Update in-memory config if successful
        if result:
            if 'enabled_symbols' not in self.config:
                self.config['enabled_symbols'] = []
            if symbol not in self.config['enabled_symbols']:
                self.config['enabled_symbols'].append(symbol)
                
        return result
        
    def remove_from_watchlist(self, symbol):
        """Remove a symbol from the watchlist"""
        if not self.db:
            self.initialize_database_connection()
            
        # Remove from database
        result = self.db.remove_from_watchlist(symbol)
        
        # Update in-memory config if successful
        if result and 'enabled_symbols' in self.config and symbol in self.config['enabled_symbols']:
            self.config['enabled_symbols'].remove(symbol)
            
        return result
    
    def load_order_history(self):
        """Load order history from JSON file"""
        history_file = Path(self.order_history_file)
        if history_file.exists():
            with open(self.order_history_file, 'r') as f:
                return json.load(f)
        else:
            # Create empty history
            empty_history = {"orders": []}
            with open(self.order_history_file, 'w') as f:
                json.dump(empty_history, f, indent=4)
            return empty_history
    
    def save_order_history(self):
        """Save order history to JSON file"""
        with open(self.order_history_file, 'w') as f:
            json.dump(self.order_history, f, indent=4)
            
    def connect_db(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            return False
            
    def close_db(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")
    
    def get_latest_candle(self, symbol):
        """Get the latest candle data for a symbol"""
        if not self.conn:
            if not self.connect_db():
                return None
                
        try:
            # Query to get the latest candle data
            query = """
                SELECT h.date, h.open, h.high, h.low, h.close, h.volume, s.symbol
                FROM history_data h
                JOIN stocks s ON h.stock_id = s.id
                WHERE s.symbol = ?
                ORDER BY h.date DESC
                LIMIT 1
            """
            
            df = pd.read_sql_query(query, self.conn, params=(symbol,))
            
            if len(df) == 0:
                return None
                
            return df.iloc[0].to_dict()
            
        except sqlite3.Error as e:
            logging.error(f"Error getting latest candle for {symbol}: {e}")
            return None
    
    def get_signal_stocks(self):
        """Get stocks with buy signals from the signal generator"""
        from generate_signals import SignalGenerator
        
        signal_gen = SignalGenerator()
        
        try:
            # Make sure the database connection is established
            if not signal_gen.conn:
                if not signal_gen.connect_db():
                    logging.error("Failed to connect to database in SignalGenerator")
                    return []
            
            # Get enabled symbols if any specified, otherwise use all stocks
            enabled_symbols = self.config.get("enabled_symbols", [])
            
            if enabled_symbols:
                signals_list = signal_gen.analyze_multiple_stocks(symbols=enabled_symbols)
            else:
                signals_list = signal_gen.analyze_multiple_stocks()
                
            # Filter for stocks with buy signals
            buy_signals = []
            for signals in signals_list:
                if signals.get('combined_signal_desc') == 'STRONG BUY':
                    symbol = signals.get('symbol')
                    
                    # Check if this is a new signal or already in pending signals
                    if symbol not in self.pending_signals:
                        buy_signals.append(signals)
                        
                        # Mark this symbol as pending confirmation
                        self.pending_signals[symbol] = {
                            'signal_date': signals.get('date'),
                            'confirmation_count': 0,
                            'close': signals.get('close')
                        }
            
            return buy_signals
            
        except Exception as e:
            logging.error(f"Error getting signal stocks: {e}", exc_info=True)
            return []
        finally:
            if signal_gen.conn:
                signal_gen.close_db()
    
    def check_confirmation_candles(self):
        """Check if any pending signals have received confirmation candle(s)"""
        confirmed_orders = []
        
        for symbol, signal_data in list(self.pending_signals.items()):
            # Get the latest candle
            latest_candle = self.get_latest_candle(symbol)
            
            if not latest_candle:
                continue
                
            # Check if this is a new candle after the signal
            if latest_candle['date'] > signal_data['signal_date']:
                # Increment confirmation count
                self.pending_signals[symbol]['confirmation_count'] += 1
                
                # Update the signal date to this candle's date
                self.pending_signals[symbol]['signal_date'] = latest_candle['date']
                
                logging.info(f"{symbol}: Confirmation candle #{self.pending_signals[symbol]['confirmation_count']} observed")
                
                # Check if we have enough confirmation candles
                if self.pending_signals[symbol]['confirmation_count'] >= self.config.get('confirmation_candles', 1):
                    # We have a confirmed signal!
                    confirmed_orders.append({
                        'symbol': symbol,
                        'entry_price': latest_candle['close'],
                        'signal_price': signal_data['close'],
                        'date': latest_candle['date']
                    })
                    
                    # Remove from pending signals
                    del self.pending_signals[symbol]
        
        return confirmed_orders
    
    def calculate_order_params(self, confirmed_order):
        """Calculate order parameters based on config"""
        symbol = confirmed_order['symbol']
        current_price = confirmed_order['entry_price']
        
        # Calculate position size
        capital = self.config.get('capital_per_trade', 10000)
        position_size = int(capital / current_price)
        
        # Calculate limit price if using limit orders
        limit_offset = self.config.get('limit_price_offset', 0.5) / 100  # convert percentage to decimal
        limit_price = round(current_price * (1 - limit_offset), 2)
        
        # Calculate stop loss and target
        stop_loss_percent = self.config.get('stop_loss_percent', 5) / 100
        target_percent = self.config.get('target_percent', 10) / 100
        
        stop_loss = round(current_price * (1 - stop_loss_percent), 2)
        target = round(current_price * (1 + target_percent), 2)
        
        return {
            'symbol': symbol,
            'position_size': position_size,
            'order_type': self.config.get('order_type', 'LIMIT'),
            'limit_price': limit_price,
            'current_price': current_price,
            'stop_loss': stop_loss,
            'target': target,
            'time_in_force': self.config.get('time_in_force', 'DAY')
        }
    
    def place_order(self, order_params):
        """Place an order with the broker (or simulate if demo)"""
        logging.info(f"Attempting to place order: {json.dumps(order_params)}")
        logging.info(f"Current broker: {self.config.get('broker')}")
        if self.config.get('broker') == 'demo':
            # Simulate order placement in demo mode
            logging.info(f"DEMO ORDER: {json.dumps(order_params)}")
            
            # Prepare order record
            order = {
                'symbol': order_params['symbol'],
                'quantity': order_params['position_size'],
                'price': order_params['current_price'],
                'limit_price': order_params['limit_price'],
                'order_type': order_params['order_type'],
                'stop_loss': order_params['stop_loss'],
                'target': order_params['target'],
                'time_in_force': order_params['time_in_force'],
                'status': 'simulated',
                'timestamp': datetime.now().isoformat(),
                'broker': 'demo'
            }
            
            # Add to order history
            self.order_history['orders'].append(order)
            self.save_order_history()
            
            return {
                'success': True,
                'order_id': f"demo_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                'message': "Demo order simulated successfully"
            }
        elif self.config.get('broker') == 'dhan':
            # Log before calling Dhan
            logging.info("Calling place_dhan_super_order...")
            return self.place_dhan_super_order(order_params)
        else:
            # Implement other broker integration here
            logging.warning("Broker integration not implemented - order not placed")
            return {
                'success': False,
                'message': "Broker integration not implemented"
            }
            
    def place_dhan_super_order(self, order_params):
        """Place a super order with Dhan broker"""
        try:
            logging.info(f"Placing Dhan Super Order for {order_params['symbol']}")
            
            # Check if required settings are available
            dhan_client_id = self.config.get('dhan_client_id')
            access_token = self.config.get('api_secret')
            api_url = self.config.get('dhan_api_url')
            
            # For Dhan we don't require api_key, only access_token
            if not dhan_client_id or not access_token:
                logging.error("Dhan client ID or access token not configured")
                return {'success': False, 'message': "Dhan credentials not configured"}
                
            # Get security ID for the symbol - this should come from your database
            security_id = self.get_security_id_for_symbol(order_params['symbol'])
            if not security_id:
                logging.error(f"Security ID not found for {order_params['symbol']}")
                return {'success': False, 'message': f"Security ID not found for {order_params['symbol']}"}
            
            # Determine order price based on order type
            price = order_params['limit_price'] if order_params['order_type'] == 'LIMIT' else None
            
            # Determine trailing stop loss settings
            trailing_jump = self.config.get('dhan_trailing_jump', 10)
            
            # Create a unique correlation ID
            correlation_id = f"auto_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Log the parameters for debugging
            logging.info(f"Security ID: {security_id}")
            logging.info(f"Order type: {order_params['order_type']}")
            logging.info(f"Quantity: {order_params['position_size']}")
            logging.info(f"Price: {price}")
            logging.info(f"Stop loss: {order_params['stop_loss']}")
            logging.info(f"Target: {order_params['target']}")
            
            # Use the direct http.client approach as in the working example
            try:
                import http.client
                
                logging.info("Using direct HTTP client implementation")
                conn = http.client.HTTPSConnection("api.dhan.co")
                
                # Format the payload exactly as in the working example
                payload_dict = {
                    "dhanClientId": dhan_client_id,
                    "correlationId": correlation_id,
                    "transactionType": "BUY",
                    "exchangeSegment": self.get_dhan_exchange_segment(order_params['symbol']),
                    "productType": self.config.get('dhan_product_type', 'CNC'),
                    "orderType": order_params['order_type'],
                    "securityId": security_id,
                    "quantity": order_params['position_size'],
                    "price": price,
                    "targetPrice": str(order_params['target']),
                    "stopLossPrice": str(order_params['stop_loss']),
                    "trailingJump": trailing_jump
                }
                
                # Remove None values from payload
                payload_dict = {k: v for k, v in payload_dict.items() if v is not None}
                
                payload = json.dumps(payload_dict)
                logging.info(f"Payload prepared: {payload}")
                
                headers = {
                    'Content-Type': 'application/json',
                    'access-token': access_token
                }
                
                logging.info("Making direct HTTP request to Dhan API")
                conn.request("POST", "/v2/super/orders", payload, headers)
                res = conn.getresponse()
                data = res.read()
                response_text = data.decode("utf-8")
                
                logging.info(f"Response status: {res.status}")
                logging.info(f"Response body: {response_text}")
                
                if res.status == 200:
                    response_data = json.loads(response_text)
                    logging.info(f"Dhan Super Order placed successfully: {response_data}")
                    
                    # Prepare order record
                    order = {
                        'symbol': order_params['symbol'],
                        'quantity': order_params['position_size'],
                        'price': order_params['current_price'] if order_params['order_type'] == 'MARKET' else order_params['limit_price'],
                        'order_type': order_params['order_type'],
                        'stop_loss': order_params['stop_loss'],
                        'target': order_params['target'],
                        'status': 'open',
                        'timestamp': datetime.now().isoformat(),
                        'broker': 'dhan',
                        'order_id': response_data.get('orderId'),
                        'security_id': security_id,
                        'trailing_jump': trailing_jump
                    }
                    
                    # Add to order history
                    self.order_history['orders'].append(order)
                    self.save_order_history()
                    
                    return {
                        'success': True,
                        'order_id': response_data.get('orderId'),
                        'message': "Dhan Super Order placed successfully"
                    }
                else:
                    logging.error(f"Failed to place Dhan Super Order: {response_text}")
                    return {
                        'success': False,
                        'status_code': res.status,
                        'message': f"Failed to place Dhan Super Order: {response_text}"
                    }
            except Exception as req_error:
                logging.error(f"HTTP client error: {req_error}", exc_info=True)
                
                # Fall back to requests library method if http.client fails
                logging.info("Falling back to requests library method")
                
                # Set up the headers
                headers = {
                    'Content-Type': 'application/json',
                    'access-token': access_token
                }
                
                # Prepare the super order request
                super_order_endpoint = f"{api_url}/super/orders"
                
                # Prepare the payload
                payload = {
                    'dhanClientId': dhan_client_id,
                    'correlationId': correlation_id,
                    'transactionType': 'BUY',
                    'exchangeSegment': self.get_dhan_exchange_segment(order_params['symbol']),
                    'productType': self.config.get('dhan_product_type', 'CNC'),
                    'orderType': order_params['order_type'],
                    'securityId': security_id,
                    'quantity': order_params['position_size'],
                    'price': str(price) if price else None,
                    'targetPrice': str(order_params['target']),
                    'stopLossPrice': str(order_params['stop_loss']),
                    'trailingJump': trailing_jump
                }
                
                # Remove None values from payload
                payload = {k: v for k, v in payload.items() if v is not None}
                
                logging.info(f"Dhan Super Order payload: {json.dumps(payload)}")
                
                # Make the API request using requests library
                response = requests.post(super_order_endpoint, headers=headers, json=payload)
                
                # Log full response for debugging
                logging.info(f"Dhan API response status: {response.status_code}")
                logging.info(f"Dhan API response body: {response.text}")
                
                # Check if the request was successful
                if response.status_code == 200:
                    response_data = response.json()
                    logging.info(f"Dhan Super Order placed successfully: {response_data}")
                    
                    # Prepare order record
                    order = {
                        'symbol': order_params['symbol'],
                        'quantity': order_params['position_size'],
                        'price': order_params['current_price'] if order_params['order_type'] == 'MARKET' else order_params['limit_price'],
                        'order_type': order_params['order_type'],
                        'stop_loss': order_params['stop_loss'],
                        'target': order_params['target'],
                        'status': 'open',
                        'timestamp': datetime.now().isoformat(),
                        'broker': 'dhan',
                        'order_id': response_data.get('orderId'),
                        'security_id': security_id,
                        'trailing_jump': trailing_jump
                    }
                    
                    # Add to order history
                    self.order_history['orders'].append(order)
                    self.save_order_history()
                    
                    return {
                        'success': True,
                        'order_id': response_data.get('orderId'),
                        'message': "Dhan Super Order placed successfully"
                    }
                else:
                    logging.error(f"Failed to place Dhan Super Order: {response.text}")
                    return {
                        'success': False,
                        'status_code': response.status_code,
                        'message': f"Failed to place Dhan Super Order: {response.text}"
                    }
        
        except Exception as e:
            logging.error(f"Error placing Dhan Super Order: {e}", exc_info=True)
            return {
                'success': False,
                'message': f"Error placing Dhan Super Order: {str(e)}"
            }
    
    def get_security_id_for_symbol(self, symbol_or_name):
        """Get the security ID for a given symbol or name from the database (robust match)"""
        if not self.conn:
            if not self.connect_db():
                return None
        import re
        def normalize(s):
            if not s:
                return ''
            return re.sub(r'[^A-Z0-9]', '', str(s).upper())
        try:
            cursor = self.conn.cursor()
            # Fetch all symbols and names with their security IDs
            cursor.execute("SELECT symbol, name, security_id FROM stocks")
            rows = cursor.fetchall()
            norm_input = normalize(symbol_or_name)
            # Try direct symbol match
            for symbol, name, secid in rows:
                if normalize(symbol) == norm_input:
                    return secid
            # Try direct name match
            for symbol, name, secid in rows:
                if normalize(name) == norm_input:
                    return secid
            # Try substring match
            for symbol, name, secid in rows:
                if norm_input in normalize(symbol) or normalize(symbol) in norm_input:
                    return secid
                if norm_input in normalize(name) or normalize(name) in norm_input:
                    return secid
            # Try acronym match
            def acronym(s):
                return ''.join(word[0] for word in re.findall(r'\b\w', s.upper())) if s else ''
            input_acronym = acronym(symbol_or_name)
            for symbol, name, secid in rows:
                if acronym(symbol) == input_acronym or acronym(name) == input_acronym:
                    return secid
            logging.warning(f"Security ID not found for symbol or name: {symbol_or_name}")
            return None
        except sqlite3.Error as e:
            logging.error(f"Database error getting security ID: {e}")
            return None
    
    def get_dhan_exchange_segment(self, symbol):
        """Determine the exchange segment for a symbol"""
        # This is a simple implementation that assumes NSE_EQ for all
        # In a real implementation, you would need to determine the correct exchange segment
        # based on your database or other logic
        
        # Default to NSE equities
        return "NSE_EQ"
    
    def send_notification(self, message):
        """Send notification about order placement"""
        email = self.config.get('notification_email')
        if not email:
            logging.info("No notification email configured")
            return
            
        try:
            # Simple logging of notification for now
            logging.info(f"NOTIFICATION: {message}")
            # TODO: Implement actual email sending or other notification method
        except Exception as e:
            logging.error(f"Error sending notification: {e}")
    
    def process_signals(self):
        """Main process to check for signals and place orders"""
        if not self.config.get('enable_auto_orders', False):
            logging.info("Auto orders are disabled in config")
            return
            
        # Connect to database if not connected
        if not self.conn:
            if not self.connect_db():
                return
                
        try:
            # Get stocks with buy signals
            buy_signals = self.get_signal_stocks()
            
            if buy_signals:
                logging.info(f"Found {len(buy_signals)} new buy signals: {[s['symbol'] for s in buy_signals]}")
            
            # Check for confirmation candles
            confirmed_orders = self.check_confirmation_candles()
            
            if confirmed_orders:
                logging.info(f"Found {len(confirmed_orders)} confirmed signals: {[o['symbol'] for o in confirmed_orders]}")
                
                # Check if we have reached max positions
                current_positions = len([o for o in self.order_history['orders'] if o.get('status') in ('simulated', 'filled', 'open')])
                max_positions = self.config.get('max_positions', 5)
                
                if current_positions >= max_positions:
                    logging.warning(f"Maximum positions ({max_positions}) reached - not placing new orders")
                    return
                
                # Process each confirmed order
                for confirmed in confirmed_orders:
                    # Calculate order parameters
                    order_params = self.calculate_order_params(confirmed)
                    
                    # Place the order
                    order_result = self.place_order(order_params)
                    
                    if order_result.get('success'):
                        # Send notification
                        notification = (
                            f"Buy order placed for {confirmed['symbol']} at {order_params['limit_price']}. "
                            f"Stop: {order_params['stop_loss']}, Target: {order_params['target']}, "
                            f"Quantity: {order_params['position_size']}"
                        )
                        self.send_notification(notification)
        
        except Exception as e:
            logging.error(f"Error in process_signals: {e}", exc_info=True)
        finally:
            # Close database connection
            self.close_db()

    def modify_dhan_super_order(self, order_id, leg_name, changes):
        """Modify a pending Dhan Super Order
        
        Args:
            order_id: The ID of the order to modify
            leg_name: The leg to modify (ENTRY_LEG, TARGET_LEG, STOP_LOSS_LEG)
            changes: Dictionary of parameters to change (quantity, price, stopLossPrice, targetPrice, trailingJump)
        """
        try:
            logging.info(f"Modifying Dhan Super Order {order_id}, leg {leg_name}")
            
            # Check if required settings are available
            dhan_client_id = self.config.get('dhan_client_id')
            access_token = self.config.get('api_secret')
            api_url = self.config.get('dhan_api_url')
            
            if not dhan_client_id or not access_token:
                logging.error("Dhan client ID or access token not configured")
                return {'success': False, 'message': "Dhan credentials not configured"}
            
            # Prepare the modify order request
            modify_endpoint = f"{api_url}/super/orders/{order_id}"
            
            # Set up the headers
            headers = {
                'Content-Type': 'application/json',
                'access-token': access_token
            }
            
            # Prepare the payload
            payload = {
                'dhanClientId': dhan_client_id,
                'orderId': order_id,
                'orderType': changes.get('orderType'),
                'legName': leg_name,
                'quantity': changes.get('quantity'),
                'price': changes.get('price'),
                'stopLossPrice': changes.get('stopLossPrice'),
                'targetPrice': changes.get('targetPrice'),
                'trailingJump': changes.get('trailingJump')
            }
            
            # Remove None values from payload
            payload = {k: v for k, v in payload.items() if v is not None}
            
            logging.info(f"Dhan Modify Order payload: {json.dumps(payload)}")
            
            # Make the API request
            response = requests.put(modify_endpoint, headers=headers, json=payload)
            
            # Check if the request was successful
            if response.status_code == 200:
                response_data = response.json()
                logging.info(f"Dhan Super Order modified successfully: {response_data}")
                
                # Update the order in order history
                for order in self.order_history['orders']:
                    if order.get('order_id') == order_id and order.get('broker') == 'dhan':
                        # Update the order details
                        if 'quantity' in changes:
                            order['quantity'] = changes['quantity']
                        if 'price' in changes:
                            order['price'] = changes['price']
                        if 'stopLossPrice' in changes:
                            order['stop_loss'] = changes['stopLossPrice']
                        if 'targetPrice' in changes:
                            order['target'] = changes['targetPrice']
                        if 'trailingJump' in changes:
                            order['trailing_jump'] = changes['trailingJump']
                        
                        # Save updated order history
                        self.save_order_history()
                        break
                
                return {
                    'success': True,
                    'order_id': order_id,
                    'message': "Dhan Super Order modified successfully"
                }
            else:
                logging.error(f"Failed to modify Dhan Super Order: {response.text}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'message': f"Failed to modify Dhan Super Order: {response.text}"
                }
        
        except Exception as e:
            logging.error(f"Error modifying Dhan Super Order: {e}", exc_info=True)
            return {
                'success': False,
                'message': f"Error modifying Dhan Super Order: {str(e)}"
            }
    
    def cancel_dhan_super_order_leg(self, order_id, leg_name):
        """Cancel a specific leg of a Dhan Super Order
        
        Args:
            order_id: The ID of the order to cancel
            leg_name: The leg to cancel (ENTRY_LEG, TARGET_LEG, STOP_LOSS_LEG)
        """
        try:
            logging.info(f"Cancelling Dhan Super Order {order_id}, leg {leg_name}")
            
            # Check if required settings are available
            dhan_client_id = self.config.get('dhan_client_id')
            access_token = self.config.get('api_secret')
            api_url = self.config.get('dhan_api_url')
            
            if not dhan_client_id or not access_token:
                logging.error("Dhan client ID or access token not configured")
                return {'success': False, 'message': "Dhan credentials not configured"}
            
            # Prepare the cancel order request
            cancel_endpoint = f"{api_url}/super/orders/{order_id}/{leg_name}"
            
            # Set up the headers
            headers = {
                'Content-Type': 'application/json',
                'access-token': access_token
            }
            
            # Make the API request (DELETE method)
            response = requests.delete(cancel_endpoint, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                response_data = response.json()
                logging.info(f"Dhan Super Order leg cancelled successfully: {response_data}")
                
                # Update order status in history if it was the main entry leg
                if leg_name == 'ENTRY_LEG':
                    for order in self.order_history['orders']:
                        if order.get('order_id') == order_id and order.get('broker') == 'dhan':
                            order['status'] = 'cancelled'
                            # Save updated order history
                            self.save_order_history()
                            break
                
                return {
                    'success': True,
                    'order_id': order_id,
                    'message': f"Dhan Super Order leg {leg_name} cancelled successfully"
                }
            else:
                logging.error(f"Failed to cancel Dhan Super Order leg: {response.text}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'message': f"Failed to cancel Dhan Super Order leg: {response.text}"
                }
        
        except Exception as e:
            logging.error(f"Error cancelling Dhan Super Order leg: {e}", exc_info=True)
            return {
                'success': False,
                'message': f"Error cancelling Dhan Super Order leg: {str(e)}"
            }
    
    def get_dhan_super_orders(self):
        """Get all super orders from Dhan broker"""
        try:
            logging.info("Retrieving all Dhan Super Orders")
            
            # Check if required settings are available
            dhan_client_id = self.config.get('dhan_client_id')
            access_token = self.config.get('api_secret')
            api_url = self.config.get('dhan_api_url')
            
            if not dhan_client_id or not access_token:
                logging.error("Dhan client ID or access token not configured")
                return {'success': False, 'message': "Dhan credentials not configured"}
            
            # Prepare the request
            orders_endpoint = f"{api_url}/super/orders"
            
            # Set up the headers
            headers = {
                'Content-Type': 'application/json',
                'access-token': access_token
            }
            
            # Set up params (dhanClientId is required)
            params = {
                'dhanClientId': dhan_client_id
            }
            
            # Make the API request
            response = requests.get(orders_endpoint, headers=headers, params=params)
            
            # Check if the request was successful
            if response.status_code == 200:
                orders_data = response.json()
                logging.info(f"Retrieved {len(orders_data)} Dhan Super Orders")
                
                # Update our local order history with the latest status from Dhan
                self.update_order_history_from_dhan(orders_data)
                
                return {
                    'success': True,
                    'orders': orders_data
                }
            else:
                logging.error(f"Failed to retrieve Dhan Super Orders: {response.text}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'message': f"Failed to retrieve Dhan Super Orders: {response.text}"
                }
        
        except Exception as e:
            logging.error(f"Error retrieving Dhan Super Orders: {e}", exc_info=True)
            return {
                'success': False,
                'message': f"Error retrieving Dhan Super Orders: {str(e)}"
            }
    
    def update_order_history_from_dhan(self, dhan_orders):
        """Update local order history with latest status from Dhan API"""
        # Create a map of order IDs for faster lookup
        order_map = {order.get('order_id'): i for i, order in enumerate(self.order_history['orders']) 
                     if order.get('broker') == 'dhan' and order.get('order_id')}
        
        # Update existing orders and add new ones
        for dhan_order in dhan_orders:
            order_id = dhan_order.get('orderId')
            
            if order_id in order_map:
                # Update existing order
                index = order_map[order_id]
                order = self.order_history['orders'][index]
                
                # Update status
                dhan_status = dhan_order.get('orderStatus')
                if dhan_status:
                    if dhan_status == 'TRADED':
                        order['status'] = 'filled'
                    elif dhan_status == 'PART_TRADED':
                        order['status'] = 'partially_filled'
                    elif dhan_status == 'CANCELLED':
                        order['status'] = 'cancelled'
                    elif dhan_status == 'REJECTED':
                        order['status'] = 'rejected'
                    elif dhan_status == 'PENDING':
                        order['status'] = 'open'
                
                # Update other details if available
                if 'remainingQuantity' in dhan_order:
                    order['remaining_quantity'] = dhan_order['remainingQuantity']
                if 'filledQty' in dhan_order:
                    order['filled_quantity'] = dhan_order['filledQty']
                if 'averageTradedPrice' in dhan_order:
                    order['average_price'] = dhan_order['averageTradedPrice']
                
                # Update leg details
                if 'legDetails' in dhan_order and dhan_order['legDetails']:
                    order['legs'] = dhan_order['legDetails']
            
        # Save the updated order history
        self.save_order_history()

    def load_dhan_credentials(self):
        """Load Dhan credentials from .env file"""
        # Try loading from both .env and .env-new files
        
        # First try the standard .env file
        dotenv.load_dotenv('.env')
        
        # Get the API key from environment
        dhan_api_key = os.getenv('DHAN_ACCESS_TOKEN') or os.getenv('DHAN_API_KEY')
        
        if not dhan_api_key:
            # If not found, try .env-new
            env_path = Path('.env-new')
            if (env_path.exists()):
                dotenv.load_dotenv('.env-new')
                dhan_api_key = os.getenv('DHAN_ACCESS_TOKEN') or os.getenv('DHAN_API_KEY')
        
        # If we found a key in either file, use it
        if dhan_api_key:
            try:
                # For simple version, just use the token directly
                self.config['broker'] = 'dhan'
                self.config['dhan_access_token'] = dhan_api_key
                
                # Try to extract client ID from the token if possible
                try:
                    import jwt
                    # JWT tokens have three parts separated by dots
                    # We don't need to verify the signature, just extract the payload
                    payload = jwt.decode(dhan_api_key, options={"verify_signature": False})
                    dhan_client_id = payload.get('dhanClientId')
                    
                    if dhan_client_id:
                        self.config['dhan_client_id'] = dhan_client_id
                except:
                    # If we couldn't extract client ID from token, use the configured one
                    logging.info("Could not extract client ID from token, using configured value")
                
                # Save the updated configuration
                self.save_config()
                
                logging.info(f"Successfully loaded Dhan credentials")
                return True
            except Exception as e:
                logging.error(f"Error processing Dhan credentials: {e}")
        else:
            logging.warning("Dhan API key not found in .env or .env-new files")
        
        return False

def main():
    """Main function to run auto order placer"""
    auto_order = AutoOrderPlacer()
    
    try:
        # Load Dhan credentials from .env-new file
        auto_order.load_dhan_credentials()
        
        # Process trading signals
        auto_order.process_signals()
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()