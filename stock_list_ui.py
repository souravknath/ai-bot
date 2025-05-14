import customtkinter as ctk
import sqlite3
import logging
import pandas as pd
import json  # Add missing import for JSON handling
from tkinter import ttk
import tkinter as tk
import plotly.graph_objects as go
from plotly.offline import plot
import webbrowser
import os
from datetime import datetime, timedelta
from stock_fetcher import StockFetcher
# Import SignalGenerator class
from generate_signals import SignalGenerator
from screener_auto_order import fetch_screener_stocks
from auto_order import AutoOrderPlacer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StockListApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Stock Database Viewer")
        self.root.geometry("1000x600")
        
        # Set appearance mode and theme
        ctk.set_appearance_mode("System")  # "System", "Dark" or "Light"
        ctk.set_default_color_theme("blue")  # "blue", "green", "dark-blue"
        
        # Connect to the database
        try:
            self.conn = sqlite3.connect("stock_data.db")
            logging.info("Connected to database successfully")
        except sqlite3.Error as e:
            logging.error(f"Error connecting to database: {e}")
            tk.messagebox.showerror("Database Error", f"Could not connect to database: {e}")
            exit(1)
        
        # Initialize stock fetcher and signal generator
        self.stock_fetcher = StockFetcher()
        self.signal_generator = SignalGenerator()
        self.signal_generator.connect_db()
        
        # Create main frame
        self.main_frame = ctk.CTkFrame(root)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Create search frame
        self.search_frame = ctk.CTkFrame(self.main_frame)
        self.search_frame.pack(fill="x", padx=10, pady=10)
        
        # Search components
        self.search_label = ctk.CTkLabel(self.search_frame, text="Search:")
        self.search_label.pack(side="left", padx=(5, 0))
        
        self.search_var = tk.StringVar()
        self.search_entry = ctk.CTkEntry(self.search_frame, width=200, textvariable=self.search_var)
        self.search_entry.pack(side="left", padx=5)
        
        self.search_button = ctk.CTkButton(self.search_frame, text="Search", command=self.search_stocks)
        self.search_button.pack(side="left", padx=5)
        
        self.clear_button = ctk.CTkButton(self.search_frame, text="Clear", command=self.clear_search)
        self.clear_button.pack(side="left", padx=5)
        
        # View Chart button
        self.chart_button = ctk.CTkButton(self.search_frame, text="View Chart", command=self.view_chart)
        self.chart_button.pack(side="left", padx=5)
        
        # Generate Signals button
        self.signals_button = ctk.CTkButton(self.search_frame, text="Generate Signals", command=self.generate_signals)
        self.signals_button.pack(side="left", padx=5)
        
        # Add Auto Order button
        self.auto_order_btn = ctk.CTkButton(self.search_frame, text="Auto Order Settings", command=self.show_auto_order_config)
        self.auto_order_btn.pack(side="left", padx=5)
        
        # Add Save Settings button for quick saving auto orders config
        self.save_settings_btn = ctk.CTkButton(self.search_frame, text="Save Settings", 
                                        fg_color="#009900", hover_color="#006600",
                                        command=self.quick_save_settings)
        self.save_settings_btn.pack(side="left", padx=5)
        
        # Status label
        self.status_var = tk.StringVar()
        self.status_label = ctk.CTkLabel(self.search_frame, textvariable=self.status_var)
        self.status_label.pack(side="right", padx=10)
        
        # Second row for signal filters
        self.filter_frame = ctk.CTkFrame(self.main_frame)
        self.filter_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        # Signal filter components
        ctk.CTkLabel(self.filter_frame, text="Signal Filter:").pack(side="left", padx=5)
        
        self.signal_filter_var = tk.StringVar(value="All")
        signal_options = ["All", "Buy Signal"]  # Simplified options to show only Buy signals
        signal_dropdown = ctk.CTkOptionMenu(self.filter_frame, values=signal_options, variable=self.signal_filter_var, command=self.filter_by_signal)
        signal_dropdown.pack(side="left", padx=5)

        # Create a tabbed view for stocks and signals
        self.tab_view = ctk.CTkTabview(self.main_frame)
        self.tab_view.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Create tabs
        self.all_stocks_tab = self.tab_view.add("All Stocks")
        self.orders_tab = self.tab_view.add("Orders")
        self.signals_tab = self.tab_view.add("Signals")  # New tab
        
        # Create treeview frame for All Stocks tab
        self.tree_frame = ctk.CTkFrame(self.all_stocks_tab)
        self.tree_frame.pack(fill="both", expand=True)

        # Create treeview frame for Orders tab
        self.orders_tree_frame = ctk.CTkFrame(self.orders_tab)
        self.orders_tree_frame.pack(fill="both", expand=True)
        
        # Create Treeview with additional columns for signals in All Stocks tab
        self.tree = ttk.Treeview(self.tree_frame, columns=("id", "security_id", "symbol", "name", "exchange", 
                                                    "instrument", "added_date", "last_updated", "ma_signal", 
                                                    "rsi_signal", "combined_signal"))
        
        # Define columns for All Stocks tab
        self.tree.column("#0", width=0, stretch=False)  # Hide the first column
        self.tree.column("id", width=50, anchor="center")
        self.tree.column("security_id", width=100, anchor="center")
        self.tree.column("symbol", width=100, anchor="center")
        self.tree.column("name", width=150, anchor="w")
        self.tree.column("exchange", width=80, anchor="center")
        self.tree.column("instrument", width=80, anchor="center")
        self.tree.column("added_date", width=100, anchor="center")
        self.tree.column("last_updated", width=100, anchor="center")
        self.tree.column("ma_signal", width=80, anchor="center")
        self.tree.column("rsi_signal", width=80, anchor="center")
        self.tree.column("combined_signal", width=100, anchor="center")
        
        # Define column headings for All Stocks tab
        self.tree.heading("id", text="ID", command=lambda: self.sort_column("id", False))
        self.tree.heading("security_id", text="Security ID", command=lambda: self.sort_column("security_id", False))
        self.tree.heading("symbol", text="Symbol", command=lambda: self.sort_column("symbol", False))
        self.tree.heading("name", text="Name", command=lambda: self.sort_column("name", False))
        self.tree.heading("exchange", text="Exchange", command=lambda: self.sort_column("exchange", False))
        self.tree.heading("instrument", text="Instrument", command=lambda: self.sort_column("instrument", False))
        self.tree.heading("added_date", text="Added Date", command=lambda: self.sort_column("added_date", False))
        self.tree.heading("last_updated", text="Last Updated", command=lambda: self.sort_column("last_updated", False))
        self.tree.heading("ma_signal", text="MA Signal", command=lambda: self.sort_column("ma_signal", False))
        self.tree.heading("rsi_signal", text="RSI Signal", command=lambda: self.sort_column("rsi_signal", False))
        self.tree.heading("combined_signal", text="Combined", command=lambda: self.sort_column("combined_signal", False))
        
        # Add double-click binding for quick chart view
        self.tree.bind("<Double-1>", lambda event: self.view_chart())
        
        # Add scrollbars to All Stocks tab
        self.y_scrollbar = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree.yview)
        self.x_scrollbar = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.y_scrollbar.set, xscrollcommand=self.x_scrollbar.set)
        
        # Pack scrollbars and treeview for All Stocks tab
        self.y_scrollbar.pack(side="right", fill="y")
        self.x_scrollbar.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)
        
        # Create Treeview for Orders tab
        self.orders_tree = ttk.Treeview(self.orders_tree_frame, columns=(
            "symbol", "broker", "quantity", "price", "order_type", 
            "stop_loss", "target", "status", "timestamp"))
        
        # Define columns for Orders tab
        self.orders_tree.column("#0", width=0, stretch=False)  # Hide the first column
        self.orders_tree.column("symbol", width=100, anchor="center")
        self.orders_tree.column("broker", width=80, anchor="center")
        self.orders_tree.column("quantity", width=80, anchor="center")
        self.orders_tree.column("price", width=100, anchor="center")
        self.orders_tree.column("order_type", width=80, anchor="center")
        self.orders_tree.column("stop_loss", width=100, anchor="center")
        self.orders_tree.column("target", width=100, anchor="center")
        self.orders_tree.column("status", width=100, anchor="center")
        self.orders_tree.column("timestamp", width=150, anchor="center")
        
        # Define column headings for Orders tab
        self.orders_tree.heading("symbol", text="Symbol", command=lambda: self.sort_orders_column("symbol", False))
        self.orders_tree.heading("broker", text="Broker", command=lambda: self.sort_orders_column("broker", False))
        self.orders_tree.heading("quantity", text="Quantity", command=lambda: self.sort_orders_column("quantity", False))
        self.orders_tree.heading("price", text="Price", command=lambda: self.sort_orders_column("price", False))
        self.orders_tree.heading("order_type", text="Order Type", command=lambda: self.sort_orders_column("order_type", False))
        self.orders_tree.heading("stop_loss", text="Stop Loss", command=lambda: self.sort_orders_column("stop_loss", False))
        self.orders_tree.heading("target", text="Target", command=lambda: self.sort_orders_column("target", False))
        self.orders_tree.heading("status", text="Status", command=lambda: self.sort_orders_column("status", False))
        self.orders_tree.heading("timestamp", text="Date/Time", command=lambda: self.sort_orders_column("timestamp", False))
        
        # Add double-click binding for quick chart view
        self.orders_tree.bind("<Double-1>", lambda event: self.view_order_chart())
        
        # Add scrollbars to Orders tab
        self.orders_y_scrollbar = ttk.Scrollbar(self.orders_tree_frame, orient="vertical", command=self.orders_tree.yview)
        self.orders_x_scrollbar = ttk.Scrollbar(self.orders_tree_frame, orient="horizontal", command=self.orders_tree.xview)
        self.orders_tree.configure(yscrollcommand=self.orders_y_scrollbar.set, xscrollcommand=self.orders_x_scrollbar.set)
        
        # Pack scrollbars and treeview for Orders tab
        self.orders_y_scrollbar.pack(side="right", fill="y")
        self.orders_x_scrollbar.pack(side="bottom", fill="x")
        self.orders_tree.pack(fill="both", expand=True)
        
        # Create treeview frame for Signals tab
        self.signals_tree_frame = ctk.CTkFrame(self.signals_tab)
        self.signals_tree_frame.pack(fill="both", expand=True)
        
        # Create Treeview for Screener signals
        self.signals_tree = ttk.Treeview(self.signals_tree_frame, columns=("symbol", "name", "cmp"))
        self.signals_tree.column("#0", width=0, stretch=False)
        self.signals_tree.column("symbol", width=100, anchor="center")
        self.signals_tree.column("name", width=200, anchor="w")
        self.signals_tree.column("cmp", width=100, anchor="center")
        self.signals_tree.heading("symbol", text="Symbol")
        self.signals_tree.heading("name", text="Name")
        self.signals_tree.heading("cmp", text="CMP")
        self.signals_tree.pack(fill="both", expand=True)
        
        # Add refresh button for signals
        self.refresh_signals_btn = ctk.CTkButton(self.signals_tab, text="Refresh Signals", command=self.load_screener_signals)
        self.refresh_signals_btn.pack(pady=10)
        # Add Place Auto Orders button
        self.place_orders_btn = ctk.CTkButton(self.signals_tab, text="Place Auto Orders for Screener Stocks", command=self.place_auto_orders_for_screener)
        self.place_orders_btn.pack(pady=10)
        
        # Status bar at bottom
        self.bottom_frame = ctk.CTkFrame(self.main_frame, height=30)
        self.bottom_frame.pack(fill="x", pady=(0, 5))
        
        self.count_var = tk.StringVar()
        self.count_label = ctk.CTkLabel(self.bottom_frame, textvariable=self.count_var)
        self.count_label.pack(side="left", padx=10)
        
        # Chart options
        self.chart_options_frame = ctk.CTkFrame(self.bottom_frame)
        self.chart_options_frame.pack(side="right", padx=10)
        
        ctk.CTkLabel(self.chart_options_frame, text="Period:").pack(side="left", padx=(0, 5))
        
        self.period_var = tk.StringVar(value="1 Month")
        period_options = ["1 Day", "1 Week", "1 Month", "3 Months", "6 Months", "1 Year"]
        period_dropdown = ctk.CTkOptionMenu(self.chart_options_frame, values=period_options, variable=self.period_var)
        period_dropdown.pack(side="left", padx=5)
        
        # Store signals data
        self.signals_data = {}
        
        # Create right-click menu for tree
        self.context_menu = tk.Menu(self.tree, tearoff=0)
        self.context_menu.add_command(label="View Regular Chart", command=self.view_chart)
        self.context_menu.add_command(label="View Signal Chart", command=self.view_signal_chart)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Generate Signals for Selected", command=self.generate_signals_for_selected)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Add to Auto Order Watchlist", command=self.add_to_auto_order_watchlist)
        
        # Bind right-click to tree
        self.tree.bind("<Button-3>", self.show_context_menu)
        
        # Load data initially
        self.load_data()
        # Load Screener signals initially
        self.load_screener_signals()
        
        # Show broker setting in status bar
        try:
            from db_handler import DatabaseHandler
            db = DatabaseHandler()
            if db.connect():
                db.set_setting('broker', 'dhan', 'Broker to use for trading (demo, dhan, zerodha, etc)')
                broker = db.get_all_settings().get('broker', 'demo')
                self.status_var.set(f"Current broker: {broker}")
        except Exception:
            pass
    
    def load_data(self):
        """Load all stock data from the database"""
        try:
            # Clear existing data
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # Fetch data from database
            query = """
                SELECT id, security_id, symbol, name, exchange_segment, instrument, added_date, last_updated 
                FROM stocks ORDER BY symbol
            """
            df = pd.read_sql_query(query, self.conn)
            
            # Add data to treeview
            for _, row in df.iterrows():
                self.tree.insert("", "end", values=tuple(row))
            
            # Update status
            self.count_var.set(f"Total Stocks: {len(df)}")
            self.status_var.set("Data loaded successfully")
            
            # Load order data for Orders tab
            self.load_orders()
            
            logging.info(f"Loaded {len(df)} stocks from database")
        except sqlite3.Error as e:
            logging.error(f"Error loading data: {e}")
            self.status_var.set(f"Error: {e}")
    
    def search_stocks(self):
        """Search stocks based on user input"""
        search_term = self.search_var.get().strip()
        if not search_term:
            self.load_data()
            return
        
        try:
            # Clear existing data
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # Fetch filtered data
            query = """
                SELECT id, security_id, symbol, name, exchange_segment, instrument, added_date, last_updated 
                FROM stocks 
                WHERE symbol LIKE ? OR name LIKE ? OR security_id LIKE ?
                ORDER BY symbol
            """
            search_param = f"%{search_term}%"
            df = pd.read_sql_query(query, self.conn, params=(search_param, search_param, search_param))
            
            # Add data to treeview
            for _, row in df.iterrows():
                self.tree.insert("", "end", values=tuple(row))
            
            # Update status
            self.count_var.set(f"Found Stocks: {len(df)}")
            self.status_var.set(f"Found {len(df)} matching stocks")
            
            logging.info(f"Found {len(df)} stocks matching '{search_term}'")
        except sqlite3.Error as e:
            logging.error(f"Error searching data: {e}")
            self.status_var.set(f"Error: {e}")
    
    def clear_search(self):
        """Clear search and reload all data"""
        self.search_var.set("")
        self.load_data()
    
    def sort_column(self, col, reverse):
        """Sort treeview by clicking on column headers"""
        data_list = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        try:
            # Try to sort numerically if possible
            data_list.sort(key=lambda x: int(x[0]), reverse=reverse)
        except ValueError:
            # Otherwise sort as string
            data_list.sort(reverse=reverse)
        
        # Rearrange items in sorted positions
        for index, (val, k) in enumerate(data_list):
            self.tree.move(k, '', index)
        
        # Reverse sort next time
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse))
    
    def view_chart(self):
        """View candlestick chart for the selected stock"""
        selection = self.tree.selection()
        if not selection:
            self.status_var.set("Please select a stock first")
            logging.warning("Chart view attempted without stock selection")
            return
        
        # Get selected stock data
        stock_data = self.tree.item(selection[0])['values']
        if not stock_data or len(stock_data) < 8:
            self.status_var.set("Invalid stock data")
            logging.error(f"Invalid stock data for selection: {stock_data}")
            return
        
        try:
            # Extract stock details from selected row
            stock_id = stock_data[0]       # Database primary key ID
            security_id = stock_data[1]    # Security ID (e.g. INE009A01021)
            symbol = stock_data[2]         # Stock symbol (e.g. RELIANCE)
            name = stock_data[3]           # Company name
            exchange_segment = stock_data[4]  # Exchange segment (e.g. NSE_EQ)
            instrument = stock_data[5]     # Instrument type (e.g. EQUITY)
            
            # Write diagnostic info to a log file (using UTF-8 encoding)
            with open("chart_debug.log", "w", encoding="utf-8") as f:
                f.write(f"Viewing chart for: ID={stock_id}, Security ID={security_id}, Symbol={symbol}\n")
                f.write(f"Python libraries check:\n")
                try:
                    import pandas as pd
                    f.write("✓ pandas version: " + pd.__version__ + "\n")
                except ImportError:
                    f.write("✗ pandas not installed\n")
                    self.status_var.set("Error: pandas not installed. Run 'pip install pandas'")
                    return
                    
                try:
                    import plotly
                    f.write("✓ plotly version: " + plotly.__version__ + "\n")
                except ImportError:
                    f.write("✗ plotly not installed\n")
                    self.status_var.set("Error: plotly not installed. Run 'pip install plotly'")
                    return
            
            logging.info(f"Viewing chart for: ID={stock_id}, Security ID={security_id}, Symbol={symbol}")
            
            # Calculate date range based on selected period
            period = self.period_var.get()
            end_date = datetime.now() - timedelta(days=1)  # Yesterday
            
            if period == "1 Day":
                # For "1 Day" chart, we need at least 2 days of data to create a proper chart
                # This ensures we have intraday data points for yesterday
                start_date = end_date - timedelta(days=2)
            elif period == "1 Week":
                start_date = end_date - timedelta(days=7)
            elif period == "1 Month":
                start_date = end_date - timedelta(days=30)
            elif period == "3 Months":
                start_date = end_date - timedelta(days=90)
            elif period == "6 Months":
                start_date = end_date - timedelta(days=180)
            else:  # 1 Year
                start_date = end_date - timedelta(days=365)
            
            from_date = start_date.strftime("%Y-%m-%d")
            to_date = end_date.strftime("%Y-%m-%d")
            
            self.status_var.set(f"Fetching data for {symbol}...")
            self.root.update()
            
            # Write to debug log (using UTF-8 encoding)
            with open("chart_debug.log", "a", encoding="utf-8") as f:
                f.write(f"Date range: from {from_date} to {to_date}\n")
            
            try:
                # First try to fetch data from database using stock_id for direct relation
                query = """
                    SELECT timestamp, date, open, high, low, close, volume
                    FROM history_data
                    WHERE stock_id = ? 
                    AND date BETWEEN ? AND ?
                    ORDER BY timestamp
                """
                logging.info(f"Executing query with params: stock_id={stock_id}, from_date={from_date}, to_date={to_date}")
                df = pd.read_sql_query(query, self.conn, params=(stock_id, from_date, to_date))
                logging.info(f"Query returned {len(df)} records")
                
                # Write to debug log (using UTF-8 encoding)
                with open("chart_debug.log", "a", encoding="utf-8") as f:
                    f.write(f"Stock ID query returned {len(df)} records\n")
                
                # If no data in DB or very little data, try using security_id
                if len(df) < 5:
                    self.status_var.set(f"Trying security_id lookup for {symbol}")
                    query = """
                        SELECT timestamp, date, open, high, low, close, volume
                        FROM history_data
                        WHERE security_id = ? 
                        AND date BETWEEN ? AND ?
                        ORDER BY timestamp
                    """
                    logging.info(f"Executing security_id query with params: security_id={security_id}, from_date={from_date}, to_date={to_date}")
                    df = pd.read_sql_query(query, self.conn, params=(security_id, from_date, to_date))
                    logging.info(f"Security ID query returned {len(df)} records")
                    
                    # Write to debug log (using UTF-8 encoding)
                    with open("chart_debug.log", "a", encoding="utf-8") as f:
                        f.write(f"Security ID query returned {len(df)} records\n")
                
                # Check if we have data to display
                if len(df) < 2:
                    self.status_var.set(f"No historical data available for {symbol}")
                    logging.warning(f"Insufficient data for {symbol}: only {len(df)} records available")
                    
                    # Write to debug log (using UTF-8 encoding)
                    with open("chart_debug.log", "a", encoding="utf-8") as f:
                        f.write(f"Insufficient data for chart: only {len(df)} records\n")
                    return
                
                logging.info(f"Creating chart with {len(df)} data points")
                
                # Write some data samples to debug log (using UTF-8 encoding)
                with open("chart_debug.log", "a", encoding="utf-8") as f:
                    f.write(f"DataFrame columns: {df.columns.tolist()}\n")
                    f.write(f"First few rows:\n{df.head().to_string()}\n")
                
                # For charting, use the date column if available, otherwise use timestamp
                if 'date' in df.columns and not df['date'].isnull().all():
                    # If date is stored as string, convert to datetime
                    if isinstance(df['date'].iloc[0], str):
                        df['date'] = pd.to_datetime(df['date'])
                    x_values = df['date']
                else:
                    x_values = pd.to_datetime(df['timestamp'], unit='s')
                
                # Create candlestick chart
                fig = go.Figure()
                
                # Add candlestick trace
                fig.add_trace(
                    go.Candlestick(
                        x=x_values,
                        open=df['open'],
                        high=df['high'],
                        low=df['low'],
                        close=df['close'],
                        name=symbol
                    )
                )
                
                # Add volume as bar chart on secondary y-axis
                fig.add_trace(
                    go.Bar(
                        x=x_values,
                        y=df['volume'],
                        name="Volume",
                        marker_color='rgba(128,128,128,0.5)',
                        opacity=0.5,
                        yaxis="y2"
                    )
                )
                
                # Layout adjustments
                fig.update_layout(
                    title=f"{name} ({symbol}) - {period} Chart",
                    xaxis_title="Date",
                    yaxis_title="Price",
                    xaxis_rangeslider_visible=False,
                    yaxis2=dict(
                        title="Volume",
                        overlaying="y",
                        side="right",
                        showgrid=False
                    ),
                    height=800,
                    template="plotly_dark" if ctk.get_appearance_mode() == "Dark" else "plotly_white"
                )
                
                # Save and open the HTML file
                chart_dir = "charts"
                if not os.path.exists(chart_dir):
                    os.makedirs(chart_dir)
                
                filename = f"{chart_dir}/{symbol}_{from_date}_{to_date}.html"
                
                # Write to debug log (using UTF-8 encoding)
                with open("chart_debug.log", "a", encoding="utf-8") as f:
                    f.write(f"Saving chart to: {filename}\n")
                
                try:
                    plot(fig, filename=filename, auto_open=False)
                    
                    # Write to debug log (using UTF-8 encoding)
                    with open("chart_debug.log", "a", encoding="utf-8") as f:
                        f.write(f"Chart file created successfully\n")
                    
                    self.status_var.set(f"Chart created for {symbol}")
                    logging.info(f"Chart file created at: {filename}")
                    
                    try:
                        webbrowser.open('file://' + os.path.abspath(filename))
                        # Write to debug log (using UTF-8 encoding)
                        with open("chart_debug.log", "a", encoding="utf-8") as f:
                            f.write(f"Browser launched with URL: file://{os.path.abspath(filename)}\n")
                    except Exception as browser_err:
                        err_msg = f"Error opening browser: {str(browser_err)}"
                        logging.error(err_msg)
                        self.status_var.set(f"Chart created but couldn't open browser. Check {filename}")
                        
                        # Write to debug log (using UTF-8 encoding)
                        with open("chart_debug.log", "a", encoding="utf-8") as f:
                            f.write(f"Browser error: {str(browser_err)}\n")
                
                except Exception as plot_err:
                    err_msg = f"Error creating chart file: {str(plot_err)}"
                    logging.error(err_msg)
                    self.status_var.set(err_msg)
                    
                    # Write to debug log (using UTF-8 encoding)
                    with open("chart_debug.log", "a", encoding="utf-8") as f:
                        f.write(f"Plot error: {str(plot_err)}\n")
                
            except sqlite3.Error as sql_err:
                err_msg = f"Database error: {sql_err}"
                logging.error(err_msg)
                self.status_var.set(err_msg)
                
                # Write to debug log (using UTF-8 encoding)
                with open("chart_debug.log", "a", encoding="utf-8") as f:
                    f.write(f"SQL error: {str(sql_err)}\n")
                
        except Exception as e:
            err_msg = f"Error generating chart: {str(e)}"
            logging.error(err_msg, exc_info=True)
            self.status_var.set(err_msg)
            
            # Write to debug log (using UTF-8 encoding)
            with open("chart_debug.log", "a", encoding="utf-8") as f:
                f.write(f"General error: {str(e)}\n")
                import traceback
                f.write(traceback.format_exc())

    def generate_signals(self):
        """Generate technical signals for selected stocks or all visible stocks"""
        selection = self.tree.selection()
        
        if selection:
            # Generate signals only for selected stocks
            stock_data = [self.tree.item(item)['values'] for item in selection]
            symbols = [data[2] for data in stock_data if len(data) > 2]  # Extract symbols from selected rows
            self.status_var.set(f"Generating signals for {len(symbols)} selected stocks...")
        else:
            # Generate signals for all visible stocks in the treeview
            stock_data = [self.tree.item(item)['values'] for item in self.tree.get_children()]
            symbols = [data[2] for data in stock_data if len(data) > 2]  # Extract symbols from all rows
            self.status_var.set(f"Generating signals for all {len(symbols)} visible stocks...")
        
        if not symbols:
            self.status_var.set("No stocks to generate signals for")
            return
            
        self.root.update()
        
        # Show busy cursor
        self.root.config(cursor="wait")
        self.root.update()
        
        try:
            logging.info(f"Generating signals for {len(symbols)} stocks")
            
            # Process each stock
            signals_list = []
            for i, symbol in enumerate(symbols):
                # Update status to show progress
                self.status_var.set(f"Processing {symbol} ({i+1}/{len(symbols)})...")
                self.root.update()
                
                # Generate signals for this stock
                signals = self.signal_generator.analyze_stock(symbol=symbol, days=100, show_chart=False)
                if signals:
                    signals_list.append(signals)
                    
                    # Store in our signals data dictionary
                    self.signals_data[symbol] = signals
            
            # Update the treeview with signal data
            self.update_treeview_with_signals()
            
            # Auto-switch to signals tab if we found buy signals
            buy_signals = [s for s in self.signals_data.values() if s.get('combined_signal_desc') == 'STRONG BUY']
            if buy_signals:
                self.status_var.set(f"Found {len(buy_signals)} buy signals!")
            else:
                self.status_var.set(f"Generated signals for {len(signals_list)} stocks - No buy signals found")
            
            # Reset cursor
            self.root.config(cursor="")
            
        except Exception as e:
            logging.error(f"Error generating signals: {e}", exc_info=True)
            self.status_var.set(f"Error generating signals: {str(e)}")
            # Reset cursor
            self.root.config(cursor="")
    
    def update_treeview_with_signals(self):
        """Update the treeview with generated signal data"""
        # Iterate through all items in the treeview
        for item_id in self.tree.get_children():
            item_values = self.tree.item(item_id)['values']
            symbol = item_values[2] if len(item_values) > 2 else None
            
            if symbol in self.signals_data:
                signals = self.signals_data[symbol]
                
                # Extract signals from the data
                ma_signal = signals.get('ma_signal_desc', 'NEUTRAL')
                rsi_signal = signals.get('rsi_signal_desc', 'NEUTRAL')
                combined_signal = signals.get('combined_signal_desc', 'NEUTRAL')
                
                # Update the item with signal values
                new_values = list(item_values)
                
                # Ensure the list is at least 11 items long
                while len(new_values) < 11:
                    new_values.append('')
                
                # Set signal values
                new_values[8] = ma_signal  # MA Signal
                new_values[9] = rsi_signal  # RSI Signal
                new_values[10] = combined_signal  # Combined Signal
                
                # Update the item in the treeview
                self.tree.item(item_id, values=new_values)
                
                # Color-code based on signals
                self.color_code_item(item_id, combined_signal)
    
    def color_code_item(self, item_id, signal):
        """Apply color coding to treeview items based on signals"""
        if signal == 'STRONG BUY':
            self.tree.item(item_id, tags=('strong_buy',))
        elif signal == 'STRONG SELL':
            self.tree.item(item_id, tags=('strong_sell',))
        elif signal == 'BUY':
            self.tree.item(item_id, tags=('buy',))
        elif signal == 'SELL':
            self.tree.item(item_id, tags=('sell',))
        else:
            self.tree.item(item_id, tags=('neutral',))
            
        # Configure tag colors
        self.tree.tag_configure('strong_buy', background='#90ee90')  # Light green
        self.tree.tag_configure('buy', background='#d0f0c0')  # Lighter green
        self.tree.tag_configure('strong_sell', background='#ffcccb')  # Light red
        self.tree.tag_configure('sell', background='#ffe4e1')  # Lighter red
        self.tree.tag_configure('neutral', background='')  # Default
    
    def filter_by_signal(self, signal_type):
        """Filter stocks based on the selected signal type"""
        # If no signals have been generated yet, generate them
        if not self.signals_data:
            self.generate_signals()
            
        # Clear current display
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Fetch all stocks from the database
        query = """
            SELECT id, security_id, symbol, name, exchange_segment, instrument, added_date, last_updated 
            FROM stocks
            ORDER BY symbol
        """
        df = pd.read_sql_query(query, self.conn)
        
        # Filter based on signal type
        filtered_stocks = []
        
        for _, row in df.iterrows():
            symbol = row['symbol']
            if symbol in self.signals_data:
                signals = self.signals_data[symbol]
                
                # Match the filtering criteria
                if signal_type == "All":
                    filtered_stocks.append(row)
                elif signal_type == "Buy Signal" and signals.get('combined_signal_desc') == 'STRONG BUY':
                    filtered_stocks.append(row)
            elif signal_type == "All":
                filtered_stocks.append(row)
        
        # Add filtered stocks to the treeview
        for stock in filtered_stocks:
            values = tuple(stock)
            item_id = self.tree.insert("", "end", values=values)
            
            symbol = stock['symbol']
            if symbol in self.signals_data:
                signals = self.signals_data[symbol]
                
                # Add signal columns
                new_values = list(values)
                while len(new_values) < 11:
                    new_values.append('')
                
                # For simplicity, show the same signal status in all columns when it's a buy
                if signals.get('combined_signal_desc') == 'STRONG BUY':
                    new_values[8] = 'BUY'   # MA Signal
                    new_values[9] = 'BUY'   # RSI Signal
                    new_values[10] = 'BUY'  # Combined Signal
                else:
                    new_values[8] = 'NEUTRAL'
                    new_values[9] = 'NEUTRAL'
                    new_values[10] = 'NEUTRAL'
                
                # Update with signal values
                self.tree.item(item_id, values=new_values)
                
                # Apply color coding
                if signals.get('combined_signal_desc') == 'STRONG BUY':
                    self.tree.item(item_id, tags=('buy',))
                else:
                    self.tree.item(item_id, tags=('neutral',))
                    
                # Configure tag colors
                self.tree.tag_configure('buy', background='#90ee90')  # Light green
                self.tree.tag_configure('neutral', background='')  # Default
        
        # Update status
        if signal_type == "All":
            self.count_var.set(f"Showing all stocks: {len(filtered_stocks)}")
        else:
            self.count_var.set(f"{signal_type} signals: {len(filtered_stocks)}")
    
    def view_signal_chart(self):
        """View technical signal chart for the selected stock with MA and RSI indicators"""
        selection = self.tree.selection()
        if not selection:
            self.status_var.set("Please select a stock first")
            return
            
        stock_data = self.tree.item(selection[0])['values']
        symbol = stock_data[2] if len(stock_data) > 2 else None
        
        if not symbol:
            self.status_var.set("Invalid stock data")
            return
            
        self.status_var.set(f"Generating signal chart for {symbol}...")
        self.root.update()
        
        # Generate signal chart
        signals = self.signal_generator.analyze_stock(symbol=symbol, days=100, show_chart=True)
        
        if signals:
            self.status_var.set(f"Generated signal chart for {symbol}")
        else:
            self.status_var.set(f"Could not generate signal chart for {symbol}")
    
    def show_context_menu(self, event):
        """Show the context menu on right-click"""
        # Select the item that was right-clicked first
        item_id = self.tree.identify_row(event.y)
        if (item_id):
            # First select the item
            self.tree.selection_set(item_id)
            # Then show the context menu
            self.context_menu.post(event.x_root, event.y_root)
            
    def generate_signals_for_selected(self):
        """Generate signals only for selected stocks"""
        selection = self.tree.selection()
        if not selection:
            self.status_var.set("Please select at least one stock first")
            return
            
        # Extract symbols from selected rows
        stock_data = [self.tree.item(item)['values'] for item in selection]
        symbols = [data[2] for data in stock_data if len(data) > 2]
        
        if not symbols:
            self.status_var.set("No valid stocks selected")
            return
            
        self.status_var.set(f"Generating signals for {len(symbols)} selected stocks...")
        self.root.update()
        
        # Show busy cursor
        self.root.config(cursor="wait")
        self.root.update()
        
        try:
            # Process each stock
            signals_list = []
            for i, symbol in enumerate(symbols):
                # Update status to show progress
                self.status_var.set(f"Processing {symbol} ({i+1}/{len(symbols)})...")
                self.root.update()
                
                # Generate signals for this stock
                signals = self.signal_generator.analyze_stock(symbol=symbol, days=100, show_chart=False)
                if signals:
                    signals_list.append(signals)
                    
                    # Store in our signals data dictionary
                    self.signals_data[symbol] = signals
            
            # Update the treeview with signal data
            self.update_treeview_with_signals()
            
            # Update status
            self.status_var.set(f"Generated signals for {len(signals_list)} stocks")
            
        except Exception as e:
            logging.error(f"Error generating signals: {e}", exc_info=True)
            self.status_var.set(f"Error generating signals: {str(e)}")
        finally:
            # Reset cursor
            self.root.config(cursor="")

    def add_to_auto_order_watchlist(self):
        """Add the selected stock to the auto order watchlist"""
        selection = self.tree.selection()
        if not selection:
            self.status_var.set("Please select a stock first")
            return
            
        # Get selected stock data
        stock_data = self.tree.item(selection[0])['values']
        symbol = stock_data[2] if len(stock_data) > 2 else None
        
        if not symbol:
            self.status_var.set("Invalid stock data")
            return
            
        try:
            # Use database instead of JSON file
            from db_handler import DatabaseHandler
            
            db = DatabaseHandler()
            if not db.connect():
                self.status_var.set("Error connecting to database")
                return
                
            # Add symbol to watchlist
            result = db.add_to_watchlist(symbol)
            
            if result:
                self.status_var.set(f"Added {symbol} to auto order watchlist")
            else:
                self.status_var.set(f"Failed to add {symbol} to watchlist")
                
            db.close()
            
        except Exception as e:
            logging.error(f"Error adding to watchlist: {e}", exc_info=True)
            self.status_var.set(f"Error adding to watchlist: {str(e)}")
            
    def show_auto_order_config(self):
        """Show the auto order configuration dialog"""
        # Create a new top-level window
        config_window = ctk.CTkToplevel(self.root)
        config_window.title("Auto Order Configuration")
        config_window.geometry("600x650")
        config_window.resizable(True, True)
        
        # Make sure it opens on top
        config_window.transient(self.root)
        config_window.grab_set()
        
        # Load settings from database
        from db_handler import DatabaseHandler
        
        db = DatabaseHandler()
        if not db.connect():
            self.status_var.set("Error connecting to database")
            return
            
        # Get all settings and watchlist
        settings = db.get_all_settings()
        watchlist_symbols = db.get_watchlist()
        
        # Create config dict with defaults for any missing settings
        config = {
            "api_key": settings.get("api_key", ""),
            "api_secret": settings.get("api_secret", ""),
            "broker": settings.get("broker", "demo"),
            "capital_per_trade": settings.get("capital_per_trade", 10000),
            "max_positions": settings.get("max_positions", 5),
            "stop_loss_percent": settings.get("stop_loss_percent", 5),
            "target_percent": settings.get("target_percent", 10),
            "enable_auto_orders": settings.get("enable_auto_orders", False),
            "confirmation_candles": settings.get("confirmation_candles", 1),
            "order_type": settings.get("order_type", "LIMIT"),
            "limit_price_offset": settings.get("limit_price_offset", 0.5),
            "time_in_force": settings.get("time_in_force", "DAY"),
            "notification_email": settings.get("notification_email", ""),
            "enabled_symbols": watchlist_symbols
        }
        
        # Create scrollable frame to contain all settings
        main_frame = ctk.CTkScrollableFrame(config_window, width=580, height=600)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Title and description
        title_label = ctk.CTkLabel(main_frame, text="Auto Order Settings", font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 5), anchor="w")
        
        desc_label = ctk.CTkLabel(main_frame, text="Configure automated order placement based on 50-day MA and RSI 50 signals.")
        desc_label.pack(pady=(0, 15), anchor="w")
        
        # Add Save button at the top
        save_top_btn = ctk.CTkButton(
            main_frame, 
            text="Save Settings", 
            fg_color="#009900", 
            hover_color="#006600",
            command=lambda: save_config()
        )
        save_top_btn.pack(pady=(0, 15), anchor="center")
        
        # Broker settings section
        broker_frame = ctk.CTkFrame(main_frame)
        broker_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(broker_frame, text="Broker Settings", font=("Arial", 14, "bold")).pack(anchor="w", pady=5, padx=10)
        
        # Broker selection
        broker_row = ctk.CTkFrame(broker_frame)
        broker_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(broker_row, text="Broker:").pack(side="left", padx=(5, 10))
        broker_var = ctk.StringVar(value=config.get("broker", "demo"))
        broker_options = ["demo", "zerodha", "upstox", "fyers", "dhan"]  
        broker_dropdown = ctk.CTkOptionMenu(broker_row, values=broker_options, variable=broker_var, command=lambda x: update_broker_fields())
        broker_dropdown.pack(side="left", padx=5, fill="x", expand=True)
        
        # API Key / Secret
        api_key_row = ctk.CTkFrame(broker_frame)
        api_key_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(api_key_row, text="API Key:").pack(side="left", padx=(5, 10))
        api_key_var = ctk.StringVar(value=config.get("api_key", ""))
        api_key_entry = ctk.CTkEntry(api_key_row, textvariable=api_key_var, width=300)
        api_key_entry.pack(side="left", padx=5, fill="x", expand=True)
        
        # Function to update broker fields based on selection
        def update_broker_fields():
            selected_broker = broker_var.get()
            if selected_broker == "dhan":
                api_key_entry.configure(state="disabled")
                api_key_row.pack_forget()  # Hide the API key row
            else:
                api_key_entry.configure(state="normal")
                api_key_row.pack(fill="x", padx=10, pady=2)  # Show the API key row
        
        # Call once to set initial state
        update_broker_fields()
        
        api_secret_row = ctk.CTkFrame(broker_frame)
        api_secret_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(api_secret_row, text="API Secret:").pack(side="left", padx=(5, 10))
        api_secret_var = ctk.StringVar(value=config.get("api_secret", ""))
        api_secret_entry = ctk.CTkEntry(api_secret_row, textvariable=api_secret_var, width=300, show="*")
        api_secret_entry.pack(side="left", padx=5, fill="x", expand=True)
        
        # Order settings section
        order_frame = ctk.CTkFrame(main_frame)
        order_frame.pack(fill="x", padx=5, pady=5, anchor="w")
        
        ctk.CTkLabel(order_frame, text="Order Settings", font=("Arial", 14, "bold")).pack(anchor="w", pady=5, padx=10)
        
        # Capital per trade
        capital_row = ctk.CTkFrame(order_frame)
        capital_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(capital_row, text="Capital per trade:").pack(side="left", padx=(5, 10))
        capital_var = ctk.StringVar(value=str(config.get("capital_per_trade", 10000)))
        capital_entry = ctk.CTkEntry(capital_row, textvariable=capital_var, width=100)
        capital_entry.pack(side="left", padx=5)
        
        # Max positions
        max_pos_row = ctk.CTkFrame(order_frame)
        max_pos_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(max_pos_row, text="Max positions:").pack(side="left", padx=(5, 10))
        max_pos_var = ctk.StringVar(value=str(config.get("max_positions", 5)))
        max_pos_entry = ctk.CTkEntry(max_pos_row, textvariable=max_pos_var, width=100)
        max_pos_entry.pack(side="left", padx=5)
        
        # Stop loss percent
        sl_row = ctk.CTkFrame(order_frame)
        sl_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(sl_row, text="Stop loss %:").pack(side="left", padx=(5, 10))
        sl_var = ctk.StringVar(value=str(config.get("stop_loss_percent", 5)))
        sl_entry = ctk.CTkEntry(sl_row, textvariable=sl_var, width=100)
        sl_entry.pack(side="left", padx=5)
        
        # Target percent
        target_row = ctk.CTkFrame(order_frame)
        target_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(target_row, text="Target profit %:").pack(side="left", padx=(5, 10))
        target_var = ctk.StringVar(value=str(config.get("target_percent", 10)))
        target_entry = ctk.CTkEntry(target_row, textvariable=target_var, width=100)
        target_entry.pack(side="left", padx=5)
        
        # Confirmation candles
        confirm_row = ctk.CTkFrame(order_frame)
        confirm_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(confirm_row, text="Confirmation candles:").pack(side="left", padx=(5, 10))
        confirm_var = ctk.StringVar(value=str(config.get("confirmation_candles", 1)))
        confirm_entry = ctk.CTkEntry(confirm_row, textvariable=confirm_var, width=100)
        confirm_entry.pack(side="left", padx=5)
        
        # Order type
        order_type_row = ctk.CTkFrame(order_frame)
        order_type_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(order_type_row, text="Order type:").pack(side="left", padx=(5, 10))
        order_type_var = ctk.StringVar(value=config.get("order_type", "LIMIT"))
        order_type_options = ["LIMIT", "MARKET"]
        order_type_dropdown = ctk.CTkOptionMenu(order_type_row, values=order_type_options, variable=order_type_var)
        order_type_dropdown.pack(side="left", padx=5)
        
        # Limit price offset (only if LIMIT order)
        limit_price_row = ctk.CTkFrame(order_frame)
        limit_price_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(limit_price_row, text="Limit price offset %:").pack(side="left", padx=(5, 10))
        limit_price_var = ctk.StringVar(value=str(config.get("limit_price_offset", 0.5)))
        limit_price_entry = ctk.CTkEntry(limit_price_row, textvariable=limit_price_var, width=100)
        limit_price_entry.pack(side="left", padx=5)
        
        # Notification settings
        notif_frame = ctk.CTkFrame(main_frame)
        notif_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(notif_frame, text="Notifications", font=("Arial", 14, "bold")).pack(anchor="w", pady=5, padx=10)
        
        # Email
        email_row = ctk.CTkFrame(notif_frame)
        email_row.pack(fill="x", padx=10, pady=2)
        ctk.CTkLabel(email_row, text="Email for notifications:").pack(side="left", padx=(5, 10))
        email_var = ctk.StringVar(value=config.get("notification_email", ""))
        email_entry = ctk.CTkEntry(email_row, textvariable=email_var, width=300)
        email_entry.pack(side="left", padx=5, fill="x", expand=True)
        
        # Enable auto orders
        enable_row = ctk.CTkFrame(main_frame)
        enable_row.pack(fill="x", padx=5, pady=15)
        
        enable_var = ctk.BooleanVar(value=config.get("enable_auto_orders", False))
        enable_switch = ctk.CTkSwitch(enable_row, text="Enable Auto Orders", variable=enable_var)
        enable_switch.pack(side="left", padx=15)
        
        # Watchlist section
        watchlist_frame = ctk.CTkFrame(main_frame)
        watchlist_frame.pack(fill="x", padx=5, pady=5)
        
        watchlist_header = ctk.CTkFrame(watchlist_frame)
        watchlist_header.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(watchlist_header, text="Watchlist Stocks", font=("Arial", 14, "bold")).pack(side="left", padx=5)
        
        # Add remove buttons to watchlist
        refresh_btn = ctk.CTkButton(watchlist_header, text="Refresh", width=80, 
                                     command=lambda: refresh_watchlist(watchlist))
        refresh_btn.pack(side="right", padx=5)
        
        clear_btn = ctk.CTkButton(watchlist_header, text="Clear All", width=80, fg_color="#FF5555", hover_color="#AA0000",
                                   command=lambda: clear_watchlist(watchlist))
        clear_btn.pack(side="right", padx=5)
        
        # Watchlist display
        watchlist_container = ctk.CTkFrame(watchlist_frame)
        watchlist_container.pack(fill="x", padx=10, pady=5)
        
        # Create a scrollable frame for the watchlist
        watchlist = ctk.CTkScrollableFrame(watchlist_container, height=150)
        watchlist.pack(fill="x", expand=True)
        
        # Function to populate watchlist
        def populate_watchlist():
            # Clear current watchlist
            for widget in watchlist.winfo_children():
                widget.destroy()
                
            # Get watchlist symbols
            if not db.conn:
                db.connect()
            
            symbols = db.get_watchlist()
            
            if not symbols:
                ctk.CTkLabel(watchlist, text="No stocks in watchlist").pack(pady=10)
                return
                
            # Create a row for each symbol
            for symbol in symbols:
                symbol_row = ctk.CTkFrame(watchlist)
                symbol_row.pack(fill="x", padx=5, pady=2)
                
                ctk.CTkLabel(symbol_row, text=symbol, width=100).pack(side="left", padx=5)
                
                # Add a remove button
                remove_btn = ctk.CTkButton(symbol_row, text="Remove", width=80, 
                                          command=lambda s=symbol: remove_from_watchlist(s, watchlist))
                remove_btn.pack(side="right", padx=5)
        
        # Function to remove a symbol from watchlist
        def remove_from_watchlist(symbol, watchlist_frame):
            if not db.conn:
                db.connect()
                
            if db.remove_from_watchlist(symbol):
                # Refresh watchlist
                populate_watchlist()
        
        # Function to clear all symbols
        def clear_watchlist(watchlist_frame):
            if not db.conn:
                db.connect()
                
            db.clear_watchlist()
            
            # Refresh watchlist
            populate_watchlist()
            
        # Function to refresh watchlist
        def refresh_watchlist(watchlist_frame):
            if not db.conn:
                db.connect()
                
            populate_watchlist()
        
        # Populate the watchlist initially
        populate_watchlist()
        
        # Save button
        button_frame = ctk.CTkFrame(config_window)
        button_frame.pack(fill="x", padx=10, pady=10)
        
        def save_config():
            try:
                if not db.conn:
                    db.connect()
                    
                # Update settings with values from form
                settings = {
                    "broker": broker_var.get(),
                    "api_key": api_key_var.get().strip(),
                    "api_secret": api_secret_var.get().strip(),
                    "notification_email": email_var.get().strip(),
                    "enable_auto_orders": enable_var.get(),
                    "order_type": order_type_var.get()
                }
                
                # Handle numeric values with proper error checking
                try:
                    settings["capital_per_trade"] = int(capital_var.get())
                except (ValueError, TypeError):
                    settings["capital_per_trade"] = 10000  # Default if invalid
                    
                try:
                    settings["max_positions"] = int(max_pos_var.get())
                except (ValueError, TypeError):
                    settings["max_positions"] = 5  # Default if invalid
                    
                try:
                    settings["stop_loss_percent"] = float(sl_var.get())
                except (ValueError, TypeError):
                    settings["stop_loss_percent"] = 5.0  # Default if invalid
                    
                try:
                    settings["target_percent"] = float(target_var.get())
                except (ValueError, TypeError):
                    settings["target_percent"] = 10.0  # Default if invalid
                    
                try:
                    settings["confirmation_candles"] = int(confirm_var.get())
                except (ValueError, TypeError):
                    settings["confirmation_candles"] = 1  # Default if invalid
                
                try:
                    settings["limit_price_offset"] = float(limit_price_var.get())
                except (ValueError, TypeError):
                    settings["limit_price_offset"] = 0.5  # Default if invalid
                
                # Save each setting to database
                for key, value in settings.items():
                    db.set_setting(key, value)
                
                self.status_var.set("Auto order configuration saved to database")
                logging.info("Auto order configuration saved to database")
                config_window.destroy()
                
            except Exception as e:
                error_msg = f"Error saving configuration: {str(e)}"
                logging.error(error_msg, exc_info=True)
                tk.messagebox.showerror("Error", error_msg)
        
        save_btn = ctk.CTkButton(button_frame, text="Save Configuration", command=save_config)
        save_btn.pack(side="right", padx=10)
        
        cancel_btn = ctk.CTkButton(button_frame, text="Cancel", fg_color="#FF5555", hover_color="#AA0000",
                                  command=lambda: [db.close(), config_window.destroy()])
        cancel_btn.pack(side="right", padx=10)
        
        # Start button for scheduler
        def start_auto_orders():
            try:
                # Save the configuration first
                save_config()
                
                # Launch the auto order script
                import subprocess
                import os
                
                # Get the current directory
                current_dir = os.path.dirname(os.path.abspath(__file__))
                
                # Start the batch file
                subprocess.Popen(["cmd", "/c", "start", "run_auto_orders.bat"], 
                                cwd=current_dir, 
                                shell=True)
                
                self.status_var.set("Auto order system started")
                
            except Exception as e:
                logging.error(f"Error starting auto orders: {e}", exc_info=True)
                tk.messagebox.showerror("Error", f"Failed to start auto order system: {str(e)}")
        
        start_btn = ctk.CTkButton(button_frame, text="Start Auto Orders", 
                                 fg_color="#009900", hover_color="#006600",
                                 command=start_auto_orders)
        start_btn.pack(side="left", padx=10)
        
        # Handle window closing
        config_window.protocol("WM_DELETE_WINDOW", lambda: [db.close(), config_window.destroy()])

    def quick_save_settings(self):
        """Quick save auto order settings to the database"""
        try:
            from auto_order import AutoOrderPlacer
            from db_handler import DatabaseHandler
            
            # Show busy cursor
            self.root.config(cursor="wait")
            self.root.update()
            
            # First check if we have existing settings in the database
            db = DatabaseHandler()
            if not db.connect():
                self.status_var.set("Error connecting to database")
                self.root.config(cursor="")
                return
                
            settings = db.get_all_settings()
            
            # If we have no settings yet, initialize AutoOrderPlacer which will create defaults
            if not settings:
                self.status_var.set("Initializing auto order settings...")
                self.root.update()
                
                # Create and initialize auto order placer (will set up default settings)
                auto_order = AutoOrderPlacer()
            else:
                # Update timestamp to mark as refreshed
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                db.set_setting("last_saved", current_time, "Last time settings were saved")
            
            # Update status with count of settings stored
            settings = db.get_all_settings()  # Get refreshed settings
            watchlist = db.get_watchlist()
            
            self.status_var.set(f"Auto order settings saved to database: {len(settings)} settings, {len(watchlist)} watchlist items")
            logging.info(f"Auto order settings saved to database: {len(settings)} settings, {len(watchlist)} watchlist items")
            
            # Reset cursor
            self.root.config(cursor="")
            
            db.close()
            
        except Exception as e:
            error_msg = f"Error saving settings: {str(e)}"
            logging.error(error_msg, exc_info=True)
            self.status_var.set(error_msg)
            self.root.config(cursor="")
            
            # Show error dialog
            tk.messagebox.showerror("Error", error_msg)

    def load_orders(self):
        """Load order data from order history file and display in Orders tab"""
        # Clear current display
        for item in self.orders_tree.get_children():
            self.orders_tree.delete(item)
        
        try:
            # Load the order history from JSON file
            from pathlib import Path
            
            order_history_file = Path("order_history.json")
            if not order_history_file.exists():
                # No orders to display
                message_label = ctk.CTkLabel(
                    self.orders_tree_frame, 
                    text="No order history found. Place orders through Auto Orders to see them here.",
                    font=("Arial", 12)
                )
                message_label.place(relx=0.5, rely=0.5, anchor="center")
                return
                
            with open(order_history_file, "r") as f:
                order_history = json.load(f)
            
            # Check if we have any orders
            if not order_history.get("orders", []):
                message_label = ctk.CTkLabel(
                    self.orders_tree_frame, 
                    text="No orders have been placed yet. Use Auto Orders to place orders.",
                    font=("Arial", 12)
                )
                message_label.place(relx=0.5, rely=0.5, anchor="center")
                return
            
            # Add each order to the treeview
            for order in order_history.get("orders", []):
                # Extract and format order details
                symbol = order.get("symbol", "N/A")
                broker = order.get("broker", "N/A")
                quantity = str(order.get("quantity", 0))
                price = f"{float(order.get('price', 0)):.2f}" if "price" in order else "N/A"
                order_type = order.get("order_type", "N/A")
                stop_loss = f"{float(order.get('stop_loss', 0)):.2f}" if "stop_loss" in order else "N/A"
                target = f"{float(order.get('target', 0)):.2f}" if "target" in order else "N/A"
                status = order.get("status", "unknown")
                timestamp = order.get("timestamp", "N/A")
                
                # Insert into orders tree
                item_id = self.orders_tree.insert("", "end", values=(
                    symbol, broker, quantity, price, order_type,
                    stop_loss, target, status, timestamp
                ))
                
                # Apply color coding based on order status
                if status == 'filled' or status == 'simulated':
                    self.orders_tree.item(item_id, tags=('active',))
                elif status == 'cancelled':
                    self.orders_tree.item(item_id, tags=('cancelled',))
                elif status == 'rejected':
                    self.orders_tree.item(item_id, tags=('rejected',))
                else:
                    self.orders_tree.item(item_id, tags=('pending',))
            
            # Configure tag colors for orders tab
            self.orders_tree.tag_configure('active', background='#90ee90')  # Light green
            self.orders_tree.tag_configure('cancelled', background='#FFD580')  # Light orange
            self.orders_tree.tag_configure('rejected', background='#ffcccb')  # Light red
            self.orders_tree.tag_configure('pending', background='#add8e6')  # Light blue
            
            # Count active orders
            active_orders = sum(1 for order in order_history.get("orders", []) 
                              if order.get("status") in ["filled", "simulated", "open"])
            
            # CustomTkinter's TabView doesn't support setting tab text directly
            # Instead we'll update the tab title by accessing the tab directly
            # self.tab_view.set("Orders", f"Orders ({active_orders})")
            
            # Just log the active orders count instead of trying to modify tab title
            logging.info(f"Active orders: {active_orders}")
            
            logging.info(f"Loaded {len(order_history.get('orders', []))} orders from order history")
            
        except Exception as e:
            logging.error(f"Error loading order history: {e}", exc_info=True)
            message_label = ctk.CTkLabel(
                self.orders_tree_frame, 
                text=f"Error loading order history: {str(e)}",
                font=("Arial", 12)
            )
            message_label.place(relx=0.5, rely=0.5, anchor="center")

    def sort_orders_column(self, col, reverse):
        """Sort treeview in orders tab by clicking on column headers"""
        data_list = [(self.orders_tree.set(k, col), k) for k in self.orders_tree.get_children('')]
        
        try:
            # Try to sort numerically if possible
            # Remove formatting characters
            clean_data = []
            for val, k in data_list:
                try:
                    if "," in val:
                        clean_data.append((float(val.replace(",", "")), k))
                    else:
                        clean_data.append((float(val), k))
                except ValueError:
                    clean_data.append((val.lower(), k))  # Sort strings case-insensitive
                    
            clean_data.sort(reverse=reverse)
            data_list = clean_data
        except ValueError:
            # Otherwise sort as string
            data_list.sort(key=lambda x: x[0].lower(), reverse=reverse)
        
        # Rearrange items in sorted positions
        for index, (val, k) in enumerate(data_list):
            self.orders_tree.move(k, '', index)
        
        # Reverse sort next time
        self.orders_tree.heading(col, command=lambda: self.sort_orders_column(col, not reverse))
    
    def view_order_chart(self):
        """View chart for the selected stock in orders tab"""
        selection = self.orders_tree.selection()
        if not selection:
            self.status_var.set("Please select an order first")
            return
            
        order_data = self.orders_tree.item(selection[0])['values']
        symbol = order_data[0] if len(order_data) > 0 else None
        
        if not symbol:
            self.status_var.set("Invalid order data")
            return
            
        self.status_var.set(f"Generating chart for {symbol}...")
        self.root.update()
        
        # Get stock details from database
        try:
            query = "SELECT id, name FROM stocks WHERE symbol = ?"
            cursor = self.conn.cursor()
            cursor.execute(query, (symbol,))
            result = cursor.fetchone()
            
            if result:
                stock_id, name = result
                
                # Store selection in tree for view_chart to use
                # Find the item in the main tree
                found = False
                for item_id in self.tree.get_children():
                    item_values = self.tree.item(item_id)['values']
                    item_symbol = item_values[2] if len(item_values) > 2 else None
                    if item_symbol == symbol:
                        # Select this item
                        self.tree.selection_set(item_id)
                        found = True
                        break
                
                if found:
                    # View the regular chart
                    self.view_chart()
                else:
                    # Show signal chart instead
                    self.view_signal_chart_from_orders(symbol)
            else:
                # Stock not found, try signal chart
                self.view_signal_chart_from_orders(symbol)
                
        except Exception as e:
            logging.error(f"Error viewing order chart: {e}", exc_info=True)
            self.status_var.set(f"Error viewing chart: {str(e)}")
    
    def view_signal_chart_from_orders(self, symbol):
        """View signal chart for a symbol from the orders tab"""
        self.status_var.set(f"Generating signal chart for {symbol}...")
        self.root.update()
        
        # Generate signal chart
        signals = self.signal_generator.analyze_stock(symbol=symbol, days=100, show_chart=True)
        
        if signals:
            self.status_var.set(f"Generated signal chart for {symbol}")
            # Update our stored signals data
            self.signals_data[symbol] = signals
        else:
            self.status_var.set(f"Could not generate signal chart for {symbol}")

    def load_screener_signals(self):
        """Fetch and display all Screener table data in the Signals tab, including Security ID from the stocks table by matching with name and symbol columns (robust normalization, two-way substring, acronym)"""
        import re
        def normalize(s):
            if not s:
                return ''
            return re.sub(r'[^A-Z0-9]', '', str(s).upper())
        def acronym(s):
            if not s:
                return ''
            return ''.join(word[0] for word in re.findall(r'\b\w', s.upper()))
        try:
            result = fetch_screener_stocks()
            headers = result.get('headers', [])
            rows = result.get('rows', [])
            if 'Security ID' not in headers:
                headers.insert(1, 'Security ID')
            for col in self.signals_tree['columns']:
                self.signals_tree.heading(col, text='')
                self.signals_tree.column(col, width=0)
            self.signals_tree['columns'] = headers
            for col in headers:
                self.signals_tree.heading(col, text=col)
                self.signals_tree.column(col, width=120, anchor="center")
            self.signals_tree.delete(*self.signals_tree.get_children())
            if not rows:
                self.status_var.set("No Screener signals found.")
                return
            # Build normalized name/symbol -> security_id mapping, and acronym -> security_id
            key_to_secid = {}
            acronym_to_secid = {}
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT name, symbol, security_id FROM stocks")
                for name, symbol, secid in cursor.fetchall():
                    norm_name = normalize(name)
                    norm_symbol = normalize(symbol)
                    key_to_secid[norm_name] = secid
                    key_to_secid[norm_symbol] = secid
                    acronym_to_secid[acronym(name)] = secid
                    acronym_to_secid[acronym(symbol)] = secid
            except Exception:
                pass
            for row in rows:
                screener_name = row.get('Name') or row.get('name')
                screener_symbol = row.get('Symbol') or row.get('symbol')
                norm_screener_name = normalize(screener_name)
                norm_screener_symbol = normalize(screener_symbol)
                screener_acronym = acronym(screener_name)
                secid = ''
                # Try direct match
                if norm_screener_name and norm_screener_name in key_to_secid:
                    secid = key_to_secid[norm_screener_name]
                elif norm_screener_symbol and norm_screener_symbol in key_to_secid:
                    secid = key_to_secid[norm_screener_symbol]
                # Two-way substring match
                if not secid:
                    for db_key, db_secid in key_to_secid.items():
                        if (norm_screener_name and (norm_screener_name in db_key or db_key in norm_screener_name)) or \
                           (norm_screener_symbol and (norm_screener_symbol in db_key or db_key in norm_screener_symbol)):
                            secid = db_secid
                            break
                # Acronym match
                if not secid and screener_acronym:
                    secid = acronym_to_secid.get(screener_acronym, '')
                values = []
                for col in headers:
                    if col == 'Security ID':
                        values.append(secid)
                    else:
                        values.append(row.get(col, ''))
                self.signals_tree.insert("", "end", values=values)
            self.status_var.set(f"Loaded {len(rows)} Screener signals.")
        except Exception as e:
            self.status_var.set(f"Error loading Screener signals: {e}")

    def place_auto_orders_for_screener(self):
        import logging
        import re
        print("place_auto_orders_for_screener called")
        logging.info("place_auto_orders_for_screener called")
        try:
            result = fetch_screener_stocks()
            print(f"Fetched screener stocks: {result}")
            rows = result.get('rows', [])
            if not rows:
                print("No Screener stocks to place orders for.")
                self.status_var.set("No Screener stocks to place orders for.")
                return
            auto_order = AutoOrderPlacer()
            placed = 0
            skipped = 0
            
            for row in rows:
                # Try to find symbol in various fields
                name_keys = ['Name', 'name', 'Company', 'company']
                symbol_keys = ['Symbol', 'symbol', 'NSE Symbol', 'BSE Symbol', 'Ticker']
                
                # First try to get symbol from symbol fields
                symbol = None
                for key in symbol_keys:
                    if key in row and row[key]:
                        symbol = row[key]
                        break
                
                # If no symbol found, try name fields
                if not symbol:
                    for key in name_keys:
                        if key in row and row[key]:
                            symbol = row[key]
                            break
                
                # Try to find price in various fields
                price_found = False
                cmp_val = None
                
                # Look for price in dedicated price fields
                price_keys = ['CMPRs.', 'cmp', 'Current Price', 'Price', 'LTP', 'Last Price']
                for key in price_keys:
                    if key in row and row[key]:
                        try:
                            price_text = str(row[key])
                            # Extract numeric part using regex
                            price_match = re.search(r'(\d+[,.]?\d*)', price_text)
                            if price_match:
                                price_value = price_match.group(1).replace(',', '')
                                cmp_val = float(price_value)
                                price_found = True
                                break
                        except Exception as e:
                            print(f"Error extracting price from {key}: {e}")
                
                # If no price found, try to find in any numeric column
                if not price_found:
                    # First try to find price in dedicated price fields, prioritizing "CMPRs."
                    price_keys = ['CMPRs.', 'CMP Rs.', 'cmp', 'Current Price', 'Price', 'LTP', 'Last Price']
                    for key in price_keys:
                        if key in row and row[key]:
                            try:
                                price_text = str(row[key])
                                # Extract numeric part using regex
                                price_match = re.search(r'(\d+[,.]?\d*)', price_text)
                                if price_match:
                                    price_value = price_match.group(1).replace(',', '')
                                    cmp_val = float(price_value)
                                    price_found = True
                                    break
                            except Exception as e:
                                logging.error(f"Error extracting price from {key}: {e}")
                    
                    # If still no price found, try to find in any numeric column
                    if not price_found:
                        for key, value in row.items():
                            # Skip non-price-like fields and explicitly exclude S.No. column
                            if key in ['S.No.', 'S No', 'Sr.', 'Sr No', 'Serial', '#'] or any(skip_word in key.lower() for skip_word in ['date', 'year', 'volume', 'quantity', 'no.', 'sl.', 'sl no', 'serial']):
                                continue
                            try:
                                if value:
                                    price_match = re.search(r'(\d+[,.]?\d*)', str(value))
                                    if price_match:
                                        price_value = price_match.group(1).replace(',', '')
                                        price = float(price_value)
                                        # Sanity check for price range - most Indian stocks are between 10 and 50,000
                                        if 10 <= price <= 50000:  # Adjusted price range for more realistic stock prices
                                            cmp_val = price
                                            price_found = True
                                            logging.info(f"Found price {price} in column {key}")
                                            break
                            except Exception:
                                continue
                
                print(f"Preparing to place order for symbol: {symbol}, cmp: {cmp_val}")
                
                if symbol and cmp_val:
                    confirmed = {
                        'symbol': symbol,
                        'entry_price': cmp_val,
                        'signal_price': cmp_val,
                        'date': None
                    }
                    try:
                        order_params = auto_order.calculate_order_params(confirmed)
                        print(f"Order params: {order_params}")
                        if order_params['position_size'] <= 0:
                            print(f"Skipping order: position size is zero for {symbol}")
                            skipped += 1
                            continue
                        result = auto_order.place_order(order_params)
                        print(f"Order result: {result}")
                        if result.get('success'):
                            placed += 1
                        else:
                            print(f"Order placement failed for {symbol}: {result.get('message')}")
                            skipped += 1
                    except Exception as e:
                        print(f"Error calculating/placing order for {symbol}: {e}")
                        logging.error(f"Error calculating/placing order for {symbol}: {e}")
                        skipped += 1
                else:
                    print(f"Skipping order: missing symbol or cmp. symbol={symbol}, cmp={cmp_val}")
                    skipped += 1
                    
            status_message = f"Placed auto orders for {placed} Screener stocks. Skipped {skipped} stocks."
            print(status_message)
            self.status_var.set(status_message)
            self.load_orders()
        except Exception as e:
            error_msg = f"Error placing auto orders: {e}"
            print(error_msg)
            logging.error(error_msg, exc_info=True)
            self.status_var.set(error_msg)

if __name__ == "__main__":
    root = ctk.CTk()
    app = StockListApp(root)
    root.mainloop()