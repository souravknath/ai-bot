import customtkinter as ctk
import sqlite3
import logging
import pandas as pd
from tkinter import ttk
import tkinter as tk
import plotly.graph_objects as go
from plotly.offline import plot
import webbrowser
import os
from datetime import datetime, timedelta
from stock_fetcher import StockFetcher

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
        
        # Initialize stock fetcher
        self.stock_fetcher = StockFetcher()
        
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
        
        # Status label
        self.status_var = tk.StringVar()
        self.status_label = ctk.CTkLabel(self.search_frame, textvariable=self.status_var)
        self.status_label.pack(side="right", padx=10)
        
        # Create treeview frame
        self.tree_frame = ctk.CTkFrame(self.main_frame)
        self.tree_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Create Treeview
        self.tree = ttk.Treeview(self.tree_frame, columns=("id", "security_id", "symbol", "name", "exchange", "instrument", "added_date", "last_updated"))
        
        # Define columns
        self.tree.column("#0", width=0, stretch=False)  # Hide the first column
        self.tree.column("id", width=50, anchor="center")
        self.tree.column("security_id", width=100, anchor="center")
        self.tree.column("symbol", width=100, anchor="center")
        self.tree.column("name", width=250, anchor="w")
        self.tree.column("exchange", width=100, anchor="center")
        self.tree.column("instrument", width=100, anchor="center")
        self.tree.column("added_date", width=150, anchor="center")
        self.tree.column("last_updated", width=150, anchor="center")
        
        # Define column headings
        self.tree.heading("id", text="ID", command=lambda: self.sort_column("id", False))
        self.tree.heading("security_id", text="Security ID", command=lambda: self.sort_column("security_id", False))
        self.tree.heading("symbol", text="Symbol", command=lambda: self.sort_column("symbol", False))
        self.tree.heading("name", text="Name", command=lambda: self.sort_column("name", False))
        self.tree.heading("exchange", text="Exchange", command=lambda: self.sort_column("exchange", False))
        self.tree.heading("instrument", text="Instrument", command=lambda: self.sort_column("instrument", False))
        self.tree.heading("added_date", text="Added Date", command=lambda: self.sort_column("added_date", False))
        self.tree.heading("last_updated", text="Last Updated", command=lambda: self.sort_column("last_updated", False))
        
        # Add double-click binding for quick chart view
        self.tree.bind("<Double-1>", lambda event: self.view_chart())
        
        # Add scrollbars
        self.y_scrollbar = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree.yview)
        self.x_scrollbar = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.y_scrollbar.set, xscrollcommand=self.x_scrollbar.set)
        
        # Pack scrollbars and treeview
        self.y_scrollbar.pack(side="right", fill="y")
        self.x_scrollbar.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)
        
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
        
        # Load data initially
        self.load_data()
    
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

if __name__ == "__main__":
    root = ctk.CTk()
    app = StockListApp(root)
    root.mainloop()