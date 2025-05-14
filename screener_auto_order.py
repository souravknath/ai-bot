# screener_auto_order.py
import requests
from bs4 import BeautifulSoup
import sys
import os
import re
import logging
from auto_order import AutoOrderPlacer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f"screener_auto_order.log"
)

SCREENER_URL = "https://www.screener.in/screen/raw/?sort=&order=&source_id=2520868&query=Current+price+%3E+DMA+50+*+0.99+AND%0D%0ACurrent+price+%3C+DMA+50+*+1.01+AND%0D%0ARSI+%3E+50+AND%0D%0AMarket+Capitalization+%3E+10000"
COOKIES = {
    'csrftoken': 'Qvx5wX5lmLEXAUJTlrzYtN9t3vxfPWQD',
    'sessionid': 'pa6hyitb81jlj5hcmjetvba7u8psqf1u',
}
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'referer': SCREENER_URL,
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
}

def fetch_screener_stocks():
    response = requests.get(SCREENER_URL, headers=HEADERS, cookies=COOKIES)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    if not table:
        print("No table found on the page.")
        logging.error("No table found on the screener page")
        return {'headers': [], 'rows': []}

    # Get headers
    headers = []
    header_row = table.find('tr')
    if header_row:
        for th in header_row.find_all(['th', 'td']):
            headers.append(th.get_text(strip=True))
    
    # Find important column indices
    cmp_col_idx = None
    name_col_idx = None
    symbol_col_idx = None
    
    # More comprehensive check for price columns
    for i, h in enumerate(headers):
        h_lower = h.strip().lower()
        if h_lower == "cmprs." or h in ["CMPRs.", "CMP Rs."]:  # Prioritize this column first
            cmp_col_idx = i
            break
        elif h_lower in ["current price", "cmp", "price", "cmp(rs)"]:
            cmp_col_idx = i
        elif h_lower in ["name", "company"]:
            name_col_idx = i
        elif h_lower in ["symbol", "ticker", "nse symbol", "bse symbol"]:
            symbol_col_idx = i
    
    logging.info(f"Headers detected: {headers}")
    logging.info(f"CMP column index: {cmp_col_idx}, Name column index: {name_col_idx}, Symbol column index: {symbol_col_idx}")
    
    # Print headers for debugging
    print('Screener headers:', headers)
    
    # Print the raw text of the first 3 data rows for debugging
    all_rows = table.find_all('tr')[1:]
    print('First 3 data rows (raw text):')
    for row in all_rows[:3]:
        print([td.get_text(strip=True) for td in row.find_all('td')])
    
    # Get all rows
    stocks = []
    for row in all_rows:
        cols = row.find_all('td')
        if not cols or len(cols) != len(headers):
            continue
        
        stock = {}
        for i, col in enumerate(cols):
            text = col.get_text(strip=True)
            link = col.find('a')
            if link:
                text = link.get_text(strip=True)
            stock[headers[i]] = text
        
        # Find price value in any possible column
        found_price = False
        if cmp_col_idx is not None:
            try:
                price_text = cols[cmp_col_idx].get_text(strip=True)
                # Extract numeric price from text that might contain "Rs." or other text
                price_match = re.search(r'(\d+[,.]?\d*)', price_text)
                if price_match:
                    price_value = price_match.group(1).replace(',', '')
                    stock['cmp'] = float(price_value)
                    found_price = True
            except Exception as e:
                logging.error(f"Error extracting price: {e}")
        
        # If price not found in designated column, try to find in any column with a number
        if not found_price:
            for i, col in enumerate(cols):
                text = col.get_text(strip=True)
                # Skip columns that are likely not price columns
                if any(word in headers[i].lower() for word in ['date', 'year', 'volume', 'quantity']):
                    continue
                try:
                    # Look for numbers that could be prices
                    price_match = re.search(r'(\d+[,.]?\d*)', text)
                    if price_match:
                        price_value = price_match.group(1).replace(',', '')
                        price = float(price_value)
                        # Basic sanity check: most stock prices are between 1 and 100,000
                        if 1 <= price <= 100000:
                            stock['cmp'] = price
                            found_price = True
                            logging.info(f"Found price {price} in column {headers[i]}")
                            break
                except Exception:
                    pass

        # Make sure we have both a symbol and a price
        if not stock.get('cmp'):
            stock['cmp'] = None
            logging.warning(f"Could not determine price for stock: {stock}")
            
        # Try to get both name and symbol
        stock['symbol'] = None
        stock['name'] = None
        
        # Find the symbol and name based on detected columns
        if symbol_col_idx is not None:
            stock['symbol'] = stock.get(headers[symbol_col_idx])
        if name_col_idx is not None:
            stock['name'] = stock.get(headers[name_col_idx])
            
        # If we don't have a symbol yet, use the name or the first column as fallback
        if not stock['symbol'] and stock['name']:
            stock['symbol'] = stock['name']
        elif not stock['symbol'] and stock.get(headers[0]):
            stock['symbol'] = stock.get(headers[0])
            
        # If we don't have a name yet, use the symbol as fallback
        if not stock['name'] and stock['symbol']:
            stock['name'] = stock['symbol']
            
        stocks.append(stock)
        
    logging.info(f"Processed {len(stocks)} stocks from screener")
    return {'headers': headers, 'rows': stocks}

def main():
    result = fetch_screener_stocks()
    stocks = result.get('rows', [])
    if not stocks:
        print("No stocks found.")
        logging.error("No stocks found in screener results")
        return
        
    # Filter for stocks with valid price data
    valid_stocks = [s for s in stocks if s.get('cmp') is not None]
    print(f"Found {len(valid_stocks)} stocks with valid prices out of {len(stocks)} total stocks")
    logging.info(f"Found {len(valid_stocks)} stocks with valid prices out of {len(stocks)} total stocks")
    
    if not valid_stocks:
        print("No stocks with valid price data.")
        logging.error("No stocks with valid price data")
        return
    
    # Sort by cmp ascending
    valid_stocks.sort(key=lambda x: x.get('cmp', 0))
    print(f"Found {len(valid_stocks)} stocks with valid prices. Ordering...")
    
    auto_order = AutoOrderPlacer()
    for stock in valid_stocks:
        symbol = stock.get('symbol')
        price = stock.get('cmp')
        name = stock.get('name')
        
        if not symbol or not price:
            print(f"Skipping incomplete stock data: {stock}")
            logging.warning(f"Skipping incomplete stock data: {stock}")
            continue
            
        print(f"Ordering {symbol} ({name}) at CMP {price}")
        logging.info(f"Preparing order for {symbol} ({name}) at price {price}")
        
        # Prepare a confirmed order dict as expected by calculate_order_params
        confirmed = {
            'symbol': symbol,
            'entry_price': price,
            'signal_price': price,
            'date': None
        }
        
        try:
            order_params = auto_order.calculate_order_params(confirmed)
            if order_params['position_size'] <= 0:
                print(f"Skipping order: position size is zero for {symbol}")
                logging.warning(f"Skipping order: position size is zero for {symbol}")
                continue
                
            result = auto_order.place_order(order_params)
            print(f"Order result: {result}")
            logging.info(f"Order result for {symbol}: {result}")
        except Exception as e:
            print(f"Error placing order for {symbol}: {e}")
            logging.error(f"Error placing order for {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    main()
