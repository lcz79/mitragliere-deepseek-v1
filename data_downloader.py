import ccxt
import pandas as pd
import time
import argparse
from datetime import datetime, timedelta

def download_data(symbol, start_date_str, end_date_str, timeframe):
    """
    Scarica i dati storici da Bybit in modo incrementale e li salva in un file CSV.
    """
    exchange = ccxt.bybit({'options': {'defaultType': 'swap'}})
    
    # Converte le stringhe di data in millisecondi UTC
    start_date = int(datetime.strptime(start_date_str, '%Y-%m-%d').timestamp() * 1000)
    end_date = int(datetime.strptime(end_date_str, '%Y-%m-%d').timestamp() * 1000)
    
    filename = f"hist_data_{symbol.replace('/', '')}_{timeframe}_{start_date_str}_to_{end_date_str}.csv"
    
    all_ohlcv = []
    current_date = start_date
    
    print(f"Inizio download per {symbol} dal {start_date_str} al {end_date_str}...")
    
    while current_date < end_date:
        try:
            # Scarica un blocco di dati (Bybit ha un limite, es. 1000 candele)
            ohlcv = exchange.fetch_ohlcv(f"{symbol}:USDT", timeframe, since=current_date, limit=1000)
            
            if len(ohlcv) == 0:
                print("Nessun altro dato disponibile. Fine del download.")
                break
            
            all_ohlcv.extend(ohlcv)
            
            # Avanza al timestamp dell'ultima candela scaricata + 1
            last_timestamp = ohlcv[-1][0]
            current_date = last_timestamp + 1 
            
            # Mostra il progresso
            last_date_str = datetime.fromtimestamp(last_timestamp / 1000).strftime('%Y-%m-%d')
            print(f"  Dati scaricati fino a: {last_date_str} ({len(all_ohlcv)} candele totali)")
            
            # Rispetta i rate limit dell'exchange
            time.sleep(exchange.rateLimit / 1000)
            
        except Exception as e:
            print(f"Errore durante il download: {e}. Riprovo tra 5 secondi...")
            time.sleep(5)
            
    # Crea un DataFrame e salvalo
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df.to_csv(filename)
    
    print(f"\nDownload completato. Dati salvati in: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloader di dati storici da Bybit.")
    parser.add_argument("--symbol", type=str, required=True, help="Simbolo da scaricare (es. BTC/USDT)")
    parser.add_argument("--start", type=str, default="2023-11-01", help="Data di inizio (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2025-11-01", help="Data di fine (YYYY-MM-DD)")
    parser.add_argument("--timeframe", type=str, default="3m", help="Timeframe (es. 3m, 4h, 1d)")
    
    args = parser.parse_args()
    download_data(args.symbol, args.start, args.end, args.timeframe)