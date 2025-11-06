import ccxt
import pandas as pd
import datetime
import time

def download_crypto_data(symbol, timeframe, since, filename):
    """
    Scarica i dati storici OHLCV per un dato simbolo e li salva in un file CSV.
    """
    # Usiamo l'exchange Binance, uno standard per i dati storici
    exchange = ccxt.binance({
        'rateLimit': 1200,  # Limite di sicurezza per le richieste
        'enableRateLimit': True,
    })

    print(f"Connessione a Binance per scaricare i dati di {symbol}...")

    # Converte la data 'since' in millisecondi UTC, come richiesto da ccxt
    since_timestamp = exchange.parse8601(since)
    
    all_ohlcv = []
    limit = 1000  # Limite massimo di candele per richiesta di Binance

    while since_timestamp < exchange.milliseconds():
        try:
            print(f"Recupero {limit} candele dal {exchange.iso8601(since_timestamp)}...")
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since_timestamp, limit)
            
            if len(ohlcv):
                since_timestamp = ohlcv[-1][0] + 1  # Imposta il timestamp per la richiesta successiva
                all_ohlcv.extend(ohlcv)
                # Pausa per rispettare il rate limit dell'exchange e non essere bloccati
                time.sleep(exchange.rateLimit / 1000) 
            else:
                break # Nessun dato restituito, il periodo è terminato

        except (ccxt.ExchangeError, ccxt.NetworkError) as error:
            print(f"Errore di rete o dell'exchange: {str(error)}. Riprovo tra 30 secondi...")
            time.sleep(30)
            continue
            
    if not all_ohlcv:
        print("Nessun dato scaricato. Verifica il simbolo o il periodo.")
        return

    print(f"\nDownload completato. Totale candele scaricate: {len(all_ohlcv)}")

    # Creazione del DataFrame pandas
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Salvataggio in CSV
    print(f"Salvataggio dei dati nel file: {filename}...")
    df.to_csv(filename, index=False)
    print(f"✓ Dati salvati con successo in '{filename}'!")


if __name__ == '__main__':
    # --- CONFIGURAZIONE ---
    simbolo = 'BTC/USDT'
    timeframe_dati = '1h'
    data_inizio = '2023-11-01T00:00:00Z' # Formato ISO 8601 UTC
    nome_file_output = f"hist_data_{simbolo.replace('/', '')}_{timeframe_dati}.csv"
    
    download_crypto_data(simbolo, timeframe_dati, data_inizio, nome_file_output)