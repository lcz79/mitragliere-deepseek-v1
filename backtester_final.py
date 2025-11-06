from backtesting import Backtest, Strategy
import polars as pl
import pandas_ta as ta # Importiamo la libreria pandas-ta
import pandas as pd

class StrategiaSemplice(Strategy):
    # Parametri della strategia
    ema_len_short = 20
    rsi_len_short = 14 # Usiamo un valore standard per l'RSI
    rsi_entry_level = 70 # Valore standard per ipercomprato

    def init(self):
        # Usiamo pandas-ta per calcolare gli indicatori.
        # È il modo standard e corretto per backtesting.py.
        # Il calcolo è comunque molto veloce.
        self.ema_short = self.I(ta.ema, pd.Series(self.data.Close), length=self.ema_len_short)
        self.rsi_short = self.I(ta.rsi, pd.Series(self.data.Close), length=self.rsi_len_short)
    
    def next(self):
        # Logica di trading
        price = self.data.Close[-1]

        # CONDIZIONE DI INGRESSO LONG
        if not self.position and price > self.ema_short[-1] and self.rsi_short[-1] > self.rsi_entry_level:
            self.buy()
        
        # CONDIZIONE DI USCITA LONG (es. chiusura sotto l'EMA)
        elif self.position and price < self.ema_short[-1]:
            self.position.close()

if __name__ == "__main__":
    filename = "hist_data_BTCUSDT_3m_2023-11-01_to_2025-11-01.csv"
    print(f"1. Caricamento dati con Polars: {filename}...")
    
    try:
        # Step 1: Caricamento e preparazione dati (qui Polars è perfetto)
        df_pl = pl.read_csv(filename).rename({"timestamp": "Timestamp"})
        df_pd = df_pl.to_pandas()
        df_pd['Timestamp'] = pd.to_datetime(df_pd['Timestamp'])
        df_pd.set_index('Timestamp', inplace=True)
        # La libreria di backtesting richiede nomi specifici
        df_pd.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        print("2. Dati pronti. Avvio del backtest...")
        
        # Step 2: Esecuzione del backtest
        bt = Backtest(df_pd, StrategiaSemplice, cash=100_000, commission=.002)
        stats = bt.run()
        
        print("\n--- 3. RISULTATI DEL BACKTEST ---")
        print(stats)
        
        print("\n4. Generazione del grafico dei risultati... (controlla la finestra del browser)")
        bt.plot()
        
    except FileNotFoundError:
        print(f"ERRORE: File '{filename}' non trovato. Esegui prima data_downloader.py.")
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")