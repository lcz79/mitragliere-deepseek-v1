from backtesting import Backtest, Strategy
import polars as pl
import pandas as pd

# Funzione EMA (corretta)
def ema(series: pl.Series, length: int) -> pl.Series:
    return series.ewm_mean(span=length, adjust=False)

# Funzione RSI (SINTASSI CORRETTA e logica robusta)
def rsi(series: pl.Series, length: int) -> pl.Series:
    delta = series.diff()
    
    # SINTASSI CORRETTA: usare pl.when(), non delta.when()
    gain = pl.when(delta > 0).then(delta).otherwise(0)
    loss = -pl.when(delta < 0).then(delta).otherwise(0)
    
    avg_gain = gain.ewm_mean(span=length, adjust=False)
    avg_loss = loss.ewm_mean(span=length, adjust=False)
    
    # Se avg_loss è zero, rs sarà infinito. Dobbiamo gestirlo.
    rs = avg_gain / avg_loss
    
    # Logica robusta per gestire divisioni per zero
    # quando rs è inf o nan, l'RSI dovrebbe essere 100 (segnale fortissimo)
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val.fill_nan(50).fill_null(50).clip(0, 100) # Pulizia finale

class StrategiaMadre(Strategy):
    ema_len_short = 20
    rsi_len_short = 7
    rsi_entry_level = 65

    def init(self):
        close_series_pl = pl.from_pandas(pd.Series(self.data.Close, name="Close"))
        
        ema_pl = ema(close_series_pl, self.ema_len_short)
        rsi_pl = rsi(close_series_pl, self.rsi_len_short)
        
        # Inseriamo i valori calcolati nella strategia
        self.ema_short = self.I(lambda: ema_pl.to_numpy())
        self.rsi_short = self.I(lambda: rsi_pl.to_numpy())
    
    def next(self):
        # La logica di trading rimane invariata
        if not self.position and self.data.Close[-1] > self.ema_short[-1] and self.rsi_short[-1] > self.rsi_entry_level:
            self.buy()
        elif not self.position and self.data.Close[-1] < self.ema_short[-1] and self.rsi_short[-1] < (100 - self.rsi_entry_level):
            self.sell()
        
        # Logica di chiusura (semplificata per ora)
        if self.position and self.data.Close[-1] < self.ema_short[-1]:
             self.position.close()

if __name__ == "__main__":
    filename = "hist_data_BTCUSDT_3m_2023-11-01_to_2025-11-01.csv"
    print(f"Caricamento ed elaborazione del file: {filename}...")
    
    try:
        df_pl = pl.read_csv(filename).rename({"timestamp": "Timestamp"})
        df_pd = df_pl.to_pandas()
        df_pd['Timestamp'] = pd.to_datetime(df_pd['Timestamp'])
        df_pd.set_index('Timestamp', inplace=True)
        df_pd.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        print("Dati caricati. Avvio del backtest...")
        # Usiamo un capitale più alto per Bitcoin
        bt = Backtest(df_pd, StrategiaMadre, cash=100_000, commission=.002)
        stats = bt.run()
        
        print("\n--- RISULTATI DEL BACKTEST ---")
        print(stats)
        
        print("\nGenerazione del grafico dei risultati... (controlla la finestra del browser)")
        bt.plot()
        
    except FileNotFoundError:
        print(f"ERRORE: File '{filename}' non trovato. Esegui prima data_downloader.py.")
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")
