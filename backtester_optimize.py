import pandas as pd
import polars as pl
import pandas_ta as ta
from backtesting import Backtest, Strategy
import multiprocessing  # <--- 1. AGGIUNGI QUESTO IMPORT

# ... (La funzione calculate_pivots e la classe StrategiaOttimizzabile non cambiano) ...
def calculate_pivots(df: pd.DataFrame) -> pd.DataFrame:
    daily_resample = df.resample('D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
    pivots = pd.DataFrame(index=daily_resample.index)
    pivots['High'] = daily_resample['High'].shift(1)
    pivots['Low'] = daily_resample['Low'].shift(1)
    pivots['Close'] = daily_resample['Close'].shift(1)
    pivots['P'] = (pivots['High'] + pivots['Low'] + pivots['Close']) / 3
    df_with_pivots = pd.merge_asof(df.sort_index(), pivots[['P']].sort_index(), left_index=True, right_index=True, direction='forward')
    return df_with_pivots

class StrategiaOttimizzabile(Strategy):
    ema_len_short: int
    rsi_len_short: int
    rsi_entry_level: int
    stop_loss_pct: float

    def init(self):
        close_series = pd.Series(self.data.Close)
        self.ema_short = self.I(ta.ema, close_series, length=self.ema_len_short)
        self.rsi_short = self.I(ta.rsi, close_series, length=self.rsi_len_short)
        self.pivot_p = self.I(lambda: self.data.df['P'])

    def next(self):
        price = self.data.Close[-1]
        sl_price = price * (1 - self.stop_loss_pct / 100)

        if not self.position and \
           price > self.ema_short[-1] and \
           price > self.pivot_p[-1] and \
           self.rsi_short[-1] > self.rsi_entry_level:
            self.buy(sl=sl_price)


if __name__ == "__main__":
    # --- 2. AGGIUNGI QUESTE RIGHE FONDAMENTALI ---
    try:
        multiprocessing.set_start_method("spawn")
    except RuntimeError:
        pass
    # ---------------------------------------------

    filename = "hist_data_BTCUSDT_3m_2023-11-01_to_2025-11-01.csv"
    print("1. Caricamento e preparazione dati...")
    
    try:
        df_pl = pl.read_csv(filename).rename({"timestamp": "Timestamp"})
        df_pd = df_pl.to_pandas()
        df_pd['Timestamp'] = pd.to_datetime(df_pd['Timestamp'])
        df_pd.set_index('Timestamp', inplace=True)
        df_pd.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data_with_pivots = calculate_pivots(df_pd).dropna()
        
        print("2. Avvio OTTIMIZZAZIONE con metodo 'spawn'... (potrebbe richiedere diversi minuti)")
        
        bt = Backtest(data_with_pivots, StrategiaOttimizzabile, cash=100_000, commission=.002)
        
        stats = bt.optimize(
            ema_len_short=range(10, 41, 5),
            rsi_len_short=range(10, 21, 2),
            rsi_entry_level=range(65, 81, 5),
            stop_loss_pct=[1.0, 2.0, 3.0],
            maximize='Equity Final [$]',
            constraint=lambda p: p.rsi_len_short < p.ema_len_short
        )
        
        print("\n--- 3. RISULTATI DELLA MIGLIORE COMBINAZIONE TROVATA ---")
        print(stats)
        
        print("\n--- Parametri Ottimali ---")
        print(stats._strategy)

        print("\n4. Generazione del grafico per la migliore strategia...")
        bt.plot()
        
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")
