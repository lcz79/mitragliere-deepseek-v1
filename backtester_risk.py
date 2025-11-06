import pandas as pd
import polars as pl
import pandas_ta as ta
from backtesting import Backtest, Strategy

# La funzione per calcolare i pivot non cambia
def calculate_pivots(df: pd.DataFrame) -> pd.DataFrame:
    daily_resample = df.resample('D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
    pivots = pd.DataFrame(index=daily_resample.index)
    pivots['High'] = daily_resample['High'].shift(1)
    pivots['Low'] = daily_resample['Low'].shift(1)
    pivots['Close'] = daily_resample['Close'].shift(1)
    pivots['P'] = (pivots['High'] + pivots['Low'] + pivots['Close']) / 3
    pivots['R1'] = (2 * pivots['P']) - pivots['Low']
    pivots['S1'] = (2 * pivots['P']) - pivots['High']
    df_with_pivots = pd.merge_asof(df.sort_index(), pivots[['P', 'R1', 'S1']].sort_index(), left_index=True, right_index=True, direction='forward')
    return df_with_pivots

# --- Strategia con GESTIONE DEL RISCHIO ---
class StrategiaRisk(Strategy):
    ema_len_short = 20
    rsi_len_short = 14
    rsi_entry_level = 70
    
    # Definiamo il nostro stop loss in percentuale
    stop_loss_pct = 2.0 # Stop loss al 2%

    def init(self):
        self.ema_short = self.I(ta.ema, pd.Series(self.data.Close), length=self.ema_len_short)
        self.rsi_short = self.I(ta.rsi, pd.Series(self.data.Close), length=self.rsi_len_short)
        self.pivot_p = self.I(lambda: self.data.df['P'])
        self.pivot_s1 = self.I(lambda: self.data.df['S1'])

    def next(self):
        price = self.data.Close[-1]
        
        # Calcoliamo il prezzo di stop loss per un potenziale acquisto
        sl_price = price * (1 - self.stop_loss_pct / 100)

        # CONDIZIONE DI INGRESSO con STOP LOSS
        if not self.position and \
           price > self.ema_short[-1] and \
           price > self.pivot_p[-1] and \
           self.rsi_short[-1] > self.rsi_entry_level:
            # Quando compriamo, impostiamo subito lo stop loss!
            self.buy(sl=sl_price)
        
        # La condizione di uscita può rimanere, ma lo SL agirà come rete di sicurezza
        elif self.position and price < self.pivot_s1[-1]:
            self.position.close()

if __name__ == "__main__":
    filename = "hist_data_BTCUSDT_3m_2023-11-01_to_2025-11-01.csv"
    print(f"1. Caricamento dati...")
    
    try:
        df_pl = pl.read_csv(filename).rename({"timestamp": "Timestamp"})
        df_pd = df_pl.to_pandas()
        df_pd['Timestamp'] = pd.to_datetime(df_pd['Timestamp'])
        df_pd.set_index('Timestamp', inplace=True)
        df_pd.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        print("2. Calcolo dei Punti Pivot...")
        data_with_pivots = calculate_pivots(df_pd)
        
        print("3. Avvio del backtest con gestione del rischio...")
        
        bt = Backtest(data_with_pivots, StrategiaRisk, cash=100_000, commission=.002)
        stats = bt.run()
        
        print("\n--- 4. RISULTATI DEL BACKTEST (con STOP LOSS) ---")
        print(stats)
        
        print("\n5. Generazione del grafico...")
        bt.plot()
        
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")