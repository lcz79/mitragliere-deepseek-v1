import pandas as pd
import polars as pl
import pandas_ta as ta
from backtesting import Backtest, Strategy
import backtesting
import multiprocessing

# La funzione per calcolare i pivot non cambia
def calculate_pivots(df: pd.DataFrame) -> pd.DataFrame:
    daily_resample = df.resample('D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
    pivots = pd.DataFrame(index=daily_resample.index)
    pivots['High'] = daily_resample['High'].shift(1)
    pivots['Low'] = daily_resample['Low'].shift(1)
    pivots['Close'] = daily_resample['Close'].shift(1)
    pivots['P'] = (pivots['High'] + pivots['Low'] + pivots['Close']) / 3
    df_with_pivots = pd.merge_asof(df.sort_index(), pivots[['P']].sort_index(), left_index=True, right_index=True, direction='forward')
    return df_with_pivots

# --- STRATEGIA CON TAKE PROFIT BASATO SU RISK/REWARD ---
class StrategiaConRR(Strategy):
    # Definiamo i parametri come variabili di classe
    ema_len_short = 20
    rsi_len_short = 14
    rsi_entry_level = 70
    stop_loss_pct = 2.0
    risk_reward_ratio = 1.5 # Il nostro nuovo parametro!

    def init(self):
        close_series = pd.Series(self.data.Close)
        self.ema_short = self.I(ta.ema, close_series, length=self.ema_len_short)
        self.rsi_short = self.I(ta.rsi, close_series, length=self.rsi_len_short)
        self.pivot_p = self.I(lambda: self.data.df['P'])

    def next(self):
        price = self.data.Close[-1]
        
        # Calcoliamo sia lo stop loss che il take profit
        sl_price = price * (1 - self.stop_loss_pct / 100)
        tp_price = price * (1 + (self.stop_loss_pct * self.risk_reward_ratio) / 100)

        if not self.position and \
           price > self.ema_short[-1] and \
           price > self.pivot_p[-1] and \
           self.rsi_short[-1] > self.rsi_entry_level:
            # Quando compriamo, impostiamo SIA lo stop-loss (sl) SIA il take-profit (tp)!
            self.buy(sl=sl_price, tp=tp_price)

if __name__ == "__main__":
    backtesting.Pool = multiprocessing.Pool
    
    filename = "hist_data_BTCUSDT_3m_2023-11-01_to_2025-11-01.csv"
    print("1. Caricamento e preparazione dati...")
    
    try:
        df_pl = pl.read_csv(filename).rename({"timestamp": "Timestamp"})
        df_pd = df_pl.to_pandas()
        df_pd['Timestamp'] = pd.to_datetime(df_pd['Timestamp'])
        df_pd.set_index('Timestamp', inplace=True)
        df_pd.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data_with_pivots = calculate_pivots(df_pd).dropna()
        
        print("2. Avvio OTTIMIZZAZIONE con Risk/Reward... (potrebbe richiedere diversi minuti)")
        
        bt = Backtest(data_with_pivots, StrategiaConRR, cash=100_000, commission=.002)
        
        stats = bt.optimize(
            ema_len_short=range(10, 41, 10),
            rsi_len_short=range(10, 21, 5),
            rsi_entry_level=range(65, 81, 5),
            stop_loss_pct=[1.0, 2.0, 3.0],
            # Aggiungiamo il nostro nuovo parametro allo spazio di ricerca!
            risk_reward_ratio=[1.5, 2.0, 2.5], 
            maximize='SQN', # Massimizziamo lo 'System Quality Number', una metrica più robusta
            constraint=lambda p: p.rsi_len_short < p.ema_len_short
        )
        
        print("\n--- 3. RISULTATI DELLA MIGLIORE COMBINAZIONE TROVATA (con RR) ---")
        print(stats)
        
        print("\n--- Parametri Ottimali ---")
        print(stats._strategy)

        print("\n4. Generazione del grafico...")
        bt.plot(filename='backtest_results_rr.html', open_browser=True)
        print("Grafico salvato in 'backtest_results_rr.html'.")
        
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")