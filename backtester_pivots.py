import pandas as pd
import polars as pl
import pandas_ta as ta
from backtesting import Backtest, Strategy

# --- Funzione per calcolare i Punti Pivot ---
def calculate_pivots(df: pd.DataFrame) -> pd.DataFrame:
    """Calcola i punti pivot giornalieri e li mappa sul timeframe originale."""
    # 1. Resample a livello giornaliero per ottenere H, L, C del giorno prima
    daily_resample = df.resample('D').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    })
    
    # 2. Calcola i pivot sui dati del giorno *precedente*
    pivots = pd.DataFrame(index=daily_resample.index)
    pivots['High'] = daily_resample['High'].shift(1)
    pivots['Low'] = daily_resample['Low'].shift(1)
    pivots['Close'] = daily_resample['Close'].shift(1)
    
    pivots['P'] = (pivots['High'] + pivots['Low'] + pivots['Close']) / 3
    pivots['R1'] = (2 * pivots['P']) - pivots['Low']
    pivots['S1'] = (2 * pivots['P']) - pivots['High']
    pivots['R2'] = pivots['P'] + (pivots['High'] - pivots['Low'])
    pivots['S2'] = pivots['P'] - (pivots['High'] - pivots['Low'])
    
    # 3. Mappa i pivot giornalieri sull'indice originale
    # Usiamo merge_asof per "portare avanti" il valore del pivot per tutto il giorno
    df_with_pivots = pd.merge_asof(
        df.sort_index(), 
        pivots[['P', 'R1', 'S1', 'R2', 'S2']].sort_index(),
        left_index=True,
        right_index=True,
        direction='forward'
    )
    return df_with_pivots

# --- La nostra nuova strategia con i Pivot ---
class StrategiaPivot(Strategy):
    ema_len_short = 20
    rsi_len_short = 14
    rsi_entry_level = 70

    def init(self):
        # Gli indicatori standard funzionano come prima
        self.ema_short = self.I(ta.ema, pd.Series(self.data.Close), length=self.ema_len_short)
        self.rsi_short = self.I(ta.rsi, pd.Series(self.data.Close), length=self.rsi_len_short)
        
        # Gli indicatori PIVOT sono già nei dati, li rendiamo accessibili
        self.pivot_p = self.I(lambda: self.data.df['P'])
        self.pivot_r1 = self.I(lambda: self.data.df['R1'])
        self.pivot_s1 = self.I(lambda: self.data.df['S1'])

    def next(self):
        price = self.data.Close[-1]

        # CONDIZIONE DI INGRESSO LONG MIGLIORATA
        # Compra solo se il prezzo è sopra il pivot point principale (trend rialzista)
        if not self.position and \
           price > self.ema_short[-1] and \
           price > self.pivot_p[-1] and \
           self.rsi_short[-1] > self.rsi_entry_level:
            self.buy()
        
        # CONDIZIONE DI USCITA LONG (es. il prezzo rompe il supporto S1)
        elif self.position and price < self.pivot_s1[-1]:
            self.position.close()

if __name__ == "__main__":
    filename = "hist_data_BTCUSDT_3m_2023-11-01_to_2025-11-01.csv"
    print(f"1. Caricamento dati con Polars: {filename}...")
    
    try:
        # Step 1: Caricamento dati
        df_pl = pl.read_csv(filename).rename({"timestamp": "Timestamp"})
        df_pd = df_pl.to_pandas()
        df_pd['Timestamp'] = pd.to_datetime(df_pd['Timestamp'])
        df_pd.set_index('Timestamp', inplace=True)
        df_pd.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        print("2. Calcolo dei Punti Pivot...")
        # Aggiungiamo i pivot ai nostri dati PRIMA di avviare il backtest
        data_with_pivots = calculate_pivots(df_pd)
        
        print("3. Dati pronti. Avvio del backtest...")
        
        # Step 2: Esecuzione del backtest (sui dati arricchiti)
        bt = Backtest(data_with_pivots, StrategiaPivot, cash=100_000, commission=.002)
        stats = bt.run()
        
        print("\n--- 4. RISULTATI DEL BACKTEST (con PIVOT) ---")
        print(stats)
        
        print("\n5. Generazione del grafico...")
        bt.plot()
        
    except FileNotFoundError:
        print(f"ERRORE: File '{filename}' non trovato.")
    except Exception as e:
        print(f"Si è verificato un errore imprevisto: {e}")