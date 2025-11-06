import pandas as pd
import numpy as np
import talib
from backtesting import Backtest, Strategy
import backtesting
import multiprocessing
import warnings

warnings.filterwarnings("ignore")

# =========================
#   STRATEGIA TREND-FOLLOWING (Test Finale su 1h)
# =========================
class TrendFollowingStrategy(Strategy):
    # I parametri rimangono invariati, si adatteranno al nuovo timeframe
    leverage = 10
    risk_per_trade_pct = 1.0
    ema_fast = 20
    ema_slow = 50
    rsi_period = 14
    atr_period = 14
    volume_ma_period = 20
    atr_sl_mult = 2.0
    atr_tp_mult = 8.0
    rsi_overbought = 70
    rsi_oversold = 30
    volume_mult = 1.3
    max_bars_in_trade = 50

    def init(self):
        close = np.asarray(self.data.Close, dtype=float)
        high  = np.asarray(self.data.High, dtype=float)
        low   = np.asarray(self.data.Low, dtype=float)
        volume = np.asarray(self.data.Volume, dtype=float)

        self.ema_f = self.I(talib.EMA, close, timeperiod=self.ema_fast)
        self.ema_s = self.I(talib.EMA, close, timeperiod=self.ema_slow)
        self.rsi = self.I(talib.RSI, close, timeperiod=self.rsi_period)
        self.macd, self.macd_signal, _ = self.I(talib.MACD, close, fastperiod=12, slowperiod=26, signalperiod=9)
        self.atr = self.I(talib.ATR, high, low, close, timeperiod=self.atr_period)
        self.volume_ma = self.I(talib.SMA, volume, timeperiod=self.volume_ma_period)
        
        self.entry_bar = None
        self.total_trades = 0
        self.filtered_signals = 0

    def next(self):
        i = len(self.data.Close) - 1
        if i < max(self.ema_slow, self.volume_ma_period) + 5: return
        
        price = float(self.data.Close[-1])
        ema_f = float(self.ema_f[-1])
        ema_s = float(self.ema_s[-1])
        rsi = float(self.rsi[-1])
        atr = float(self.atr[-1])
        macd_val = float(self.macd[-1])
        macd_sig = float(self.macd_signal[-1])
        volume = float(self.data.Volume[-1])
        vol_ma = float(self.volume_ma[-1])
        
        if price == 0 or atr == 0 or self.position: return
        
        uptrend = (ema_f > ema_s) and (macd_val > 0)
        downtrend = (ema_f < ema_s) and (macd_val < 0)
        
        if not (uptrend or downtrend): return
        
        long_signal, short_signal = False, False
        
        if uptrend:
            pullback = price <= ema_f * 1.01
            rsi_ok = rsi < self.rsi_overbought and rsi > 40
            macd_bullish = macd_val > macd_sig
            volume_ok = volume > vol_ma * self.volume_mult
            long_signal = pullback and rsi_ok and macd_bullish and volume_ok
        elif downtrend:
            pullback = price >= ema_f * 0.99
            rsi_ok = rsi > self.rsi_oversold and rsi < 60
            macd_bearish = macd_val < macd_sig
            volume_ok = volume > vol_ma * self.volume_mult
            short_signal = pullback and rsi_ok and macd_bearish and volume_ok
        
        if (uptrend or downtrend) and not (long_signal or short_signal):
            self.filtered_signals += 1
        
        if long_signal or short_signal:
            stop_distance_pct = (self.atr_sl_mult * atr) / price
            if stop_distance_pct > 0:
                size_fraction = min((self.risk_per_trade_pct / 100) / stop_distance_pct, 0.95)
                if size_fraction > 0.01:
                    if long_signal:
                        sl = price - self.atr_sl_mult * atr
                        tp = price + self.atr_tp_mult * atr
                        self.buy(size=size_fraction, sl=sl, tp=tp)
                        self.entry_bar = i
                        self.total_trades += 1
                    else: # short_signal
                        sl = price + self.atr_sl_mult * atr
                        tp = price - self.atr_tp_mult * atr
                        self.sell(size=size_fraction, sl=sl, tp=tp)
                        self.entry_bar = i
                        self.total_trades += 1

# (Il resto del codice è identico, cambia solo il nome del file)
def load_csv_to_ohlc(filename: str) -> pd.DataFrame:
    df = pd.read_csv(filename)
    cols = {c.lower(): c for c in df.columns}
    ts_col = cols.get("timestamp") or cols.get("date") or cols.get("time")
    if not ts_col: raise ValueError("Colonna timestamp non trovata")
    df["Timestamp"] = pd.to_datetime(df[ts_col])
    rename_map = {}
    for std in ["Open", "High", "Low", "Close", "Volume"]:
        for variant in [std.lower(), std.capitalize(), std.upper()]:
            if variant in df.columns: rename_map[variant] = std; break
    df = df.rename(columns=rename_map)
    needed = ["Open", "High", "Low", "Close", "Volume"]
    if not all(c in df.columns for c in needed): raise ValueError(f"Mancano colonne OHLCV")
    df = df[["Timestamp"] + needed].copy()
    df.set_index("Timestamp", inplace=True)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')].dropna()
    return df

if __name__ == "__main__":
    backtesting.Pool = multiprocessing.Pool
    
    # <<< MODIFICA CHIAVE: Usiamo i dati a 1 ora >>>
    filename = "hist_data_BTCUSDT_1h.csv"
    
    print("="*70 + "\n   📈 STRATEGIA TREND-FOLLOWING (Test Finale su 1h)\n" + "="*70)
    
    try:
        print(f"\n1️⃣  Carico dati 1h: {filename}")
        data = load_csv_to_ohlc(filename)
        print(f"   ✓ Caricati {len(data)} candelieri")
        
        initial_cash = 10_000
        leverage = 10
        
        print(f"\n2️⃣  Configurazione:")
        print(f"   💰 Capitale: ${initial_cash:,.0f} | Leva: {leverage}x | Rischio: 1% | R:R 1:2")
        
        bt = Backtest(data, TrendFollowingStrategy, cash=initial_cash, commission=0.00075, margin=1/leverage)
        
        print(f"\n3️⃣  Avvio backtest su 1h...")
        stats = bt.run()
        
        print("\n" + "="*70 + "\n   📊 RISULTATI (1h)\n" + "="*70)
        print(stats)
        
        if stats['# Trades'] > 0:
            print("\n" + "="*70 + "\n   ✅ METRICHE CHIAVE (1h)\n" + "="*70)
            print(f"Return [%]:           {stats['Return [%]']:.2f}%")
            print(f"Win Rate:             {stats['Win Rate [%]']:.2f}%")
            print(f"Max Drawdown:         {stats['Max. Drawdown [%]']:.2f}%")
            print(f"Total Trades:         {stats['# Trades']}")
        else:
            print("\n⚠️  NESSUN TRADE ESEGUITO")
        
        print(f"\n4️⃣  Salvo grafico...")
        bt.plot(filename="backtest_trend_following_1h.html", open_browser=True)
        print(f"   ✓ Grafico: backtest_trend_following_1h.html")

    except FileNotFoundError:
        print(f"\n❌ File '{filename}' non trovato!")
    except Exception as e:
        print(f"\n❌ Errore: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
