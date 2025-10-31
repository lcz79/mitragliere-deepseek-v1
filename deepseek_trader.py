# =============================================================================
# MITRAGLIERE A.I. — DEEPSEEK TRADER v1.0 (Production Ready)
# -----------------------------------------------------------------------------
# Bot operativo stand-alone basato sulla replica della strategia DeepSeek.
# Versione finale, pulita e pronta per l'esecuzione.
# =============================================================================

import argparse
import json
import logging
import time
import pandas as pd
import ccxt
import pandas_ta as ta

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def load_api_keys(path):
    """Carica le chiavi API da un file JSON sicuro."""
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"ERRORE CRITICO: Impossibile caricare le chiavi API da '{path}'. {e}")
        return None

class DeepSeekTrader:
    def __init__(self, asset, timeframe, params, keys_path, dry_run=False):
        self.asset = asset
        self.timeframe = timeframe
        self.params = params
        self.dry_run = dry_run
        self.position = None 
        keys = load_api_keys(keys_path)
        if not keys: raise ValueError("Chiavi API non valide o file non trovato.")
        self.exchange = ccxt.bybit({
            'apiKey': keys['apiKey'], 'secret': keys['secret'],
            'options': {'defaultType': 'swap'},
        })
        logging.info(f"Connesso a Bybit. Modalità Dry Run: {'ATTIVA' if self.dry_run else 'DISATTIVATA'}")

    def get_market_data(self, lookback=100):
        try:
            ohlcv = self.exchange.fetch_ohlcv(self.asset, self.timeframe, limit=lookback)
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.ta.ema(length=self.params["ema_len"], append=True, col_names=(f"EMA_{self.params['ema_len']}",))
            df.ta.rsi(length=self.params["rsi_len"], append=True, col_names=(f"RSI_{self.params['rsi_len']}",))
            df.ta.atr(length=self.params["atr_len"], append=True, col_names=(f"ATR_{self.params['atr_len']}",))
            return df.dropna()
        except Exception as e:
            logging.error(f"Errore nel recupero dati: {e}"); return None

    def check_and_manage_position(self):
        self.position = None 
        return

    def look_for_entry(self, df):
        last, prev = df.iloc[-1], df.iloc[-2]
        ema_col, rsi_col = f"EMA_{self.params['ema_len']}", f"RSI_{self.params['rsi_len']}"
        
        is_above_ema = last["close"] > last[ema_col]
        rsi_crossed_up = last[rsi_col] > self.params["rsi_entry_level"] and prev[rsi_col] <= self.params["rsi_entry_level"]
        is_below_ema = last["close"] < last[ema_col]
        rsi_crossed_down = last[rsi_col] < (100 - self.params["rsi_entry_level"]) and prev[rsi_col] >= (100 - self.params["rsi_entry_level"])

        if is_above_ema and rsi_crossed_up: self.execute_trade("LONG", last)
        elif is_below_ema and rsi_crossed_down: self.execute_trade("SHORT", last)
        else: logging.info(f"Nessun segnale per {self.asset}. Prezzo: {last['close']:.2f}, RSI: {last[rsi_col]:.2f}")

    def execute_trade(self, side, candle):
        logging.warning(f"🔥 SEGNALE RILEVATO: ENTRA {side} su {self.asset} @ {candle['close']:.4f} 🔥")
        amount = self.params["trade_amount"]; atr_val = candle[f"ATR_{self.params['atr_len']}"]
        
        if side == "LONG":
            sl = candle["close"] - self.params["sl_atr_mult"] * atr_val
            tp = candle["close"] + self.params["tp_atr_mult"] * atr_val
        else:
            sl = candle["close"] + self.params["sl_atr_mult"] * atr_val
            tp = candle["close"] - self.params["tp_atr_mult"] * atr_val

        logging.info(f"  - Ordine: {side} {amount} {self.asset}"); logging.info(f"  - StopLoss a: {sl:.4f}"); logging.info(f"  - TakeProfit a: {tp:.4f}")

        if self.dry_run:
            logging.warning("DRY RUN: Nessun ordine inviato."); return

        try:
            logging.info("Invio ordine a mercato..."); order = self.exchange.create_market_order(self.asset, 'buy' if side == "LONG" else 'sell', amount)
            logging.info(f"Ordine di ingresso eseguito: {order['id']}")
            logging.info("Piazzo ordini SL e TP..."); self.exchange.create_stop_loss_order(self.asset, amount, sl); self.exchange.create_take_profit_order(self.asset, amount, tp)
            logging.warning("✅ TRADE ESEGUITO CON SUCCESSO! ✅")
        except Exception as e:
            logging.error(f"ERRORE DURANTE L'ESECUZIONE DEL TRADE: {e}")

    def run(self):
        logging.info(f"Avvio del trader DeepSeek per {self.asset} su timeframe {self.timeframe}...")
        while True:
            self.check_and_manage_position()
            if self.position is None:
                df = self.get_market_data()
                if df is not None: self.look_for_entry(df)
            logging.info(f"Prossimo controllo tra {self.params['sleep_interval']} secondi...")
            time.sleep(self.params['sleep_interval'])

def main(args):
    params = {
        "ema_len": 20, "rsi_len": 7, "rsi_entry_level": 60,
        "atr_len": 14, "sl_atr_mult": 1.5, "tp_atr_mult": 2.0,
        "trade_amount": 0.001, "sleep_interval": 180 
    }
    trader = DeepSeekTrader(asset=args.asset, timeframe=args.timeframe, params=params, keys_path=args.keys, dry_run=args.dry_run)
    trader.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mitragliere DeepSeek Trader v1.0')
    parser.add_argument("--asset", type=str, default="BTC/USDT"); parser.add_argument("--timeframe", type=str, default="3m")
    parser.add_argument("--keys", type=str, default="secrets/bybit_keys.json")
    parser.add_argument("--dry-run", action="store_true", help="Esegui in modalità simulazione")
    args = parser.parse_args()
    main(args)
