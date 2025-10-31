# =============================================================================
# MITRAGLIERE A.I. — MAESTRO MULTI-TRADE v2.0
# -----------------------------------------------------------------------------
# Bot multi-asset e multi-thread. Il Maestro dirige un trader dedicato per
# ogni asset specificato nel file config.json, eseguendo la strategia
# DeepSeek in parallelo su tutto il mercato.
# =============================================================================

import argparse
import json
import logging
import time
import threading
import pandas as pd
import ccxt
import pandas_ta as ta

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(threadName)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def load_config(path):
    """Carica la configurazione principale."""
    try:
        with open(path, 'r') as f: return json.load(f)
    except Exception as e:
        logging.error(f"ERRORE CRITICO: Impossibile caricare config.json da '{path}'. {e}")
        return None

class DeepSeekWorker:
    """Un operaio che trada un singolo asset."""
    def __init__(self, asset, config, exchange, dry_run=False):
        self.asset = asset
        self.config = config
        self.exchange = exchange
        self.dry_run = dry_run
        self.params = config['strategy_params']
        self.risk_params = config['risk_params']
        self.timeframe = config['timeframe']
        self.position = None
        logging.info(f"Operaio per {self.asset} creato.")

    def get_market_data(self, lookback=100):
        try:
            ohlcv = self.exchange.fetch_ohlcv(self.asset, self.timeframe, limit=lookback)
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df.ta.ema(length=self.params["ema_len"], append=True, col_names=(f"EMA_{self.params['ema_len']}",))
            df.ta.rsi(length=self.params["rsi_len"], append=True, col_names=(f"RSI_{self.params['rsi_len']}",))
            df.ta.atr(length=self.params["atr_len"], append=True, col_names=(f"ATR_{self.params['atr_len']}",))
            return df.dropna()
        except Exception as e:
            logging.error(f"Errore recupero dati per {self.asset}: {e}"); return None

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
        
        # Calcola la quantità in base al controvalore in USD
        trade_amount_usd = self.risk_params['trade_amount_usd']
        price = candle['close']
        amount = trade_amount_usd / price

        atr_val = candle[f"ATR_{self.params['atr_len']}"]
        sl = (price - self.params["sl_atr_mult"] * atr_val) if side == "LONG" else (price + self.params["sl_atr_mult"] * atr_val)
        tp = (price + self.params["tp_atr_mult"] * atr_val) if side == "LONG" else (price - self.params["tp_atr_mult"] * atr_val)

        logging.info(f"  - Ordine: {side} {amount:.5f} {self.asset} (controvalore: ${trade_amount_usd})")
        logging.info(f"  - StopLoss a: {sl:.4f}"); logging.info(f"  - TakeProfit a: {tp:.4f}")

        if self.dry_run:
            logging.warning("DRY RUN: Nessun ordine inviato."); return

        try:
            order_side = 'buy' if side == "LONG" else 'sell'
            logging.info("Invio ordine a mercato..."); order = self.exchange.create_market_order(self.asset, order_side, amount)
            logging.info(f"Ordine di ingresso eseguito: {order['id']}")
            logging.info("Piazzo ordini SL e TP..."); self.exchange.create_stop_loss_order(self.asset, amount, sl); self.exchange.create_take_profit_order(self.asset, amount, tp)
            logging.warning("✅ TRADE ESEGUITO CON SUCCESSO! ✅")
        except Exception as e:
            logging.error(f"ERRORE ESECUZIONE TRADE per {self.asset}: {e}")

    def run(self):
        """Ciclo di vita dell'operaio."""
        while True:
            # (La logica di gestione posizione attiva andrà qui)
            if self.position is None:
                df = self.get_market_data()
                if df is not None and not df.empty:
                    self.look_for_entry(df)
            
            sleep_time = self.config['run_params']['sleep_interval_seconds']
            time.sleep(sleep_time)

def main(args):
    """Il Maestro che dirige l'orchestra."""
    config = load_config(args.config)
    if not config: return

    keys = load_config(config['api_keys_path'])
    if not keys: return

    exchange = ccxt.bybit({'apiKey': keys['apiKey'], 'secret': keys['secret'], 'options': {'defaultType': 'swap'}})
    
    logging.info(f"🤖 MAESTRO AVVIATO. Modalità Dry Run: {'ATTIVA' if args.dry_run else 'DISATTIVATA'} 🤖")
    logging.info(f"Asset sotto monitoraggio: {config['assets_to_trade']}")

    threads = []
    for asset in config['assets_to_trade']:
        worker = DeepSeekWorker(asset, config, exchange, args.dry_run)
        thread = threading.Thread(target=worker.run, name=f"Worker-{asset.replace('/','-')}")
        threads.append(thread)
        thread.start()
        time.sleep(5) # Scagliona l'avvio per non sovraccaricare l'API

    for thread in threads:
        thread.join() # Attende che tutti i thread terminino (non accadrà mai in questo loop infinito)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mitragliere Maestro Multi-Trade v2.0')
    parser.add_argument("--config", type=str, default="config.json")
    parser.add_argument("--dry-run", action="store_true", help="Esegui in modalità simulazione")
    args = parser.parse_args()
    main(args)