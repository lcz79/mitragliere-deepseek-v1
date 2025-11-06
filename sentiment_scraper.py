import requests
import json

# =============================================================================
# Configurazione dello Spione di Sentimento
# =============================================================================
# INSERISCI QUI LA TUA CHIAVE API OTTENUTA DA CRYPTOPANIC
API_KEY = "a34f666b065e5f719d95b2eae0bd6e7a99cba69f" 

# Valuta di cui vogliamo le notizie (es. Bitcoin)
CRYPTO_SYMBOL = "BTC"
# =============================================================================


def fetch_crypto_sentiment(api_key, currency_symbol):
    """
    Si connette all'API di CryptoPanic per scaricare le ultime notizie 
    e il sentiment per una specifica criptovaluta.
    """
    api_url = f"https://cryptopanic.com/api/v1/posts/?auth_token={api_key}&currencies={currency_symbol}&public=true"
    
    print(f"📡 Tentativo di connessione a CryptoPanic per le notizie su {currency_symbol}...")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()

        print("✅ Connessione a CryptoPanic riuscita! Dati ricevuti.")
        data = response.json()
        
        print(f"\n--- ULTIME NOTIZIE PER {currency_symbol} ---")
        
        if not data.get('results'):
            print("Nessuna notizia recente trovata per questo simbolo.")
            return None

        # Analizziamo e stampiamo le ultime 5 notizie
        for news_item in data['results'][:5]:
            title = news_item.get('title', 'N/A')
            
            # CryptoPanic fornisce un voto "bullish", "bearish", etc.
            sentiment = "Neutro"
            votes = news_item.get('votes', {})
            if int(votes.get('bullish', 0)) > int(votes.get('bearish', 0)):
                sentiment = "Bullish 🐂"
            elif int(votes.get('bearish', 0)) > int(votes.get('bullish', 0)):
                sentiment = "Bearish 🐻"

            print(f"- Titolo: {title}")
            print(f"  Sentiment: {sentiment}\n")

        return data

    except requests.exceptions.RequestException as e:
        print(f"❌ Errore durante la connessione a CryptoPanic: {e}")
        return None


if __name__ == "__main__":
    if API_KEY == "LA_TUA_CHIAVE_API_VA_QUI":
        print("🛑 ERRORE: Devi inserire la tua chiave API di CryptoPanic nello script prima di eseguirlo.")
    else:
        fetch_crypto_sentiment(API_KEY, CRYPTO_SYMBOL)