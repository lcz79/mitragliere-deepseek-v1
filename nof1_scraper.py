import requests
import json

# =============================================================================
# URL v3 - Trovato tramite Ispezione del Traffico di Rete
# =============================================================================
# Questo è l'endpoint REALE che il sito nof1.ai chiama per popolare la classifica.
# Trovato ispezionando il traffico di rete del browser.
API_URL = "https://api.nof1.ai/api/v2/leaderboard"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
# =============================================================================


def fetch_nof1_leaderboard():
    """
    Tenta di connettersi all'API di nof1.ai (con l'URL corretto e verificato).
    """
    print(f"📡 Tentativo di connessione a: {API_URL}")

    try:
        response = requests.get(API_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()

        print("✅ Connessione riuscita! Dati ricevuti.")
        data = response.json()
        
        print("\n--- INIZIO DATI RICEVUTI ---")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("--- FINE DATI RICEVUTI ---\n")

        return data

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ Errore HTTP: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"❌ Errore di connessione: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"❌ Errore di Timeout: {timeout_err}")
    except requests.exceptions.RequestException as err:
        print(f"❌ Errore sconosciuto: {err}")
    
    return None


if __name__ == "__main__":
    print("--- Avvio dello Spione per nof1.ai (v3 con URL verificato) ---")
    leaderboard_data = fetch_nof1_leaderboard()

    if leaderboard_data:
        print("🎉 Missione compiuta! Lo spione ha scaricato i dati con successo.")
    else:
        print("😔 Qualcosa è andato storto. Non siamo riusciti a scaricare i dati.")