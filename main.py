import os
import time
import logging
import sqlite3
import datetime
from typing import Dict, Any, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv

# ==========================================
# 1. CARGA DE CONFIGURACIÓN Y CREDENCIALES
# ==========================================
load_dotenv()

NANSEN_API_KEY = os.getenv("NANSEN_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not all([NANSEN_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise ValueError(
        "❌ ERROR DE CONFIGURACIÓN: Revisa tu archivo .env. "
        "Debes definir NANSEN_API_KEY, TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID."
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("cryptonomy_bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CryptonomyEngine")

# URLs oficiales de Nansen API v1
NANSEN_NETFLOW_URL = "https://api.nansen.ai/api/v1/smart-money/netflow"
NANSEN_SCREENER_URL = "https://api.nansen.ai/api/v1/token-screener"
NANSEN_DEX_TRADES_URL = "https://api.nansen.ai/api/v1/smart-money/dex-trades"

HEADERS = {
    "apikey": NANSEN_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# CORRECCIÓN: Eliminados los espacios en blanco al final de cada string
EXCLUDED_TOKENS = {
    "USDC", "USDT", "DAI", "USDE", "USDS", "FDUSD", "FRAX",
    "PYUSD", "TUSD", "LUSD", "USDD", "GUSD", "CRVUSD", "AGEUR",
    "WETH", "WBTC", "WSOL", "WMATIC", "WBNB", "WAVAX"
}

SUPPORTED_CHAINS = ["ethereum", "solana", "base", "bnb", "arbitrum"]

SIGNAL_COOLDOWN_HOURS = 12
DAILY_CREDIT_BUDGET = 2000

SMART_MONEY_WEIGHTS = {
    "Fund": 5,
    "180D Smart Trader": 5,
    "90D Smart Trader": 4,
    "30D Smart Trader": 3,
    "Smart Trader": 2,
    "Smart HL Perps Trader": 3
}

# ==========================================
# 2. GESTOR DE BASE DE DATOS LOCAL
# ==========================================
class DatabaseManager:
    def __init__(self, db_path="cryptonomy_bot.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signal_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_symbol TEXT,
                    chain TEXT,
                    signal_type TEXT,
                    timestamp DATETIME,
                    price_at_signal REAL,
                    score INTEGER,
                    risk_level TEXT,
                    status TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_budget (
                    date TEXT PRIMARY KEY,
                    credits_used INTEGER
                )
            """)
            conn.commit()

    def is_in_cooldown(self, symbol: str, chain: str, signal_type: str, cooldown_hours: int = 12) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            query = """
                SELECT timestamp FROM signal_alerts 
                WHERE token_symbol = ? AND chain = ? AND signal_type = ?
                ORDER BY timestamp DESC LIMIT 1
            """
            cursor.execute(query, (symbol, chain, signal_type))
            row = cursor.fetchone()
            if row:
                last_time = datetime.datetime.fromisoformat(row[0])
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                if last_time.tzinfo is None:
                    last_time = last_time.replace(tzinfo=datetime.timezone.utc)
                if (now_utc - last_time).total_seconds() < cooldown_hours * 3600:
                    return True
        return False

    def record_alert(self, symbol: str, chain: str, signal_type: str, price: float, score: int, risk_level: str):
        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO signal_alerts (token_symbol, chain, signal_type, timestamp, price_at_signal, score, risk_level, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, chain, signal_type, now_str, price, score, risk_level, "ALERTED"))
            conn.commit()

    def add_credits_used(self, amount: int):
        # CORRECCIÓN: Usar fecha UTC para consistencia con el resto del sistema
        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO api_budget (date, credits_used) VALUES (?, ?)
                ON CONFLICT(date) DO UPDATE SET credits_used = credits_used + ?
            """, (today, amount, amount))
            conn.commit()

    def get_credits_used_today(self) -> int:
        today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT credits_used FROM api_budget WHERE date = ?", (today,))
            row = cursor.fetchone()
            return row[0] if row else 0

db = DatabaseManager()

# ==========================================
# 3. CLIENTE HTTP RESILIENTE
# ==========================================
class NansenClient:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def post(self, url: str, payload: Dict[str, Any], credit_cost: int = 5) -> Optional[Dict[str, Any]]:
        used = db.get_credits_used_today()
        if used + credit_cost > DAILY_CREDIT_BUDGET:
            logger.warning(f"⚠️ Presupuesto de créditos superado ({used}/{DAILY_CREDIT_BUDGET}). Cancelando consulta.")
            return None
        
        try:
            res = self.session.post(url, headers=HEADERS, json=payload, timeout=15)
            if res.status_code == 200:
                db.add_credits_used(credit_cost)
                return res.json()
            elif res.status_code == 429:
                logger.error("❌ API Nansen 429: Rate Limit alcanzado. Pausando 15 segundos...")
                time.sleep(15)
            elif res.status_code in [401, 403]:
                logger.error(f"❌ Error de Autenticación Nansen ({res.status_code}): Revisa tu NANSEN_API_KEY.")
            else:
                logger.error(f"❌ Error HTTP Nansen {res.status_code}: {res.text}")
        except Exception as e:
            logger.error(f"❌ Excepción en consulta HTTP a Nansen: {e}")
        return None

client = NansenClient()

# ==========================================
# 4. ENVÍO DIRECTO A TELEGRAM
# ==========================================
def send_telegram_alert(message: str) -> bool:
    # CORRECCIÓN: Eliminados espacios en la f-string y claves del diccionario
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        data = res.json()
        if data.get("ok"):
            logger.info("✅ Alerta enviada con éxito a Telegram.")
            return True
        else:
            logger.error(f"❌ Error devuelto por Telegram: {data.get('description')}")
    except Exception as e:
        logger.error(f"❌ Excepción enviando mensaje a Telegram: {e}")
    return False

# ==========================================
# 5. MOTOR DE SCORING Y RIESGO
# ==========================================
class OpportunityEngine:
    @staticmethod
    def calculate_score_and_risk(token_data: Dict[str, Any], dex_trades: List[Dict[str, Any]] = None) -> Tuple[int, str, List[str]]:
        score = 50
        risk_score = 0
        reasons = []
        
        mcap = token_data.get("market_cap_usd") or 0
        liquidity = token_data.get("liquidity") or 0
        netflow = token_data.get("net_flow_usd") or token_data.get("net_flow_24h_usd") or 0
        fdv = token_data.get("fdv") or 0
        price_change = token_data.get("price_change") or 0
        token_age_days = token_data.get("token_age_days", 999)

        # MEJORA: Penalizar tokens demasiado nuevos (menos de 2 días) para evitar rug pulls
        if token_age_days < 2:
            risk_score += 40
            reasons.append("⚠️ Token muy nuevo (< 2 días), alto riesgo de volatilidad extrema")

        if mcap > 0:
            flow_mc_ratio = (netflow / mcap) * 100
            if flow_mc_ratio >= 1.0:
                score += 25
                reasons.append("✓ Acumulación SM Excepcional (>1% del Market Cap)")
            elif flow_mc_ratio >= 0.20:
                score += 15
                reasons.append("✓ Acumulación SM Fuerte (0.20% - 1.0% del MCap)")
            elif flow_mc_ratio < 0.05:
                score -= 10

        if liquidity > 0 and mcap > 0:
            liq_mc_ratio = liquidity / mcap
            if liq_mc_ratio < 0.05:
                risk_score += 30
                reasons.append("⚠️ Liquidez Baja en relación al Market Cap (<5%)")
            else:
                score += 10
                reasons.append("✓ Liquidez Saludable en DEX (>5% del MCap)")

        if mcap > 0 and fdv > 0:
            fdv_mc_ratio = fdv / mcap
            if fdv_mc_ratio > 10:
                risk_score += 25
                reasons.append("⚠️ Alto Riesgo de Dilución Futura (FDV/MCap > 10)")

        if dex_trades:
            labels_found = set()
            for trade in dex_trades:
                # La API de Nansen usa 'trader_address_label' en dex-trades
                label = trade.get("trader_address_label") or trade.get("label")
                if label in SMART_MONEY_WEIGHTS:
                    labels_found.add(label)
                    score += SMART_MONEY_WEIGHTS[label]
            
            if "Fund" in labels_found:
                reasons.append("✓ Participación confirmada de Fondos Institucionales")
            if len(labels_found) >= 2:
                reasons.append(f"✓ Consenso de múltiples categorías SM: {', '.join(labels_found)}")

        if price_change <= 3.0 and netflow > 0:
            score += 10
            reasons.append("✓ Precio comprimido/plano a pesar de la presión compradora (Oportunidad)")

        score = max(0, min(100, score))
        
        if risk_score >= 50:
            risk_level = "HIGH RISK"
        elif risk_score >= 25:
            risk_level = "MODERATE RISK"
        else:
            risk_level = "LOW RISK"
            
        return score, risk_level, reasons

# ==========================================
# 6. VALIDACIÓN Y PROCESAMIENTO
# ==========================================
def deep_validation(token_symbol: str, chain: str) -> List[Dict[str, Any]]:
    """Nivel 2: Consulta transacciones DEX recientes ajustando el esquema JSON a la especificación de Nansen."""
    payload = {
        "chains": [chain],
        "filters": {
            "include_smart_money_labels": ["Fund", "Smart Trader", "30D Smart Trader", "90D Smart Trader"]
        },
        "pagination": {"page": 1, "per_page": 15},
        "order_by": [{"field": "trade_value_usd", "direction": "DESC"}]
    }
    res = client.post(NANSEN_DEX_TRADES_URL, payload, credit_cost=5)
    if res and "data" in res:
        # CORRECCIÓN: La API de Nansen devuelve 'token_bought_symbol' y 'token_sold_symbol'
        return [
            trade for trade in res.get("data", []) 
            if str(trade.get("token_bought_symbol", "")).upper() == token_symbol.upper() 
            or str(trade.get("token_sold_symbol", "")).upper() == token_symbol.upper()
        ]
    return []

def process_candidate(item: Dict[str, Any], chain: str, signal_type: str, time_label: str):
    symbol = str(item.get("token_symbol", "N/A")).strip().upper()
    
    if not symbol or symbol == "N/A" or symbol in EXCLUDED_TOKENS:
        return
        
    if db.is_in_cooldown(symbol, chain, signal_type, SIGNAL_COOLDOWN_HOURS):
        logger.info(f"⏭️ {symbol} ({chain}) omitido por Cooldown activo.")
        return

    mcap = item.get("market_cap_usd") or 0
    price = item.get("price_usd") or item.get("price") or 0
    netflow = item.get("net_flow_usd") or item.get("net_flow_24h_usd") or item.get("net_flow_1h_usd") or 0
    liquidity = item.get("liquidity") or 0

    # MEJORA: Filtros base más estrictos para eliminar ruido
    if mcap < 500000 or liquidity < 100000:
        return

    dex_trades = deep_validation(symbol, chain)
    score, risk_level, reasons = OpportunityEngine.calculate_score_and_risk(item, dex_trades)

    if score < 70 and signal_type != "SMART_MONEY_DUMP_24H":
        logger.info(f"🔍 Candidato {symbol} desestimado por Score insuficiente ({score}/100)")
        return

    reasons_formatted = "\n".join([f"  {r}" for r in reasons])
    msg = (
        f"💎 <b>CRYPTONOMY SMART MONEY ENGINE V4.1</b> 💎\n\n"
        f"🪙 <b>Token:</b> ${symbol} ({chain.upper()})\n"
        f"🎯 <b>Señal:</b> {signal_type.replace('_', ' ')}\n"
        f"⏱️ <b>Horizonte:</b> {time_label}\n\n"
        f"📊 <b>CONVICTION SCORE:</b> <b>{score}/100</b>\n"
        f"🛡️ <b>Perfil de Riesgo:</b> {risk_level}\n\n"
        f"📈 <b>Métricas Clave:</b>\n"
        f"• <b>Net Flow SM:</b> +${netflow:,.2f} USD\n"
        f"• <b>Market Cap:</b> ${mcap:,.0f} USD\n"
        f"• <b>Liquidez:</b> ${liquidity:,.0f} USD\n"
        f"• <b>Precio Actual:</b> ${price:.4f}\n\n"
        f"💡 <b>¿Por qué esta señal?</b>\n"
        f"{reasons_formatted if reasons_formatted else '  ✓ Criterios de acumulación cumplidos.'}\n\n"
        f"⚙️ <i>Ejecutado con validación profunda en dos niveles.</i>"
    )
    
    if send_telegram_alert(msg):
        db.record_alert(symbol, chain, signal_type, price, score, risk_level)

# ==========================================
# 7. LAS 6 ESTRATEGIAS DE SEÑAL
# ==========================================
def signal_smart_money_inflow_1h():
    logger.info("--- [Ejecutando Señal 1: Smart Money Inflow 1H] ---")
    payload = {
        "chains": SUPPORTED_CHAINS,
        "filters": {
            "include_smart_money_labels": ["Fund", "Smart Trader", "30D Smart Trader"],
            "include_stablecoins": False
        },
        "pagination": {"page": 1, "per_page": 15},
        "order_by": [{"field": "net_flow_1h_usd", "direction": "DESC"}]
    }
    data = client.post(NANSEN_NETFLOW_URL, payload, credit_cost=5)
    if data and "data" in data:
        for item in data["data"]:
            process_candidate(item, item.get("chain", "ethereum"), "SMART_MONEY_INFLOW_1H", "1 Hora")

def signal_smart_money_inflow_24h():
    logger.info("--- [Ejecutando Señal 2: Smart Money Inflow 24H] ---")
    payload = {
        "chains": SUPPORTED_CHAINS,
        "filters": {
            "include_smart_money_labels": ["Fund", "Smart Trader", "90D Smart Trader"],
            "include_stablecoins": False
        },
        "pagination": {"page": 1, "per_page": 15},
        "order_by": [{"field": "net_flow_24h_usd", "direction": "DESC"}]
    }
    data = client.post(NANSEN_NETFLOW_URL, payload, credit_cost=5)
    if data and "data" in data:
        for item in data["data"]:
            process_candidate(item, item.get("chain", "ethereum"), "SMART_MONEY_INFLOW_24H", "24 Horas")

def signal_smart_money_dump_24h():
    logger.info("--- [Ejecutando Señal 3: Smart Money Dump 24H] ---")
    payload = {
        "chains": SUPPORTED_CHAINS,
        "filters": {
            "include_smart_money_labels": ["Fund", "Smart Trader"],
            "include_stablecoins": False
        },
        "pagination": {"page": 1, "per_page": 15},
        "order_by": [{"field": "net_flow_24h_usd", "direction": "ASC"}]
    }
    data = client.post(NANSEN_NETFLOW_URL, payload, credit_cost=5)
    if data and "data" in data:
        for item in data["data"]:
            outflow = item.get("net_flow_24h_usd") or 0
            if outflow <= -50000: # Umbral ajustado para evitar ruido de micro-ventas
                symbol = str(item.get("token_symbol", "N/A")).strip().upper()
                chain = str(item.get("chain", "N/A")).strip()
                if not db.is_in_cooldown(symbol, chain, "SM_DUMP_24H", 12):
                    msg = (
                        f"⚠️ <b>ALERTA DE RIESGO: SMART MONEY DUMP 24H</b> ⚠️\n\n"
                        f"🪙 <b>Token:</b> ${symbol} ({chain.upper()})\n"
                        f"📉 <b>Net Outflow SM:</b> ${outflow:,.2f} USD\n"
                        f"💡 <i>Instrucción: Salida masiva de capital detectada. Proteger posiciones.</i>"
                    )
                    if send_telegram_alert(msg):
                        db.record_alert(symbol, chain, "SM_DUMP_24H", 0, 0, "HIGH RISK")

def signal_screener_divergence_24h():
    logger.info("--- [Ejecutando Señal 4: Screener Divergence 24H] ---")
    payload = {
        "chains": SUPPORTED_CHAINS,
        "timeframe": "24h", # CORRECCIÓN: Parámetro requerido por la API de Nansen
        "pagination": {"page": 1, "per_page": 15},
        "filters": {"only_smart_money": True},
        "order_by": [{"field": "buy_volume", "direction": "DESC"}]
    }
    data = client.post(NANSEN_SCREENER_URL, payload, credit_cost=5)
    if data and "data" in data:
        for item in data["data"]:
            buy_vol = item.get("buy_volume") or 0
            price_change = item.get("price_change") or 0
            if buy_vol >= 25000 and price_change <= 5.0: # Umbral ligeramente subido para mayor calidad
                process_candidate(item, item.get("chain", "ethereum"), "SCREENER_DIVERGENCE_24H", "24 Horas")

def signal_vc_accumulation_7d():
    logger.info("--- [Ejecutando Señal 5: Acumulación Institucional VC 7D] ---")
    payload = {
        "chains": SUPPORTED_CHAINS,
        "filters": {
            "include_smart_money_labels": ["Fund"],
            "include_stablecoins": False
        },
        "pagination": {"page": 1, "per_page": 15},
        "order_by": [{"field": "net_flow_7d_usd", "direction": "DESC"}]
    }
    data = client.post(NANSEN_NETFLOW_URL, payload, credit_cost=5)
    if data and "data" in data:
        for item in data["data"]:
            netflow_7d = item.get("net_flow_7d_usd") or 0
            if netflow_7d >= 100000: # Umbral aumentado para asegurar relevancia institucional
                process_candidate(item, item.get("chain", "ethereum"), "VC_ACCUMULATION_7D", "7 Días")

def signal_macro_convention_30d():
    logger.info("--- [Ejecutando Señal 6: Convención Macro 30D] ---")
    payload = {
        "chains": SUPPORTED_CHAINS,
        "filters": {
            "include_smart_money_labels": ["Fund", "180D Smart Trader", "90D Smart Trader"],
            "include_stablecoins": False
        },
        "pagination": {"page": 1, "per_page": 15},
        "order_by": [{"field": "net_flow_30d_usd", "direction": "DESC"}]
    }
    data = client.post(NANSEN_NETFLOW_URL, payload, credit_cost=5)
    if data and "data" in data:
        for item in data["data"]:
            netflow_30d = item.get("net_flow_30d_usd") or 0
            if netflow_30d >= 250000: # Umbral aumentado para señales macro de alta convicción
                process_candidate(item, item.get("chain", "ethereum"), "MACRO_CONVENTION_30D", "30 Días")

# ==========================================
# 8. EJECUCIÓN CONTINUA
# ==========================================
def run_all_signals():
    logger.info("🚀 Iniciando escaneo completo de señales...")
    signal_smart_money_inflow_1h()
    signal_smart_money_inflow_24h()
    signal_screener_divergence_24h()
    signal_vc_accumulation_7d()
    signal_macro_convention_30d()
    signal_smart_money_dump_24h()
    logger.info("✅ Escaneo completado.")

if __name__ == "__main__":
    print("==================================================")
    print("🤖 CRYPTONOMY NANSEN BOT V4.1 - OPTIMIZADO")
    print("==================================================")
    send_telegram_alert("⚙️ <b>Cryptonomy Bot Actualizado:</b> Filtros anti-ruido, validación DEX y gestión de créditos optimizada. Escaneo iniciado.")
    
    try:
        run_all_signals()
        LOOP_INTERVAL_MINUTES = 30
        logger.info(f"⏳ Bot escuchando activamente. Próximo escaneo en {LOOP_INTERVAL_MINUTES} minutos...")
        while True:
            time.sleep(LOOP_INTERVAL_MINUTES * 60)
            run_all_signals()
    except KeyboardInterrupt:
        logger.info("🛑 Bot detenido manualmente por el usuario. Apagado seguro.")