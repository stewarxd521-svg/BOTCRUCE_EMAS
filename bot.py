"""
EMA Strategy Bot v2 — WebSocket-First Edition
══════════════════════════════════════════════════════════════════════════════════
Integración con KlineWebSocketCache v4:

  ANTES  (v1):  Cada 60 s → REST /fapi/v1/klines × N_símbolos → RAM → EMAs
  AHORA  (v2):  WebSocket siempre activo → buffers en RAM → EMAs sobre datos
                locales → CERO peticiones REST en operación normal.

ARQUITECTURA
────────────
  ┌─────────────────────────────────────────────────────────────────┐
  │  KlineWebSocketCache (hilo daemon con su propio event loop)     │
  │    1. Backfill REST inicial  (1 vez al arrancar)                │
  │    2. WebSocket klines 1m    → buffers thread-safe              │
  │    3. Monitor de reloj       → cierra velas por close_time      │
  │    4. Safety refresh         → red de seguridad cada 10 min     │
  └────────────────────────────┬────────────────────────────────────┘
                               │ get_dataframe() – thread-safe
  ┌────────────────────────────▼────────────────────────────────────┐
  │  Bot (event loop principal de asyncio)                          │
  │    • run_scan()  → lee caché → detect_signal() → abre trades   │
  │    • ws_price_loop() → miniTicker → verifica TP/SL en tiempo   │
  │      real → cierra trades                                       │
  │    • Dashboard HTTP + API /api/state                            │
  └─────────────────────────────────────────────────────────────────┘

CAMBIOS RESPECTO A v1
─────────────────────
  ❌ Eliminado : get_klines_multi()   (REST por símbolo en cada ciclo)
  ❌ Eliminado : process_symbol()     (descargaba, parseaba, detectaba)
  ❌ Eliminado : semáforo MAX_CONCURRENT en scan (no hay I/O REST)
  ✅ Añadido   : KlineWebSocketCache — fuente de datos principal
  ✅ Añadido   : init_cache()         — crea/reinicia el caché
  ✅ Añadido   : wait_cache_ready()   — espera backfill inicial
  ✅ Mejorado  : run_scan()           — lee memoria, ejecuta en thread pool
  ✅ Mejorado  : bot_loop()           — gestiona ciclo de vida del caché
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp
from aiohttp import web

# ── Importación del caché WebSocket ──────────────────────────────────────────
# Ambos archivos deben estar en el mismo directorio.
from KlineWebSocketCache_v4 import KlineWebSocketCache


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN  (variables de entorno → compatibles con Render / Railway)
# ══════════════════════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8700613197:AAFu7KAP3_9joN8Jq76r3ZcKIZiGcUWzSc4")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "1474510598")

EXECUTOR_URL    = os.environ.get("EXECUTOR_URL",    "https://executor-5lu0.onrender.com")
# ⚠️  DEBE coincidir exactamente con SIGNAL_SECRET en futures_executor.py
EXECUTOR_SECRET = os.environ.get("EXECUTOR_SECRET", "clave-secreta-aleatoria")

# ── EMAs ─────────────────────────────────────────────────────────────────────
EMA_FAST = int(os.environ.get("EMA_FAST",   "35"))
EMA_MID  = int(os.environ.get("EMA_MID",    "300"))
EMA_SLOW = int(os.environ.get("EMA_SLOW",   "1500"))

# Spread mínimo entre EMA_SLOW y EMA_MID (en %) para considerar señal válida
SPREAD_PCT = float(os.environ.get("SPREAD_PCT", "1.0"))

# ── Caché de velas ───────────────────────────────────────────────────────────
# Tamaño del buffer: suficiente para EMA_SLOW + margen confortable
MAX_CACHE_CANDLES = max(EMA_SLOW + 200, 1_700)   # e.g. 1 700 velas de 1m

# ── Paper Trading ─────────────────────────────────────────────────────────────
INITIAL_BALANCE = float(os.environ.get("INITIAL_BALANCE", "3000.0"))
USDT_PER_TRADE  = float(os.environ.get("USDT_PER_TRADE",  "50.0"))
MAX_LONGS       = int(os.environ.get("MAX_LONGS",  "30"))
MAX_SHORTS      = int(os.environ.get("MAX_SHORTS", "30"))
TP_PCT          = float(os.environ.get("TP_PCT", "1.0"))
SL_PCT          = float(os.environ.get("SL_PCT", "4.0"))

# ── Binance ───────────────────────────────────────────────────────────────────
QUOTE_ASSET  = "USDT"
BINANCE_REST = "https://fapi.binance.com"
BINANCE_WS   = "wss://fstream.binance.com"
INTERVAL     = "1m"

# ── Ciclo del bot ─────────────────────────────────────────────────────────────
PORT              = int(os.environ.get("PORT", "10000"))
SCAN_INTERVAL     = 60        # segundos entre detecciones de señales EMA
SYMBOLS_REFRESH_H = 6         # horas entre refrescado de la lista de símbolos

# ── Parámetros del KlineWebSocketCache ───────────────────────────────────────
CACHE_STREAMS_PER_CONN      = 50
CACHE_REST_CONCURRENCY      = 20
CACHE_BACKFILL_BATCH_SIZE   = 8
CACHE_BACKFILL_BATCH_DELAY  = 0.10
CACHE_RATE_LIMIT_CAPACITY   = 1_200
CACHE_RATE_LIMIT_REFILL     = 20.0
CACHE_SILENCE_THRESHOLD_S   = 120
CACHE_HEALTH_CHECK_S        = 60
CACHE_SAFETY_REFRESH_S      = 600


# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("EMA-Bot-v2")


# ══════════════════════════════════════════════════════════════════════════════
#  ESTADO GLOBAL DEL BOT
# ══════════════════════════════════════════════════════════════════════════════

bot_status: dict = {
    "last_scan"         : "Iniciando...",
    "total_alerts"      : 0,
    "symbols_monitored" : 0,
    "last_alerts"       : [],       # últimas 10 señales EMA detectadas
    "cache_ready"       : False,    # True cuando el backfill inicial terminó
}

# Instancia global del caché de klines (inicializada en bot_loop)
_cache: Optional[KlineWebSocketCache] = None


# ══════════════════════════════════════════════════════════════════════════════
#  PAPER TRADING — Modelo de datos
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    """Representa una operación simulada (paper trade)."""
    id            : int
    symbol        : str
    direction     : str           # "LONG" | "SHORT"
    entry_price   : float
    quantity      : float         # unidades del activo base
    usdt_size     : float         # USDT comprometidos
    open_time     : str
    tp_price      : float
    sl_price      : float
    current_price : float = 0.0
    status        : str   = "OPEN"    # "OPEN" | "TP" | "SL"
    close_price   : float = 0.0
    close_time    : str   = ""
    pnl_usdt      : float = 0.0
    roi_pct       : float = 0.0
    signal_mode       : str   = "normal"    # qué modo estaba activo al abrir
    original_signal   : str   = ""          # señal EMA antes de invertir
    effective_tp_pct  : float = TP_PCT      # % TP real aplicado
    effective_sl_pct  : float = SL_PCT      # % SL real aplicado


class TradeManager:
    """
    Gestiona el portafolio simulado.
    Usa asyncio.Lock para apertura/cierre; update_price y trades_to_close
    son síncronos (sin I/O) y seguros en contextos asyncio.
    """

    # Umbral de drawdown global (en USDT) para cierre de emergencia
    GLOBAL_SL_USDT = 6.5
    # Umbral de ganancia global (en USDT) para toma de beneficios global
    GLOBAL_TP_USDT = 7.0

    def __init__(self) -> None:
        self.balance             = INITIAL_BALANCE
        self.cycle_start_balance = INITIAL_BALANCE   # Balance al inicio del ciclo actual
        self.trades              : list[Trade] = []
        self._counter            = 0
        self._lock               = asyncio.Lock()
        self._global_sl_lock     = asyncio.Lock()    # Evita doble disparo del cierre global SL
        self._global_tp_lock     = asyncio.Lock()    # Evita doble disparo del cierre global TP
        MODE_NORMAL_TP   = 1.0   # TP modo normal
        MODE_NORMAL_SL   = 4.0   # SL modo normal
        MODE_INVERTED_TP = 4.0   # TP modo invertido
        MODE_INVERTED_SL = 1.0   # SL modo invertido

        self.signal_mode  = "normal"   # ciclo 1 siempre inicia normal
        self.signal_cycle = 1

    # ── Propiedades de consulta ───────────────────────────────────────────────

    @property
    def open_trades(self) -> list[Trade]:
        return [t for t in self.trades if t.status == "OPEN"]

    @property
    def closed_trades(self) -> list[Trade]:
        return [t for t in self.trades if t.status != "OPEN"]

    @property
    def open_longs(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "LONG"]

    @property
    def open_shorts(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "SHORT"]

    @property
    def active_symbols(self) -> set:
        return {t.symbol for t in self.open_trades}

    @property
    def total_realized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.closed_trades)

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.open_trades)

    @property
    def equity(self) -> float:
        return self.balance + sum(t.usdt_size + t.pnl_usdt for t in self.open_trades)

    # ── Abrir operación ───────────────────────────────────────────────────────

    async def open_trade(
        self, symbol: str, direction: str, price: float
    ) -> Optional[Trade]:
        async with self._lock:
            if symbol in self.active_symbols:
                return None
            if self.signal_mode == "inverted":
                effective_dir = "SHORT" if direction == "LONG" else "LONG"
                tp_pct = 4.0  # TP grande
                sl_pct = 1.0  # SL pequeño
            else:
                effective_dir = direction  # sin cambio
                tp_pct = 1.0
                sl_pct = 4.0
                
            direction=effective_dir
                
            if direction == "LONG" and len(self.open_longs) >= MAX_LONGS:
                return None
            if direction == "SHORT" and len(self.open_shorts) >= MAX_SHORTS:
                return None
            if self.balance < USDT_PER_TRADE:
                return None

            if direction == "LONG":
                tp_price = price * (1 + tp_pct / 100)
                sl_price = price * (1 - sl_pct / 100)
            else:
                tp_price = price * (1 - tp_pct / 100)
                sl_price = price * (1 + sl_pct / 100)

            self._counter += 1
            trade = Trade(
                id            = self._counter,
                symbol        = symbol,
                direction     = direction,
                entry_price   = price,
                quantity      = USDT_PER_TRADE / price,
                usdt_size     = USDT_PER_TRADE,
                open_time     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                tp_price      = tp_price,
                sl_price      = sl_price,
                current_price = price,
            )

            self.balance -= USDT_PER_TRADE
            self.trades.append(trade)
            log.info(
                f"[TRADE #{trade.id}] ABIERTO {direction} {symbol} "
                f"@ ${price:.8f} | TP: ${tp_price:.8f} | SL: ${sl_price:.8f} "
                f"| {USDT_PER_TRADE:.2f} USDT → {trade.quantity:.6f} "
                f"{symbol.replace('USDT', '')}"
            )
            return trade

    # ── Cerrar operación (helper interno, sin lock) ───────────────────────────

    def _apply_close(self, trade: Trade, close_price: float, reason: str) -> None:
        """Aplica el cierre de un trade. Debe llamarse DENTRO del lock."""
        trade.status      = reason
        trade.close_price = close_price
        trade.close_time  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        if trade.direction == "LONG":
            trade.pnl_usdt = (close_price - trade.entry_price) * trade.quantity
        else:
            trade.pnl_usdt = (trade.entry_price - close_price) * trade.quantity

        trade.roi_pct = (trade.pnl_usdt / trade.usdt_size) * 100
        self.balance  += trade.usdt_size + trade.pnl_usdt

        log.info(
            f"[TRADE #{trade.id}] CERRADO {reason} {trade.symbol} "
            f"@ ${close_price:.8f} | PnL: {trade.pnl_usdt:+.4f} USDT "
            f"({trade.roi_pct:+.2f}%)"
        )

    # ── Cerrar una operación individual ──────────────────────────────────────

    async def close_trade(
        self, trade: Trade, close_price: float, reason: str
    ) -> bool:
        async with self._lock:
            if trade.status != "OPEN":
                return False
            self._apply_close(trade, close_price, reason)
            return True

    # ── Cierre global por drawdown (-7 USDT sobre balance de ciclo) ──────────

    async def close_all_trades_global(self) -> list[Trade]:
        """
        Cierra TODAS las posiciones abiertas a precio actual (cierre global SL).
        Tras el cierre, resetea cycle_start_balance con el nuevo balance.
        Retorna la lista de trades cerrados.
        """
        closed: list[Trade] = []
        async with self._lock:
            for trade in list(self.open_trades):
                price = trade.current_price if trade.current_price > 0 else trade.entry_price
                self._apply_close(trade, price, "GLOBAL_SL")
                closed.append(trade)

            # ── Inicio del nuevo ciclo ────────────────────────────────────────
            self.cycle_start_balance = self.balance
            self.signal_mode = "inverted" if self.signal_mode == "normal" else "normal"
            self.signal_cycle += 1
            log.warning(
                f"[GLOBAL SL] {len(closed)} posiciones cerradas | "
                f"Nuevo balance de ciclo: {self.balance:.2f} USDT"
            )
        return closed

    # ── Cierre global por TP (+21 USDT sobre balance de ciclo) ──────────────

    async def close_all_trades_global_tp(self) -> list[Trade]:
        """
        Cierra TODAS las posiciones abiertas a precio actual (cierre global TP).
        Se dispara cuando el equity supera cycle_start_balance + GLOBAL_TP_USDT.
        Tras el cierre, resetea cycle_start_balance con el nuevo balance.
        Retorna la lista de trades cerrados.
        """
        closed: list[Trade] = []
        async with self._lock:
            for trade in list(self.open_trades):
                price = trade.current_price if trade.current_price > 0 else trade.entry_price
                self._apply_close(trade, price, "GLOBAL_TP")
                closed.append(trade)

            # ── Inicio del nuevo ciclo ────────────────────────────────────────
            self.cycle_start_balance = self.balance
            self.signal_mode = "inverted" if self.signal_mode == "normal" else "normal"
            self.signal_cycle += 1
            log.info(
                f"[GLOBAL TP] {len(closed)} posiciones cerradas | "
                f"Nuevo balance de ciclo: {self.balance:.2f} USDT"
            )
        return closed

    # ── Drawdown actual vs inicio de ciclo ───────────────────────────────────

    @property
    def cycle_drawdown(self) -> float:
        """Pérdida en USDT desde el inicio del ciclo actual (negativo = pérdida)."""
        return self.equity - self.cycle_start_balance

    # ── Actualización de precio (síncrono) ────────────────────────────────────

    def update_price(self, symbol: str, price: float) -> None:
        for t in self.open_trades:
            if t.symbol == symbol:
                t.current_price = price
                if t.direction == "LONG":
                    t.pnl_usdt = (price - t.entry_price) * t.quantity
                else:
                    t.pnl_usdt = (t.entry_price - price) * t.quantity
                t.roi_pct = (t.pnl_usdt / t.usdt_size) * 100

    # ── Verificar TP/SL (síncrono) ────────────────────────────────────────────

    def trades_to_close(self, symbol: str, price: float) -> list[tuple]:
        result = []
        for t in self.open_trades:
            if t.symbol != symbol:
                continue
            if t.direction == "LONG":
                if price >= t.tp_price:
                    result.append((t, "TP"))
                elif price <= t.sl_price:
                    result.append((t, "SL"))
            else:
                if price <= t.tp_price:
                    result.append((t, "TP"))
                elif price >= t.sl_price:
                    result.append((t, "SL"))
        return result


# Instancia global del gestor de trades
trade_manager = TradeManager()


# ══════════════════════════════════════════════════════════════════════════════
#  EMA — cálculo puro Python (sin pandas/numpy)
# ══════════════════════════════════════════════════════════════════════════════

def calc_ema(closes: list, period: int) -> list:
    """Calcula EMA completa. Retorna lista del mismo largo (None al inicio)."""
    if len(closes) < period:
        return []
    k   = 2.0 / (period + 1)
    ema = [None] * (period - 1)
    ema.append(sum(closes[:period]) / period)
    for price in closes[period:]:
        ema.append(price * k + ema[-1] * (1 - k))
    return ema


def detect_signal(closes: list) -> Optional[dict]:
    """
    Detecta señal LONG o SHORT según la estrategia EMA triple.

    LONG  → EMA_FAST[-1] y EMA_MID[-1] < EMA_SLOW[-1]
             spread(EMA_SLOW, EMA_MID) >= SPREAD_PCT
             EMA_FAST cruzó ARRIBA EMA_MID (prev <, actual >)

    SHORT → EMA_FAST[-1] y EMA_MID[-1] > EMA_SLOW[-1]
             spread(EMA_SLOW, EMA_MID) >= SPREAD_PCT
             EMA_FAST cruzó ABAJO  EMA_MID (prev >, actual <)
    """
    # Mínimo: EMA_SLOW necesita exactamente EMA_SLOW velas para tener un valor
    if len(closes) < EMA_SLOW:
        return None

    fast_list = calc_ema(closes, EMA_FAST)
    mid_list  = calc_ema(closes, EMA_MID)
    slow_list = calc_ema(closes, EMA_SLOW)

    if (not fast_list or not mid_list or not slow_list
            or fast_list[-1] is None or mid_list[-1] is None or slow_list[-1] is None
            or fast_list[-2] is None or mid_list[-2] is None):
        return None

    cf, pf = fast_list[-1], fast_list[-2]
    cm, pm = mid_list[-1],  mid_list[-2]
    cs     = slow_list[-1]

    spread = abs(cs - cm) / cs * 100
    if spread < SPREAD_PCT:
        return None

    if cf < cs and cm < cs and pf < pm and cf > cm:
        return {"signal": "LONG",  "cf": cf, "cm": cm, "cs": cs, "spread": spread}

    if cf > cs and cm > cs and pf > pm and cf < cm:
        return {"signal": "SHORT", "cf": cf, "cm": cm, "cs": cs, "spread": spread}

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM — mensajes
# ══════════════════════════════════════════════════════════════════════════════

async def send_telegram(session: aiohttp.ClientSession, message: str) -> None:
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                log.error(f"Telegram error {resp.status}: {await resp.text()}")
    except Exception as e:
        log.error(f"Error Telegram: {e}")


async def notify_executor(
    session: aiohttp.ClientSession, payload: dict
) -> None:
    if not EXECUTOR_URL:
        return
    url = EXECUTOR_URL.rstrip("/") + "/signal"
    try:
        async with session.post(
            url,
            json=payload,
            headers={"X-Signal-Secret": EXECUTOR_SECRET},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 200:
                log.info(
                    f"Executor ✅ {payload.get('action','?').upper()} "
                    f"{payload.get('symbol','?')} → HTTP {resp.status}"
                )
            elif resp.status == 401:
                log.error(
                    f"Executor ❌ 401 Unauthorized — verifica que EXECUTOR_SECRET "
                    f"(bot) == SIGNAL_SECRET (executor). action={payload.get('action')}"
                )
            else:
                body = await resp.text()
                log.warning(
                    f"Executor ⚠️ HTTP {resp.status} "
                    f"action={payload.get('action')} symbol={payload.get('symbol')} "
                    f"| {body[:120]}"
                )
    except Exception as e:
        log.warning(f"notify_executor [{payload.get('action')}]: {e}")


def build_open_message(trade: Trade, ema_result: dict, change_1m: float) -> str:
    emoji = "🟢" if trade.direction == "LONG" else "🔴"
    word  = "LONG  ▲" if trade.direction == "LONG" else "SHORT ▼"
    cond  = (
        "EMA35 y EMA300 BAJO EMA1500\nEMA35 cruzó ↑ EMA300"
        if trade.direction == "LONG" else
        "EMA35 y EMA300 SOBRE EMA1500\nEMA35 cruzó ↓ EMA300"
    )
    base = trade.symbol.replace("USDT", "")
    return (
        f"{emoji} <b>📂 POSICIÓN ABIERTA — {word}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>         <code>{trade.symbol}</code>\n"
        f"💰 <b>Entrada:</b>    <code>${trade.entry_price:,.8f}</code>\n"
        f"📉 <b>Cambio 1m:</b>  <code>{change_1m:+.2f}%</code>\n"
        f"📦 <b>Tamaño:</b>     <code>{USDT_PER_TRADE:.2f} USDT "
        f"({trade.quantity:.6f} {base})</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Take Profit:</b> <code>${trade.tp_price:,.8f}</code>  "
        f"<i>(+{TP_PCT}%)</i>\n"
        f"🛑 <b>Stop Loss:</b>   <code>${trade.sl_price:,.8f}</code>  "
        f"<i>(-{SL_PCT}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 EMA{EMA_FAST}:    <code>{ema_result['cf']:.8f}</code>\n"
        f"📈 EMA{EMA_MID}:   <code>{ema_result['cm']:.8f}</code>\n"
        f"📈 EMA{EMA_SLOW}: <code>{ema_result['cs']:.8f}</code>\n"
        f"📐 <b>Spread EMA{EMA_SLOW}-EMA{EMA_MID}:</b> "
        f"<code>{ema_result['spread']:.2f}%</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <i>{cond}</i>\n"
        f"💼 <b>Balance libre:</b> <code>{trade_manager.balance:.2f} USDT</code>\n"
        f"📊 <b>Posiciones:</b> "
        f"<code>{len(trade_manager.open_longs)}L / "
        f"{len(trade_manager.open_shorts)}S</code>\n"
        f"🆔 Trade <b>#{trade.id}</b>  |  ⏱ {trade.open_time}"
    )


def build_signal_no_trade_message(
    symbol: str, result: dict, price: float
) -> str:
    emoji = "🟢" if result["signal"] == "LONG" else "🔴"
    limit = MAX_LONGS if result["signal"] == "LONG" else MAX_SHORTS
    return (
        f"{emoji} <b>SEÑAL {result['signal']} — Sin trade</b>\n"
        f"📊 <code>{symbol}</code>  @ <code>${price:,.8f}</code>\n"
        f"📐 Spread: <code>{result['spread']:.2f}%</code>\n"
        f"⚠️ <i>Máx. posiciones {result['signal']} alcanzado ({limit})"
        f" o balance insuficiente</i>"
    )


def build_close_message(trade: Trade) -> str:
    if trade.status == "TP":
        emoji, reason = "✅", "TAKE PROFIT 🎯"
    else:
        emoji, reason = "❌", "STOP LOSS 🛑"

    dir_str   = "🟢 LONG" if trade.direction == "LONG" else "🔴 SHORT"
    pnl_emoji = "💚" if trade.pnl_usdt >= 0 else "❗"
    wins      = sum(1 for t in trade_manager.closed_trades if t.status == "TP")
    total     = len(trade_manager.closed_trades)
    wr_str    = f"{wins}/{total} ({wins/total*100:.1f}%)" if total else "N/A"

    return (
        f"{emoji} <b>POSICIÓN CERRADA — {reason}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>      <code>{trade.symbol}</code>  {dir_str}\n"
        f"💵 <b>Entrada:</b> <code>${trade.entry_price:,.8f}</code>\n"
        f"💵 <b>Salida:</b>  <code>${trade.close_price:,.8f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{pnl_emoji} <b>PnL:</b>    <code>{trade.pnl_usdt:+.4f} USDT</code>\n"
        f"📊 <b>ROI:</b>    <code>{trade.roi_pct:+.2f}%</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Abierto:  {trade.open_time}\n"
        f"⏱ Cerrado:  {trade.close_time}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 <b>Balance:</b>  <code>{trade_manager.balance:.2f} USDT</code>\n"
        f"💼 <b>Equity:</b>   <code>{trade_manager.equity:.2f} USDT</code>\n"
        f"📈 <b>Win Rate:</b> <code>{wr_str}</code>\n"
        f"🆔 Trade <b>#{trade.id}</b>"
    )


def build_global_sl_message(closed_trades: list, prev_cycle_balance: float) -> str:
    """Mensaje Telegram cuando se dispara el cierre global por drawdown -6.5 USDT."""
    total_pnl = sum(t.pnl_usdt for t in closed_trades)
    n         = len(closed_trades)
    new_bal   = trade_manager.balance
    drawdown  = new_bal - prev_cycle_balance

    detail_lines = "\n".join(
        f"  #{t.id} {t.symbol} {t.direction}: "
        f"<code>{t.pnl_usdt:+.4f} USDT ({t.roi_pct:+.2f}%)</code>"
        for t in closed_trades[:15]
    )
    suffix = f"\n  <i>…y {n - 15} más</i>" if n > 15 else ""

    return (
        f"🚨 <b>CIERRE GLOBAL — DRAWDOWN -{TradeManager.GLOBAL_SL_USDT:.1f} USDT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ El equity cayó más de <b>{TradeManager.GLOBAL_SL_USDT:.1f} USDT</b> "
        f"desde el inicio del ciclo.\n"
        f"🔒 Se cerraron <b>{n} posiciones</b> al precio actual.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 Balance inicio de ciclo: <code>{prev_cycle_balance:.2f} USDT</code>\n"
        f"💼 Balance nuevo (ciclo):   <code>{new_bal:.2f} USDT</code>\n"
        f"📉 Drawdown del ciclo:      <code>{drawdown:+.4f} USDT</code>\n"
        f"📊 PnL total cerrado:       <code>{total_pnl:+.4f} USDT</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Operaciones cerradas:</b>\n"
        f"{detail_lines}{suffix}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 <i>Nuevo ciclo iniciado. Balance base: {new_bal:.2f} USDT</i>"
    )


def build_global_tp_message(closed_trades: list, prev_cycle_balance: float) -> str:
    """Mensaje Telegram cuando se dispara el cierre global por take profit +21 USDT."""
    total_pnl = sum(t.pnl_usdt for t in closed_trades)
    n         = len(closed_trades)
    new_bal   = trade_manager.balance
    profit    = new_bal - prev_cycle_balance

    detail_lines = "\n".join(
        f"  #{t.id} {t.symbol} {t.direction}: "
        f"<code>{t.pnl_usdt:+.4f} USDT ({t.roi_pct:+.2f}%)</code>"
        for t in closed_trades[:15]
    )
    suffix = f"\n  <i>…y {n - 15} más</i>" if n > 15 else ""

    return (
        f"🏆 <b>CIERRE GLOBAL — TAKE PROFIT +21 USDT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ El equity superó <b>+{TradeManager.GLOBAL_TP_USDT:.0f} USDT</b> "
        f"desde el inicio del ciclo.\n"
        f"🔒 Se cerraron <b>{n} posiciones</b> al precio actual.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 Balance inicio de ciclo: <code>{prev_cycle_balance:.2f} USDT</code>\n"
        f"💼 Balance nuevo (ciclo):   <code>{new_bal:.2f} USDT</code>\n"
        f"📈 Ganancia del ciclo:      <code>{profit:+.4f} USDT</code>\n"
        f"📊 PnL total cerrado:       <code>{total_pnl:+.4f} USDT</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Operaciones cerradas:</b>\n"
        f"{detail_lines}{suffix}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 <i>Nuevo ciclo iniciado. Balance base: {new_bal:.2f} USDT</i>"
    )


def init_cache(symbols: List[str]) -> KlineWebSocketCache:
    """
    Crea e inicia una nueva instancia de KlineWebSocketCache.
    Si ya existe una instancia global, la detiene primero.
    """
    global _cache

    if _cache is not None:
        log.info("🛑 Deteniendo caché anterior...")
        try:
            _cache.stop()
        except Exception as e:
            log.warning(f"Error deteniendo caché: {e}")
        _cache = None

    log.info(
        f"🔧 Inicializando KlineWebSocketCache: "
        f"{len(symbols)} símbolos | max_candles={MAX_CACHE_CANDLES}"
    )

    _cache = KlineWebSocketCache(
        pairs               = {s: [INTERVAL] for s in symbols},
        max_candles         = MAX_CACHE_CANDLES,
        include_open_candle = True,       # La última vela abierta entra en el buffer
        backfill_on_start   = True,       # Backfill REST al arrancar
        rest_limits         = {INTERVAL: min(MAX_CACHE_CANDLES, 1_500)},
        streams_per_connection          = CACHE_STREAMS_PER_CONN,
        rest_concurrency                = CACHE_REST_CONCURRENCY,
        rest_retries                    = 4,
        rest_backoff_max                = 30.0,
        rest_min_sleep                  = 0.05,
        backfill_batch_size             = CACHE_BACKFILL_BATCH_SIZE,
        backfill_batch_delay            = CACHE_BACKFILL_BATCH_DELAY,
        rate_limit_capacity             = CACHE_RATE_LIMIT_CAPACITY,
        rate_limit_refill               = CACHE_RATE_LIMIT_REFILL,
        stream_silence_threshold_seconds= CACHE_SILENCE_THRESHOLD_S,
        stream_health_check_seconds     = CACHE_HEALTH_CHECK_S,
        safety_refresh_interval_seconds = CACHE_SAFETY_REFRESH_S,
    )
    _cache.start()
    return _cache


async def wait_cache_ready(
    expected: int,
    timeout: float = 360.0,
    poll_interval: float = 10.0,
) -> bool:
    """
    Espera hasta que ≥90 % de los pares tengan datos en el caché.
    Retorna True si se logró antes del timeout; False en caso contrario.

    Nota: «con datos» en KlineWebSocketCache equivale a que el backfill
    completó al menos una vela para ese par. En segundos after,
    detect_signal() descartará pares con < EMA_SLOW velas (silencioso).
    """
    required = max(1, int(expected * 0.90))
    deadline = time.time() + timeout
    last_log = 0.0

    while time.time() < deadline:
        stats = _cache.get_stats()
        ready = stats.get("pairs_with_data", 0)

        now = time.time()
        if now - last_log >= 20:
            log.info(
                f"⏳ Caché: {ready}/{expected} pares con datos "
                f"({ready * 100 // max(expected, 1)}%) — "
                f"tokens RL: {stats.get('rate_limiter_tokens', '?'):.0f}"
            )
            last_log = now

        if ready >= required:
            log.info(f"✅ Caché listo: {ready}/{expected} pares con datos")
            bot_status["cache_ready"] = True
            return True

        await asyncio.sleep(poll_interval)

    log.warning(
        f"⚠️ Timeout wait_cache_ready: {_cache.get_stats()['pairs_with_data']}"
        f"/{expected} pares — continuando de todas formas"
    )
    bot_status["cache_ready"] = True   # Permite al bot funcionar con datos parciales
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET — Precios en tiempo real (miniTicker de Binance Futures)
#  Sigue siendo responsable del monitoreo TP / SL en tiempo real.
# ══════════════════════════════════════════════════════════════════════════════

async def ws_price_loop(session: aiohttp.ClientSession) -> None:
    """
    Mantiene un WebSocket de miniTicker con Binance Futures para todos los
    símbolos con posición abierta.

    • Verifica TP/SL en cada tick de precio recibido.
    • Reconecta automáticamente si el conjunto de símbolos cambia o la
      conexión se cae.
    """
    log.info("WebSocket Price Manager (miniTicker) — iniciado")
    last_symbols    : frozenset = frozenset()
    reconnect_delay : float     = 3.0

    while True:
        symbols = frozenset(trade_manager.active_symbols)

        if not symbols:
            if last_symbols:
                log.info("WS miniTicker: Sin posiciones activas, cerrando conexión")
            last_symbols    = symbols
            reconnect_delay = 3.0
            await asyncio.sleep(2)
            continue

        if symbols != last_symbols:
            log.info(
                f"WS miniTicker: Conectando con {len(symbols)} símbolo(s): "
                f"{', '.join(sorted(symbols))}"
            )

        streams = "/".join(f"{s.lower()}@miniTicker" for s in sorted(symbols))
        url     = f"{BINANCE_WS}/stream?streams={streams}"

        try:
            async with session.ws_connect(
                url,
                heartbeat=20,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as ws:
                last_symbols    = symbols
                reconnect_delay = 3.0
                log.info("WS miniTicker: ✅ Conectado")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data   = json.loads(msg.data)
                            ticker = data.get("data", {})
                            sym    = ticker.get("s")
                            price  = float(ticker.get("c") or 0)

                            if sym and price > 0:
                                trade_manager.update_price(sym, price)

                                # ── Check drawdown global (-6.5 USDT del ciclo) ──────
                                if (
                                    trade_manager.open_trades
                                    and trade_manager.cycle_drawdown
                                        < -TradeManager.GLOBAL_SL_USDT
                                ):
                                    async with trade_manager._global_sl_lock:
                                        # Re-verificar dentro del lock para evitar doble disparo
                                        if (
                                            trade_manager.open_trades
                                            and trade_manager.cycle_drawdown
                                                < -TradeManager.GLOBAL_SL_USDT
                                        ):
                                            prev_bal = trade_manager.cycle_start_balance
                                            log.warning(
                                                f"[GLOBAL SL] Equity: {trade_manager.equity:.2f} | "
                                                f"Ciclo inicio: {prev_bal:.2f} | "
                                                f"Drawdown: {trade_manager.cycle_drawdown:+.4f} USDT"
                                            )
                                            closed_trades = await trade_manager.close_all_trades_global()
                                            if closed_trades:
                                                await send_telegram(
                                                    session,
                                                    build_global_sl_message(closed_trades, prev_bal)
                                                )
                                    continue   # Saltar check TP/SL individual (ya están cerrados)

                                # ── Check profit global (+21 USDT del ciclo) ─────────
                                if (
                                    trade_manager.open_trades
                                    and trade_manager.cycle_drawdown
                                        >= TradeManager.GLOBAL_TP_USDT
                                ):
                                    async with trade_manager._global_tp_lock:
                                        # Re-verificar dentro del lock para evitar doble disparo
                                        if (
                                            trade_manager.open_trades
                                            and trade_manager.cycle_drawdown
                                                >= TradeManager.GLOBAL_TP_USDT
                                        ):
                                            prev_bal = trade_manager.cycle_start_balance
                                            log.info(
                                                f"[GLOBAL TP] Equity: {trade_manager.equity:.2f} | "
                                                f"Ciclo inicio: {prev_bal:.2f} | "
                                                f"Profit: {trade_manager.cycle_drawdown:+.4f} USDT"
                                            )
                                            closed_trades = await trade_manager.close_all_trades_global_tp()
                                            if closed_trades:
                                                await send_telegram(
                                                    session,
                                                    build_global_tp_message(closed_trades, prev_bal)
                                                )
                                    continue   # Saltar check TP/SL individual (ya están cerrados)
                                # ── Check TP/SL individuales ────────────────────────
                                for trade, reason in trade_manager.trades_to_close(sym, price):
                                    closed = await trade_manager.close_trade(trade, price, reason)
                                    if closed:
                                        asyncio.create_task(
                                            notify_executor(session, {
                                                "action"     : "close",
                                                "trade_id"   : trade.id,
                                                "symbol"     : trade.symbol,
                                                "direction"  : trade.direction,
                                                "reason"     : trade.status,
                                                "close_price": trade.close_price,
                                            })
                                        )
                                        await send_telegram(
                                            session, build_close_message(trade)
                                        )

                        except (ValueError, KeyError, TypeError) as ex:
                            log.debug(f"WS miniTicker parse skip: {ex}")

                    elif msg.type in (
                        aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE
                    ):
                        log.warning("WS miniTicker: cerrado por Binance, reconectando...")
                        break

                    # Reconectar si los símbolos activos cambiaron
                    if frozenset(trade_manager.active_symbols) != last_symbols:
                        log.info("WS miniTicker: Símbolos cambiaron, reconectando...")
                        break

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"WS miniTicker error: {e}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30.0)


# ══════════════════════════════════════════════════════════════════════════════
#  BINANCE REST — Solo para obtener la lista de símbolos activos
# ══════════════════════════════════════════════════════════════════════════════

async def get_usdt_symbols(session: aiohttp.ClientSession) -> List[str]:
    """
    Obtiene todos los pares USDT Perpetual activos de Binance Futures.
    La lista se refresca cada SYMBOLS_REFRESH_H horas.
    """
    url       = f"{BINANCE_REST}/fapi/v1/exchangeInfo"
    max_tries = 5

    for attempt in range(1, max_tries + 1):
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=45)
            ) as resp:
                if resp.status in (429, 418):
                    retry_after = int(resp.headers.get("Retry-After", "10"))
                    log.warning(
                        f"get_usdt_symbols: rate limit HTTP {resp.status} "
                        f"— esperando {retry_after}s (intento {attempt}/{max_tries})"
                    )
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status != 200:
                    log.error(
                        f"get_usdt_symbols: HTTP {resp.status} "
                        f"(intento {attempt}/{max_tries})"
                    )
                    await asyncio.sleep(5 * attempt)
                    continue

                data = await resp.json(content_type=None)

                if not isinstance(data, dict) or "symbols" not in data:
                    await asyncio.sleep(5 * attempt)
                    continue

                symbols = [
                    s["symbol"]
                    for s in data["symbols"]
                    if s.get("quoteAsset")    == QUOTE_ASSET
                    and s.get("status")       == "TRADING"
                    and s.get("contractType") == "PERPETUAL"
                ]
                # 🔥 LIMITAR A 300 SÍMBOLOS
                symbols = symbols[:150]
                log.info(f"Símbolos USDT Perpetual activos en Binance Futures: {len(symbols)}")
                return symbols

        except asyncio.TimeoutError:
            log.warning(f"get_usdt_symbols: timeout (intento {attempt}/{max_tries})")
        except Exception as e:
            log.error(f"get_usdt_symbols: error (intento {attempt}/{max_tries}): {e}")

        await asyncio.sleep(5 * attempt)

    raise RuntimeError(
        f"get_usdt_symbols: No se pudo obtener la lista de símbolos "
        f"tras {max_tries} intentos."
    )


# ══════════════════════════════════════════════════════════════════════════════
#  ESCANEO EMA — Lee datos del caché (cero REST en operación normal)
# ══════════════════════════════════════════════════════════════════════════════

def _scan_all_sync(symbols: List[str]) -> list:
    """
    Función síncrona que itera todos los símbolos, lee sus buffers del caché
    y detecta señales EMA.

    Diseño:
      • Ejecuta en un thread pool (run_in_executor) para no bloquear el loop.
      • get_dataframe() es thread-safe (usa threading.Lock internamente).
      • detect_signal() es puro Python, sin I/O ni estado compartido.

    Retorna lista de (symbol, result_dict, price, change_1m).
    """
    if _cache is None:
        return []

    signals = []
    for symbol in symbols:
        try:
            df = _cache.get_dataframe(symbol, INTERVAL, only_closed=True)

            if len(df) < EMA_SLOW:
                continue   # Datos insuficientes para EMA_SLOW; saltar silenciosamente

            closes = df["close"].tolist()
            result = detect_signal(closes)

            if result and len(closes) >= 2:
                price     = closes[-1]
                change_1m = ((closes[-1] - closes[-2]) / closes[-2]) * 100
                signals.append((symbol, result, price, change_1m))

        except Exception as e:
            log.debug(f"Error escaneando {symbol}: {e}")

    return signals


async def run_scan(session: aiohttp.ClientSession, symbols: List[str]) -> None:
    """
    Ciclo de detección de señales EMA.

    Diferencias vs v1:
      • No hay semáforo de concurrencia REST — todos los datos están en RAM.
      • La carga CPU (calc_ema × N_símbolos) se delega a un thread pool
        para no bloquear el event loop principal.
      • No hay llamadas de red aquí — solo Telegram al detectar señal.
    """
    if not bot_status.get("cache_ready"):
        log.info("⏳ Caché no listo aún, omitiendo escaneo...")
        return

    loop    = asyncio.get_event_loop()
    signals = await loop.run_in_executor(None, _scan_all_sync, symbols)

    n_ready = (
        sum(1 for s in symbols if len(_cache.get_dataframe(s, INTERVAL, only_closed=True)) >= EMA_SLOW)
        if _cache else 0
    )
    log.info(
        f"Escaneo sobre {len(symbols)} símbolos — "
        f"{n_ready} con datos suficientes | "
        f"Señales: {len(signals)}"
    )

    for symbol, result, price, change_1m in signals:
        # Registrar señal en el dashboard
        bot_status["total_alerts"] += 1
        bot_status["last_alerts"].append({
            "time"   : datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "symbol" : symbol,
            "signal" : result["signal"],
            "price"  : f"{price:,.8f}",
            "spread" : result["spread"],
        })
        bot_status["last_alerts"] = bot_status["last_alerts"][-10:]

        # Intentar abrir paper trade
        trade = await trade_manager.open_trade(symbol, result["signal"], price)

        if trade:
            await notify_executor(session, {
                    "action": "open",
                    "trade_id": trade.id,
                    "symbol": trade.symbol,
                    "direction": trade.direction,
                    "price": trade.entry_price,
                })
            msg = build_open_message(trade, result, change_1m)
            log.info(
                f"  → {result['signal']} {symbol} | "
                f"spread {result['spread']:.2f}% | Trade #{trade.id}"
            )
            
            
        else:
            msg = build_signal_no_trade_message(symbol, result, price)
            log.info(
                f"  → {result['signal']} {symbol} | "
                f"spread {result['spread']:.2f}% | Sin trade (límite o balance)"
            )

        await send_telegram(session, msg)
        await asyncio.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD HTML + API
# ══════════════════════════════════════════════════════════════════════════════

DASHBOARD_JS = r"""
<script>
  const fmtPrice = (v) => Number(v || 0).toLocaleString('en-US', { minimumFractionDigits: 8, maximumFractionDigits: 8 });
  const fmt4 = (v) => Number(v || 0).toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 });
  const fmt2 = (v) => Number(v || 0).toFixed(2);

  function openRowsHTML(trades) {
    if (!trades || !trades.length)
      return '<tr><td colspan="11" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>';
    return trades.map(t => {
      const dirColor = t.direction === 'LONG' ? '#3fb950' : '#f85149';
      const pnlColor = Number(t.pnl_usdt) >= 0 ? '#3fb950' : '#f85149';
      const cur = Number(t.current_price || 0), tp = Number(t.tp_price || 0), sl = Number(t.sl_price || 0);
      const distTp = cur ? Math.abs(tp - cur) / cur * 100 : 0;
      const distSl = cur ? Math.abs(sl - cur) / cur * 100 : 0;
      return `<tr>
        <td>#${t.id}</td><td><b>${t.symbol}</b></td>
        <td style="color:${dirColor}">${t.direction === 'LONG' ? '🟢 LONG' : '🔴 SHORT'}</td>
        <td>$${fmtPrice(t.entry_price)}</td><td><b>$${fmtPrice(cur)}</b></td>
        <td style="color:#3fb950">$${fmtPrice(tp)} <small>(${distTp.toFixed(2)}%)</small></td>
        <td style="color:#f85149">$${fmtPrice(sl)} <small>(${distSl.toFixed(2)}%)</small></td>
        <td style="color:${pnlColor};font-weight:bold">${Number(t.pnl_usdt||0)>=0?'+':''}${fmt4(t.pnl_usdt)}</td>
        <td style="color:${pnlColor};font-weight:bold">${Number(t.roi_pct||0)>=0?'+':''}${fmt2(t.roi_pct)}%</td>
        <td>${fmt2(t.usdt_size)}</td>
        <td style="font-size:.72rem">${t.open_time||''}</td></tr>`;
    }).join('');
  }

  function closedRowsHTML(trades) {
    if (!trades || !trades.length)
      return '<tr><td colspan="10" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas aún</td></tr>';
    return trades.map(t => {
      const pnlColor = Number(t.pnl_usdt) >= 0 ? '#3fb950' : '#f85149';
      return `<tr style="color:${pnlColor}">
        <td>#${t.id}</td><td><b>${t.symbol}</b></td>
        <td>${t.direction==='LONG'?'🟢':'🔴'} ${t.direction}</td>
        <td>$${fmtPrice(t.entry_price)}</td><td>$${fmtPrice(t.close_price)}</td>
        <td><b>${Number(t.pnl_usdt||0)>=0?'+':''}${fmt4(t.pnl_usdt)}</b></td>
        <td><b>${Number(t.roi_pct||0)>=0?'+':''}${fmt2(t.roi_pct)}%</b></td>
        <td>${fmt2(t.usdt_size)}</td>
        <td>${t.status==='TP'?'✅ TP':'❌ SL'}</td>
        <td style="font-size:.72rem">${t.close_time||''}</td></tr>`;
    }).join('');
  }

  function alertRowsHTML(alerts) {
    if (!alerts || !alerts.length)
      return '<tr><td colspan="5" style="color:#8b949e;text-align:center;padding:.8rem">Sin señales aún...</td></tr>';
    return alerts.map(a => {
      const color = a.signal === 'LONG' ? '#3fb950' : '#f85149';
      return `<tr style="color:${color}">
        <td>${a.time||''}</td><td><b>${a.symbol||''}</b></td>
        <td>${a.signal==='LONG'?'🟢 LONG':'🔴 SHORT'}</td>
        <td>$${a.price||''}</td><td>${Number(a.spread||0).toFixed(2)}%</td></tr>`;
    }).join('');
  }

  async function refreshDashboard() {
    try {
      const res = await fetch('/api/state', { cache: 'no-store' });
      if (!res.ok) return;
      const d = await res.json();
      document.getElementById('balance_value').textContent  = fmt2(d.balance)  + ' USDT';
      document.getElementById('equity_value').textContent   = fmt2(d.equity)   + ' USDT';
      document.getElementById('rpnl_value').textContent     = (Number(d.realized_pnl)>=0?'+':'')+fmt4(d.realized_pnl)+' USDT';
      document.getElementById('upnl_value').textContent     = (Number(d.unrealized_pnl)>=0?'+':'')+fmt4(d.unrealized_pnl)+' USDT';
      document.getElementById('wr_value').textContent       = d.win_rate===null?'N/A':`${d.win_rate.toFixed(1)}% (${d.wins}✅/${d.losses}❌)`;
      // Ciclo de drawdown
      const dd = Number(d.cycle_drawdown || 0);
      const ddEl = document.getElementById('cycle_dd_value');
      if (ddEl) {
        ddEl.textContent = (dd>=0?'+':'')+fmt4(dd)+' USDT';
        ddEl.style.color = dd >= 0 ? '#3fb950' : (dd > -(d.global_sl_usdt||7)*0.5 ? '#d29922' : '#f85149');
      }
      if (document.getElementById('cycle_base_value'))
        document.getElementById('cycle_base_value').textContent = fmt2(d.cycle_start_balance) + ' USDT';
      document.getElementById('open_count_value').textContent= `${d.open_trades.length} / ${d.settings.max_longs+d.settings.max_shorts}`;
      document.getElementById('long_count_value').textContent = `${d.open_longs}L`;
      document.getElementById('short_count_value').textContent= `${d.open_shorts}S`;
      document.getElementById('alerts_value').textContent   = d.total_alerts;
      document.getElementById('last_scan_value').textContent = d.last_scan||'';
      document.getElementById('cache_stats_value').textContent =
        `WS: ${d.cache.active_ws}/${d.cache.total_ws} | Velas: ${d.cache.total_candles} | Msgs: ${d.cache.total_messages}`;
      document.getElementById('ws_symbols_value').textContent =
        'miniTicker activo en: '+(d.active_symbols.length?d.active_symbols.join(', '):'Ninguno');
      document.getElementById('open_trades_body').innerHTML  = openRowsHTML(d.open_trades);
      document.getElementById('alerts_body').innerHTML       = alertRowsHTML(d.alerts);
      document.getElementById('closed_trades_body').innerHTML= closedRowsHTML(d.closed_trades);
    } catch(e) { console.error('Dashboard refresh failed', e); }
  }

  refreshDashboard();
  setInterval(refreshDashboard, 1000);
</script>
"""


def get_dashboard_state() -> dict:
    tm = trade_manager

    def serialize_trade(t: Trade) -> dict:
        return {
            "id": t.id, "symbol": t.symbol, "direction": t.direction,
            "entry_price": t.entry_price, "quantity": t.quantity,
            "usdt_size": t.usdt_size, "open_time": t.open_time,
            "tp_price": t.tp_price, "sl_price": t.sl_price,
            "current_price": t.current_price, "status": t.status,
            "close_price": t.close_price, "close_time": t.close_time,
            "pnl_usdt": t.pnl_usdt, "roi_pct": t.roi_pct,
        }

    closed_all = tm.closed_trades
    wins       = sum(1 for t in closed_all if t.status == "TP")
    losses     = sum(1 for t in closed_all if t.status == "SL")
    total_cl   = len(closed_all)

    cache_stats = {}
    if _cache is not None:
        s = _cache.get_stats()
        cache_stats = {
            "active_ws"    : s["active_connections"],
            "total_ws"     : s["total_connections"],
            "total_candles": s["total_candles"],
            "total_messages": s["total_messages"],
            "gap_fills"    : s["total_gap_fills"],
            "clock_closes" : s["total_clock_closes"],
            "rl_tokens"    : s["rate_limiter_tokens"],
        }

    return {
        "balance"             : tm.balance,
        "equity"              : tm.equity,
        "cycle_start_balance" : tm.cycle_start_balance,
        "cycle_drawdown"      : tm.cycle_drawdown,
        "global_sl_usdt"      : TradeManager.GLOBAL_SL_USDT,
        "global_tp_usdt"      : TradeManager.GLOBAL_TP_USDT,
        "realized_pnl"        : tm.total_realized_pnl,
        "unrealized_pnl"      : tm.unrealized_pnl,
        "wins"                : wins,
        "losses"         : losses,
        "win_rate"       : (wins / total_cl * 100.0) if total_cl else None,
        "total_alerts"   : bot_status["total_alerts"],
        "last_scan"      : bot_status["last_scan"],
        "symbols_monitored": bot_status["symbols_monitored"],
        "cache_ready"    : bot_status["cache_ready"],
        "active_symbols" : sorted(tm.active_symbols),
        "open_longs"     : len(tm.open_longs),
        "open_shorts"    : len(tm.open_shorts),
        "open_trades"    : [serialize_trade(t) for t in sorted(tm.open_trades, key=lambda x: x.id)],
        "closed_trades"  : [serialize_trade(t) for t in list(reversed(closed_all))[:20]],
        "alerts"         : list(reversed(bot_status["last_alerts"])),
        "cache"          : cache_stats,
        "settings"       : {
            "ema_fast"  : EMA_FAST,   "ema_mid"  : EMA_MID,
            "ema_slow"  : EMA_SLOW,   "spread_pct": SPREAD_PCT,
            "tp_pct"    : TP_PCT,     "sl_pct"   : SL_PCT,
            "max_longs" : MAX_LONGS,  "max_shorts": MAX_SHORTS,
        },
    }


async def api_state_handler(request: web.Request) -> web.Response:
    return web.json_response(get_dashboard_state())


def build_dashboard() -> str:
    """Genera el HTML del dashboard (render inicial; JS lo refresca cada 1 s)."""
    tm         = trade_manager
    closed_all = tm.closed_trades
    wins       = sum(1 for t in closed_all if t.status == "TP")
    losses     = sum(1 for t in closed_all if t.status == "SL")
    total_cl   = len(closed_all)
    wr_str     = f"{wins/total_cl*100:.1f}%" if total_cl else "N/A"
    equity     = tm.equity
    rpnl       = tm.total_realized_pnl
    upnl       = tm.unrealized_pnl
    eq_color   = "#3fb950" if equity >= INITIAL_BALANCE else "#f85149"
    rpnl_color = "#3fb950" if rpnl >= 0 else "#f85149"
    upnl_color = "#3fb950" if upnl >= 0 else "#f85149"
    dd_color   = "#3fb950" if tm.cycle_drawdown >= 0 else ("#d29922" if tm.cycle_drawdown > -TradeManager.GLOBAL_SL_USDT * 0.5 else "#f85149")
    ws_syms    = ", ".join(sorted(tm.active_symbols)) if tm.active_symbols else "Ninguno"

    cache_line = "Iniciando..." if _cache is None else (
        lambda s: (
            f"WS: {s['active_connections']}/{s['total_connections']} activas | "
            f"Velas: {s['total_candles']} | Msgs WS: {s['total_messages']} | "
            f"Gap fills: {s['total_gap_fills']} | RL tokens: {s['rate_limiter_tokens']:.0f}"
        )
    )(_cache.get_stats())

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EMA Bot v2 — WebSocket-First | Paper Trading</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Courier New',monospace;background:#0d1117;color:#c9d1d9;padding:1.2rem}}
    h1{{color:#3fb950;margin-bottom:.8rem;font-size:1.3rem}}
    h2{{color:#58a6ff;margin:.9rem 0 .5rem;font-size:.95rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.6rem;margin-bottom:1.2rem}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:.75rem}}
    .card .label{{color:#8b949e;font-size:.7rem;margin-bottom:.25rem;text-transform:uppercase;letter-spacing:.04em}}
    .card .value{{color:#f0f6fc;font-size:1rem;font-weight:bold}}
    .ok{{color:#3fb950}} .warn{{color:#d29922}} .err{{color:#f85149}}
    .wrap{{overflow-x:auto;margin-bottom:1.2rem}}
    table{{width:100%;border-collapse:collapse;font-size:.78rem;min-width:700px}}
    th{{color:#8b949e;text-align:left;padding:.35rem .45rem;border-bottom:1px solid #30363d;white-space:nowrap;font-size:.72rem}}
    td{{padding:.3rem .45rem;border-bottom:1px solid #1c2128;white-space:nowrap}}
    tr:hover td{{background:#161b22}}
    .ws-dot{{display:inline-block;width:8px;height:8px;background:#3fb950;border-radius:50%;margin-right:5px;animation:blink 1.5s infinite}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
  </style>
</head>
<body>
  <h1>🤖 EMA Strategy Bot v2 — WebSocket-First | Paper Trading</h1>

  <div class="grid">
    <div class="card"><div class="label">Balance libre</div>
      <div class="value ok" id="balance_value">{tm.balance:.2f} USDT</div></div>
    <div class="card"><div class="label">Equity total</div>
      <div class="value" style="color:{eq_color}" id="equity_value">{equity:.2f} USDT</div></div>
    <div class="card"><div class="label">PnL realizado</div>
      <div class="value" style="color:{rpnl_color}" id="rpnl_value">{rpnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">PnL no realizado</div>
      <div class="value" style="color:{upnl_color}" id="upnl_value">{upnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">Win Rate</div>
      <div class="value warn" id="wr_value">{wr_str} ({wins}✅/{losses}❌)</div></div>
    <div class="card"><div class="label">Posiciones</div>
      <div class="value" id="open_count_value">{len(tm.open_trades)} / {MAX_LONGS+MAX_SHORTS}</div></div>
    <div class="card"><div class="label">LONG / SHORT</div>
      <div class="value">
        <span class="ok" id="long_count_value">{len(tm.open_longs)}L</span> /
        <span class="err" id="short_count_value">{len(tm.open_shorts)}S</span>
      </div></div>
    <div class="card"><div class="label">Señales EMA</div>
      <div class="value warn" id="alerts_value">{bot_status['total_alerts']}</div></div>
    <div class="card"><div class="label">Último escaneo</div>
      <div class="value" style="font-size:.75rem" id="last_scan_value">{bot_status['last_scan']}</div></div>
    <div class="card"><div class="label">TP / SL</div>
      <div class="value"><span class="ok">+{TP_PCT}%</span> / <span class="err">-{SL_PCT}%</span></div></div>
    <div class="card"><div class="label">Por operación</div>
      <div class="value warn">{USDT_PER_TRADE:.2f} USDT</div></div>
    <div class="card"><div class="label">Balance inicial</div>
      <div class="value">{INITIAL_BALANCE:.2f} USDT</div></div>
    <div class="card"><div class="label">🔄 Base de ciclo</div>
      <div class="value warn" id="cycle_base_value">{tm.cycle_start_balance:.2f} USDT</div></div>
    <div class="card"><div class="label">📉 Drawdown ciclo</div>
      <div class="value" id="cycle_dd_value" style="color:{dd_color}">{tm.cycle_drawdown:+.4f} USDT</div></div>
    <div class="card"><div class="label">🚨 Límite global SL</div>
      <div class="value err">-{TradeManager.GLOBAL_SL_USDT:.2f} USDT</div></div>
    <div class="card"><div class="label">🏆 Objetivo global TP</div>
      <div class="value ok">+{TradeManager.GLOBAL_TP_USDT:.2f} USDT</div></div>
  </div>

  <!-- KlineWebSocketCache stats -->
  <h2>📡 KlineWebSocketCache v4 — Estado del Caché</h2>
  <p id="cache_stats_value" style="color:#8b949e;font-size:.78rem;margin-bottom:.6rem">{cache_line}</p>

  <!-- Posiciones abiertas -->
  <h2><span class="ws-dot"></span>📊 Posiciones Abiertas — precios miniTicker (tiempo real)</h2>
  <p id="ws_symbols_value" style="color:#484f58;font-size:.72rem;margin-bottom:.4rem">
    miniTicker activo en: {ws_syms}
  </p>
  <div class="wrap">
    <table>
      <thead><tr>
        <th>#</th><th>Par</th><th>Dir</th>
        <th>Entrada</th><th>Precio actual</th>
        <th>Take Profit</th><th>Stop Loss</th>
        <th>PnL (USDT)</th><th>ROI%</th>
        <th>Tamaño</th><th>Abierto</th>
      </tr></thead>
      <tbody id="open_trades_body">
        <tr><td colspan="11" style="color:#8b949e;text-align:center;padding:.8rem">Cargando...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- Señales EMA -->
  <h2>📡 Señales EMA detectadas (últimas 10)</h2>
  <div class="wrap">
    <table>
      <thead><tr><th>Hora UTC</th><th>Par</th><th>Señal</th><th>Precio</th><th>Spread</th></tr></thead>
      <tbody id="alerts_body">
        <tr><td colspan="5" style="color:#8b949e;text-align:center;padding:.8rem">Cargando...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- Operaciones cerradas -->
  <h2>📋 Operaciones Cerradas (últimas 20)</h2>
  <div class="wrap">
    <table>
      <thead><tr>
        <th>#</th><th>Par</th><th>Dir</th>
        <th>Entrada</th><th>Salida</th>
        <th>PnL (USDT)</th><th>ROI%</th>
        <th>Tamaño</th><th>Resultado</th><th>Cerrado</th>
      </tr></thead>
      <tbody id="closed_trades_body">
        <tr><td colspan="10" style="color:#8b949e;text-align:center;padding:.8rem">Cargando...</td></tr>
      </tbody>
    </table>
  </div>

  <p style="color:#484f58;margin-top:.6rem;font-size:.7rem">
    Estrategia: EMA{EMA_FAST}/{EMA_MID}/{EMA_SLOW} | Spread ≥{SPREAD_PCT}% |
    Timeframe: {INTERVAL} | KlineWebSocketCache v4 (Zero REST) |
    Binance USDT Futures Perpetual | RR 1:4 |
    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
  </p>

  {DASHBOARD_JS}
</body>
</html>"""


async def health_handler(request: web.Request) -> web.Response:
    return web.Response(text=build_dashboard(), content_type="text/html")


async def start_http_server() -> None:
    app = web.Application()
    app.router.add_get("/",          health_handler)
    app.router.add_get("/health",    health_handler)
    app.router.add_get("/api/state", api_state_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Dashboard activo en http://0.0.0.0:{PORT}")
    #app.router.add_get("/api/trades/download", download_trades_handler)
    
# ── HANDLER DE DESCARGA CSV ──────────────────────────────────────────
async def download_trades_handler(request: web.Request) -> web.Response:
    """Descarga TODAS las operaciones (abiertas + cerradas) como CSV."""
    import csv, io
    tm = trade_manager
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id","symbol","direction","signal_mode","original_signal",
        "entry_price","close_price","quantity","usdt_size",
        "tp_price","sl_price","effective_tp_pct","effective_sl_pct",
        "status","pnl_usdt","roi_pct","open_time","close_time"
    ])
    for t in sorted(tm.trades, key=lambda x: x.id):
        writer.writerow([
            t.id, t.symbol, t.direction, t.signal_mode, t.original_signal,
            f"{t.entry_price:.8f}", f"{t.close_price:.8f}",
            f"{t.quantity:.6f}", f"{t.usdt_size:.2f}",
            f"{t.tp_price:.8f}", f"{t.sl_price:.8f}",
            t.effective_tp_pct, t.effective_sl_pct,
            t.status, f"{t.pnl_usdt:.4f}", f"{t.roi_pct:.2f}",
            t.open_time, t.close_time,
        ])
    csv_bytes = output.getvalue().encode("utf-8")
    filename  = f"trades_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    return web.Response(
        body=csv_bytes,
        content_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ══════════════════════════════════════════════════════════════════════════════
#  BOT LOOP PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

async def bot_loop() -> None:
    """
    Ciclo de vida principal del bot.

    Secuencia de arranque
    ─────────────────────
    1. Obtener lista de símbolos USDT Perpetual vía REST.
    2. Iniciar KlineWebSocketCache con todos los símbolos.
    3. Esperar a que el backfill inicial termine (≥90 % de pares con datos).
    4. Iniciar ws_price_loop (miniTicker para TP/SL).
    5. Entrar en el bucle de escaneo EMA cada SCAN_INTERVAL segundos.
    6. Refrescar lista de símbolos cada SYMBOLS_REFRESH_H horas;
       reiniciar el caché si el conjunto cambia.
    """
    global _cache

    async with aiohttp.ClientSession() as session:

        # ── 1. Mensaje de inicio Telegram ─────────────────────────────────────
        await send_telegram(
            session,
            f"🤖 <b>EMA Strategy Bot v2 — WebSocket-First</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 EMAs: <b>{EMA_FAST} / {EMA_MID} / {EMA_SLOW}</b>\n"
            f"📐 Spread mínimo: <b>{SPREAD_PCT}%</b>\n"
            f"🗄 Fuente de datos: <b>KlineWebSocketCache v4 (WebSocket)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Balance inicial: <b>{INITIAL_BALANCE:.2f} USDT</b>\n"
            f"📦 Por operación: <b>{USDT_PER_TRADE:.2f} USDT</b>\n"
            f"🎯 TP: <b>+{TP_PCT}%</b> | 🛑 SL: <b>-{SL_PCT}%</b>  <i>(RR 1:4)</i>\n"
            f"📊 Máx posiciones: <b>{MAX_LONGS} LONG + {MAX_SHORTS} SHORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 LONG:  EMA{EMA_FAST} y EMA{EMA_MID} bajo EMA{EMA_SLOW} "
            f"+ EMA{EMA_FAST} cruza ↑ EMA{EMA_MID}\n"
            f"🔴 SHORT: EMA{EMA_FAST} y EMA{EMA_MID} sobre EMA{EMA_SLOW} "
            f"+ EMA{EMA_FAST} cruza ↓ EMA{EMA_MID}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔍 Monitoreando todos los pares USDT Perpetual de Binance Futures"
        )

        # ── 2. Obtener lista inicial de símbolos ──────────────────────────────
        symbols: List[str] = []
        while not symbols:
            try:
                symbols = await get_usdt_symbols(session)
                bot_status["symbols_monitored"] = len(symbols)
            except RuntimeError as e:
                log.critical(str(e))
                log.info("Reintentando en 30 s...")
                await asyncio.sleep(30)

        last_symbols_set   = set(symbols)
        last_symbols_fetch = time.time()

        # ── 3. Iniciar KlineWebSocketCache ────────────────────────────────────
        log.info(f"🚀 Iniciando KlineWebSocketCache para {len(symbols)} símbolos...")
        init_cache(symbols)

        # ── 4. Esperar backfill inicial ───────────────────────────────────────
        await wait_cache_ready(expected=len(symbols), timeout=420.0)

        # ── 5. Iniciar WebSocket miniTicker (TP/SL en tiempo real) ────────────
        asyncio.create_task(ws_price_loop(session))

        # ── 6. Bucle principal de escaneo EMA ─────────────────────────────────
        while True:
            # ── Refrescar lista de símbolos cada SYMBOLS_REFRESH_H horas ─────
            elapsed_h = (time.time() - last_symbols_fetch) / 3600
            if elapsed_h >= SYMBOLS_REFRESH_H:
                try:
                    new_symbols = await get_usdt_symbols(session)
                    new_set     = set(new_symbols)
                    added       = new_set - last_symbols_set
                    removed     = last_symbols_set - new_set

                    if added or removed:
                        log.info(
                            f"Lista de símbolos cambió: "
                            f"+{len(added)} nuevos, -{len(removed)} eliminados. "
                            f"Reiniciando caché..."
                        )
                        symbols           = new_symbols
                        last_symbols_set  = new_set
                        bot_status["symbols_monitored"] = len(symbols)
                        bot_status["cache_ready"]       = False
                        init_cache(symbols)
                        await wait_cache_ready(expected=len(symbols), timeout=420.0)
                    else:
                        log.info("Lista de símbolos sin cambios.")
                        symbols = new_symbols

                    last_symbols_fetch = time.time()

                except RuntimeError as e:
                    log.error(f"No se pudo refrescar símbolos: {e} — usando lista anterior.")

            # ── Ciclo de escaneo EMA ──────────────────────────────────────────
            t0 = asyncio.get_event_loop().time()
            try:
                await run_scan(session, symbols)
                bot_status["last_scan"] = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                )
            except Exception as e:
                log.error(f"Error en escaneo: {e}", exc_info=True)

            elapsed = asyncio.get_event_loop().time() - t0
            wait    = max(0.0, SCAN_INTERVAL - elapsed)

            cache_info = ""
            if _cache is not None:
                s = _cache.get_stats()
                cache_info = (
                    f"WS: {s['active_connections']}/{s['total_connections']} | "
                    f"Velas: {s['total_candles']} | "
                    f"Msgs: {s['total_messages']}"
                )

            log.info(
                f"Escaneo en {elapsed:.1f}s | "
                f"L:{len(trade_manager.open_longs)} S:{len(trade_manager.open_shorts)} | "
                f"Balance: {trade_manager.balance:.2f} USDT | "
                f"Equity: {trade_manager.equity:.2f} USDT | "
                f"Ciclo base: {trade_manager.cycle_start_balance:.2f} | "
                f"Drawdown ciclo: {trade_manager.cycle_drawdown:+.2f} USDT | "
                f"{cache_info} | "
                f"Próximo en {wait:.1f}s"
            )
            await asyncio.sleep(wait)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║   EMA Strategy Bot v2 — WebSocket-First + Paper Trading      ║")
    log.info(f"║   EMAs: {EMA_FAST}/{EMA_MID}/{EMA_SLOW}  "
             f"TP:{TP_PCT}%  SL:{SL_PCT}% (RR 1:4)  "
             f"Balance:{INITIAL_BALANCE:.0f} USDT  ║")
    log.info(f"║   Datos: KlineWebSocketCache v4 — Zero REST en operación    ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    await asyncio.gather(
        start_http_server(),
        bot_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
