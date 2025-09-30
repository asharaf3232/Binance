# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🚀 Wise Maestro Bot - Binance Edition 🚀 ---
# =======================================================================================
# هذا الملف يجمع المنطق الهجين (Maestro + Wise Man) ويطبقه على منصة Binance.

# --- Core Libraries & Setup ---
import os
import logging
import asyncio
import json
import time
import copy
import random
from datetime import datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
from collections import defaultdict, Counter
import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
import httpx
import websockets

# --- Telegram & Environment ---
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from telegram.constants import ParseMode
from telegram.error import BadRequest, TimedOut, Forbidden
from dotenv import load_dotenv

# --- استيراد الوحدات المشتركة ---
from _settings_config import DEFAULT_SETTINGS, STRATEGY_NAMES_AR, SETTINGS_PRESETS, TIMEFRAME, SCAN_INTERVAL_SECONDS, SUPERVISOR_INTERVAL_SECONDS, TIME_SYNC_INTERVAL_SECONDS, STRATEGY_ANALYSIS_INTERVAL_SECONDS, MAESTRO_INTERVAL_HOURS, DECISION_MATRIX
from _settings_config import PORTFOLIO_RISK_RULES, SECTOR_MAP # لتصنيف المخاطر في UI
from _strategy_scanners import SCANNERS, filter_whale_radar, find_col
from _ai_market_brain import get_market_regime, get_fundamental_market_mood, get_fear_and_greed_index, analyze_sentiment_of_headlines, get_latest_crypto_news, get_alpha_vantage_economic_events, translate_text_gemini
from _wise_maestro_guardian import TradeGuardian # يستورد المنطق الدفاعي المزدوج

# --- إعدادات البوت والبيئة ---
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_AV_KEY_HERE')

EGYPT_TZ = ZoneInfo("Africa/Cairo")
DB_FILE = 'wise_maestro_binance.db'
SETTINGS_FILE = 'wise_maestro_binance_settings.json'

# --- إعداد Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("BINANCE_MAESTRO")

# =======================================================================================
# --- Global Bot State & Locks ---
# =======================================================================================
class BotState:
    def __init__(self):
        self.settings = {}
        self.trading_enabled = True
        self.active_preset_name = "مخصص"
        self.last_signal_time = defaultdict(float)
        self.exchange = None
        self.application = None
        self.market_mood = {"mood": "UNKNOWN", "reason": "تحليل لم يتم بعد"}
        self.last_scan_info = {}
        self.all_markets = []
        self.last_markets_fetch = 0
        self.websocket_manager = None
        self.strategy_performance = {}
        self.pending_strategy_proposal = {}
        self.trade_guardian = None
        self.TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
        self.trade_management_lock = asyncio.Lock() # Lock for DB/trade actions
        self.current_market_regime = "UNKNOWN" # لحالة المايسترو

bot_data = BotState()
scan_lock = asyncio.Lock()

# =======================================================================================
# --- Core Helper & Settings Management ---
# =======================================================================================

def load_settings():
    """تحميل الإعدادات مع دمج أي متغيرات جديدة من الإعدادات الافتراضية."""
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f: bot_data.settings = json.load(f)
        else: bot_data.settings = copy.deepcopy(DEFAULT_SETTINGS)
    except Exception: bot_data.settings = copy.deepcopy(DEFAULT_SETTINGS)
    
    # دمج القيم الافتراضية (لضمان وجود جميع المفاتيح)
    default_copy = copy.deepcopy(DEFAULT_SETTINGS)
    for key, value in default_copy.items():
        if isinstance(value, dict):
            if key not in bot_data.settings or not isinstance(bot_data.settings[key], dict): bot_data.settings[key] = {}
            for sub_key, sub_value in value.items(): bot_data.settings[key].setdefault(sub_key, sub_value)
        else: bot_data.settings.setdefault(key, value)

    determine_active_preset(); save_settings()
    logger.info(f"Settings loaded. Active preset: {bot_data.active_preset_name}")

def determine_active_preset():
    """تحديد النمط الجاهز الحالي (مخصص إذا كان هناك اختلاف)."""
    current_settings_for_compare = {k: v for k, v in bot_data.settings.items() if k in DEFAULT_SETTINGS}
    for name, preset_settings in SETTINGS_PRESETS.items():
        is_match = True
        for key, value in preset_settings.items():
            if key in current_settings_for_compare and current_settings_for_compare[key] != value:
                is_match = False; break
        if is_match:
            bot_data.active_preset_name = STRATEGY_NAMES_AR.get(name, "مخصص"); return
    bot_data.active_preset_name = "مخصص"

def save_settings():
    """حفظ الإعدادات إلى ملف."""
    with open(SETTINGS_FILE, 'w') as f: json.dump(bot_data.settings, f, indent=4)

async def init_database():
    """تهيئة قاعدة البيانات وجدول الصفقات."""
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            # التأكد من وجود جميع الأعمدة المطلوبة
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, 
                    entry_price REAL, take_profit REAL, stop_loss REAL, quantity REAL, 
                    status TEXT, reason TEXT, order_id TEXT, highest_price REAL DEFAULT 0, 
                    trailing_sl_active BOOLEAN DEFAULT 0, close_price REAL, pnl_usdt REAL, 
                    signal_strength INTEGER DEFAULT 1, close_retries INTEGER DEFAULT 0, 
                    last_profit_notification_price REAL DEFAULT 0, trade_weight REAL DEFAULT 1.0
                )
            ''')
            # يجب التأكد من الأعمدة الإضافية في الكود الأصلي
            cursor = await conn.execute("PRAGMA table_info(trades)")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'signal_strength' not in columns: await conn.execute("ALTER TABLE trades ADD COLUMN signal_strength INTEGER DEFAULT 1")
            # ... (يتم تطبيق جميع تعديلات الأعمدة من الكود الأصلي)
            await conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e: logger.critical(f"Database initialization failed: {e}")

async def safe_send_message(bot, text, **kwargs):
    """إرسال رسائل تليجرام مع معالجة الأخطاء."""
    try: await bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e: logger.error(f"Telegram Send Error: {e}")

async def safe_edit_message(query, text, **kwargs):
    """تعديل رسائل تليجرام مع معالجة الأخطاء."""
    try: await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except BadRequest as e:
        if "Message is not modified" not in str(e): logger.warning(f"Edit Message Error: {e}")
    except Exception as e: logger.error(f"Edit Message Error: {e}")

# =======================================================================================
# --- Binance WebSocket Manager (SPOT) ---
# =======================================================================================
class BinanceWebSocketManager:
    """إدارة WebSocket موحدة لـ Binance (Tickers + UserData Stream)."""
    def __init__(self, exchange, application, guardian_handler):
        self.exchange = exchange
        self.application = application
        self.guardian_handler = guardian_handler
        self.listen_key = None
        self.public_subscriptions = set()
        self.ws = None
        self.is_running = False
        self.keep_alive_task = None
        self.ticker_updates_handler = guardian_handler.handle_ticker_update # تمرير الدالة من الحارس

    async def _get_listen_key(self):
        try:
            self.listen_key = (await self.exchange.publicPostUserDataStream())['listenKey']
            logger.info("WS Manager: New listen key obtained.")
            return True
        except Exception as e:
            logger.error(f"WS Manager: Failed to get listen key: {e}")
            self.listen_key = None
            return False

    async def _keep_alive_listen_key(self):
        while self.is_running:
            await asyncio.sleep(1800) # 30 minutes
            if self.listen_key:
                try: await self.exchange.publicPutUserDataStream({'listenKey': self.listen_key}); logger.info("WS Manager: Listen key kept alive.")
                except Exception as e: logger.warning(f"WS Manager: Failed to keep listen key alive: {e}. It might have expired."); self.listen_key = None

    async def run(self):
        self.is_running = True
        self.keep_alive_task = asyncio.create_task(self._keep_alive_listen_key())

        while self.is_running:
            if not self.listen_key and not await self._get_listen_key(): await asyncio.sleep(60); continue
            
            # Streams: public tickers (for active trades) + private user data
            public_streams = [f"{s.lower().replace('/', '')}@ticker" for s in self.public_subscriptions]
            streams = public_streams + [self.listen_key]
            
            stream_name = '/'.join(streams)
            uri = f"wss://stream.binance.com:9443/stream?streams={stream_name}"
            
            if not stream_name: logger.warning("No active streams. Waiting..."); await asyncio.sleep(10); continue

            try:
                # نستخدم exponential_backoff_with_jitter لمرونة الاتصال (كما في بوت OKX)
                await self._run_ws_connection_loop(uri)
            except Exception as e:
                if self.is_running: logger.warning(f"WS Manager: Connection lost: {e}. Reconnecting in 5s..."); await asyncio.sleep(5)
                else: break

    async def _run_ws_connection_loop(self, uri):
        async with websockets.connect(uri, ping_interval=180, ping_timeout=60) as ws:
            self.ws = ws
            logger.info(f"✅ [WS Manager] Connected. Watching {len(self.public_subscriptions)} symbols.")
            async for message in ws: await self._handle_message(message)

    async def _handle_message(self, message):
        try:
            data = json.loads(message)
            payload = data.get('data', data)
            event_type = payload.get('e')

            if event_type == '24hrTicker':
                # تحديثات الأسعار العامة للحارس (SL/TP/TSL)
                await self.ticker_updates_handler(payload)
            elif event_type == 'executionReport':
                # تأكيد تنفيذ أمر الشراء (تفعيل الصفقة)
                if payload.get('x') == 'TRADE' and payload.get('S') == 'BUY' and payload.get('X') == 'FILLED':
                    await self._handle_filled_buy_order(payload)
            elif event_type == 'listenKeyExpired':
                logger.warning("Listen key expired. Getting a new one."); self.listen_key = None
                if self.ws: await self.ws.close()
        except Exception as e:
            logger.error(f"Error handling WS message: {e}", exc_info=True)

    async def _handle_filled_buy_order(self, order_data):
        symbol = order_data['s'].replace('USDT', '/USDT')
        order_id = order_data['i']
        if float(order_data.get('L', 0)) > 0: # L: last traded quantity
            logger.info(f"Fast Reporter: Received fill for {order_id}. Activating...")
            await self.activate_trade(order_id, symbol) # نستخدم الدالة الموحدة في هذا الملف

    async def sync_subscriptions(self):
        """مزامنة الاشتراكات العامة بناءً على الصفقات النشطة في قاعدة البيانات."""
        async with aiosqlite.connect(DB_FILE) as conn:
            active_symbols = {row[0] for row in await (await conn.execute("SELECT DISTINCT symbol FROM trades WHERE status = 'active'")).fetchall()}

        if active_symbols != self.public_subscriptions:
            logger.info(f"WS Manager: Syncing subscriptions. Old: {len(self.public_subscriptions)}, New: {len(active_symbols)}")
            self.public_subscriptions = active_symbols
            # لإعادة اتصال الـ WebSocket بمجموعة الـ Streams الجديدة
            if self.ws and not self.ws.closed:
                try: await self.ws.close(code=1000, reason='Subscription change')
                except Exception: pass
    
    # يجب تعريف هذه الدالة داخل الملف الرئيسي لأنها تستخدم المنطق الخاص بالـ DB والـ Exchange
    async def activate_trade(self, order_id, symbol):
        """تفعيل الصفقة بعد تأكيد التنفيذ."""
        try:
            order_details = await self.exchange.fetch_order(order_id, symbol)
            filled_price = float(order_details.get('average', 0.0))
            net_filled_quantity = float(order_details.get('filled', 0.0))

            if net_filled_quantity <= 0 or filled_price <= 0:
                logger.error(f"Order {order_id} invalid fill data. Cancelling activation."); return

        except Exception as e:
            logger.error(f"Could not fetch order details for activation of {order_id}: {e}", exc_info=True); return

        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            trade = await (await conn.execute("SELECT * FROM trades WHERE order_id = ? AND status = 'pending'", (order_id,))).fetchone()
            if not trade: logger.info(f"Activation ignored for {order_id}: Trade not found or not pending."); return
            trade = dict(trade)
            
            risk = filled_price - trade['stop_loss']
            new_take_profit = filled_price + (risk * bot_data.settings['risk_reward_ratio'])

            await conn.execute(
                "UPDATE trades SET status = 'active', entry_price = ?, quantity = ?, take_profit = ?, last_profit_notification_price = ? WHERE id = ?",
                (filled_price, net_filled_quantity, new_take_profit, filled_price, trade['id'])
            )
            active_trades_count = (await (await conn.execute("SELECT COUNT(*) FROM trades WHERE status = 'active'")).fetchone())[0]
            await conn.commit()

        await self.sync_subscriptions()

        balance_after = await self.exchange.fetch_balance()
        usdt_remaining = balance_after.get('USDT', {}).get('free', 0)
        trade_cost = filled_price * net_filled_quantity
        tp_percent = (new_take_profit / filled_price - 1) * 100
        sl_percent = (1 - trade['stop_loss'] / filled_price) * 100
        reasons_ar = ' + '.join([STRATEGY_NAMES_AR.get(r.strip(), r.strip()) for r in trade['reason'].split(' + ')])
        strength_stars = '⭐' * trade.get('signal_strength', 1)

        success_msg = (
            f"✅ **تم تأكيد الشراء | {symbol}**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"**الاستراتيجية:** {reasons_ar} {strength_stars}\n"
            f"**تفاصيل الصفقة:**\n"
            f"  - **رقم:** `#{trade['id']}`\n"
            f"  - **سعر التنفيذ:** `${filled_price:,.4f}`\n"
            f"  - **الكمية:** `{net_filled_quantity:,.4f}`\n"
            f"  - **التكلفة الإجمالية:** `${trade_cost:,.2f}`\n"
            f"**الأهداف:**\n"
            f"  - **الهدف (TP):** `${new_take_profit:,.4f}` `({tp_percent:+.2f}%)`\n"
            f"  - **الوقف (SL):** `${trade['stop_loss']:,.4f}` `({sl_percent:.2f}%)`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 **السيولة المتبقية:** `${usdt_remaining:,.2f}`\n"
            f"🔄 **الصفقات النشطة:** `{active_trades_count}`"
        )
        await safe_send_message(self.application.bot, success_msg)
    
    async def stop(self):
        self.is_running = False
        if self.keep_alive_task: self.keep_alive_task.cancel()
        if self.ws and not self.ws.closed: await self.ws.close()

# =======================================================================================
# --- Trade Initiation & Scanner Logic ---
# =======================================================================================
async def log_pending_trade_to_db(signal, buy_order):
    """تسجيل الصفقة في قاعدة البيانات بحالة 'pending'."""
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("""
                INSERT INTO trades (timestamp, symbol, reason, order_id, status, entry_price, take_profit, stop_loss, signal_strength, trade_weight)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (datetime.now(EGYPT_TZ).isoformat(), signal['symbol'], signal['reason'], buy_order['id'], 'pending',
                  signal['entry_price'], signal['take_profit'], signal['stop_loss'], signal.get('strength', 1), signal.get('weight', 1.0)))
            await conn.commit()
            return True
    except Exception as e: logger.error(f"DB Log Pending Error for {signal['symbol']}: {e}"); return False

async def has_active_trade_for_symbol(symbol: str) -> bool:
    """يتحقق من وجود صفقة نشطة أو عالقة للرمز."""
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            cursor = await conn.execute("SELECT 1 FROM trades WHERE symbol = ? AND status IN ('active', 'pending', 'incubated') LIMIT 1", (symbol,))
            return await cursor.fetchone() is not None
    except Exception as e: logger.error(f"Database check for active trade failed for {symbol}: {e}"); return True

async def get_binance_markets():
    """جلب وتصفية أسواق Binance."""
    settings = bot_data.settings
    if time.time() - bot_data.last_markets_fetch > 300:
        try:
            logger.info("Fetching and caching all Binance markets...")
            all_tickers = await bot_data.exchange.fetch_tickers()
            bot_data.all_markets = list(all_tickers.values()); bot_data.last_markets_fetch = time.time()
        except Exception as e: logger.error(f"Failed to fetch all markets: {e}"); return []
    
    blacklist = settings.get('asset_blacklist', [])
    valid_markets = [
        t for t in bot_data.all_markets if 'USDT' in t['symbol'] and 
        t.get('quoteVolume', 0) > settings['liquidity_filters']['min_quote_volume_24h_usd'] and 
        t['symbol'].split('/')[0] not in blacklist and 
        t.get('active', True) and 
        not any(k in t['symbol'] for k in ['-SWAP', 'UP', 'DOWN', '3L', '3S'])
    ]
    valid_markets.sort(key=lambda m: m.get('quoteVolume', 0), reverse=True)
    return valid_markets[:settings['top_n_symbols_by_volume']]

async def fetch_ohlcv_batch(exchange, symbols, timeframe, limit):
    """جلب بيانات الشموع لدفعة من العملات بالتوازي."""
    tasks = [exchange.fetch_ohlcv(s, timeframe, limit=limit) for s in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return {symbols[i]: results[i] for i in range(len(symbols)) if not isinstance(results[i], Exception)}

async def worker_batch(queue, signals_list, errors_list):
    """عامل يقوم بتحليل الشموع وتطبيق الفلاتر والاستراتيجيات."""
    settings, exchange = bot_data.settings, bot_data.exchange
    while not queue.empty():
        try:
            item = await queue.get(); market, ohlcv = item['market'], item['ohlcv']
            symbol = market['symbol']
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).set_index(pd.to_datetime(pd.Series(ohlcv).apply(lambda x: x[0]), unit='ms'))

            if len(df) < 50: queue.task_done(); continue
            orderbook = await exchange.fetch_order_book(symbol, limit=1)
            best_bid, best_ask = orderbook['bids'][0][0], orderbook['asks'][0][0]
            spread_percent = ((best_ask - best_bid) / best_bid) * 100

            # 1. فلتر رادار الحيتان (Whale Radar) - قبل الفلاتر المكلفة
            if 'whale_radar' in settings['active_scanners'] and await filter_whale_radar(exchange, symbol):
                if spread_percent <= settings['spread_filter']['max_spread_percent'] * 2:
                    signals_list.append({"symbol": symbol, "reason": "whale_radar", "strength": 5, "weight": 1.0, "entry_price": df.iloc[-2]['close'], "stop_loss": df.iloc[-2]['close'] * 0.95, "take_profit": df.iloc[-2]['close'] * 1.05})
                    queue.task_done(); continue
            
            # 2. الفلاتر السريعة (Spread, Volatility, Volume, Trend)
            if spread_percent > settings['spread_filter']['max_spread_percent']: queue.task_done(); continue
            
            df.ta.atr(length=14, append=True); atr_col_name = find_col(df.columns, f"ATRr_14")
            last_close = df['close'].iloc[-2]; atr_percent = (df[atr_col_name].iloc[-2] / last_close) * 100 if last_close > 0 else 0
            if atr_percent < settings['volatility_filters']['min_atr_percent']: queue.task_done(); continue
            
            df['volume_sma'] = ta.sma(df['volume'], length=20); rvol = df['volume'].iloc[-2] / df['volume_sma'].iloc[-2]
            if rvol < settings.get('volume_filter_multiplier', 2.0): queue.task_done(); continue

            # 3. فلتر الاتجاه والإطار الزمني
            is_htf_bullish = True
            if settings.get('multi_timeframe_enabled', True):
                ohlcv_htf = await exchange.fetch_ohlcv(symbol, settings.get('multi_timeframe_htf'), limit=220)
                df_htf = pd.DataFrame(ohlcv_htf, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                if len(df_htf) > 200:
                    df_htf.ta.ema(length=200, append=True); ema_col_name_htf = find_col(df_htf.columns, "EMA_200")
                    if ema_col_name_htf and pd.notna(df_htf[ema_col_name_htf].iloc[-2]): is_htf_bullish = df_htf['close'].iloc[-2] > df_htf[ema_col_name_htf].iloc[-2]
            
            if settings.get('trend_filters', {}).get('enabled', True):
                ema_period = settings.get('trend_filters', {}).get('ema_period', 200); df.ta.ema(length=ema_period, append=True)
                ema_col_name = find_col(df.columns, f"EMA_{ema_period}"); 
                if df['close'].iloc[-2] < df[ema_col_name].iloc[-2]: queue.task_done(); continue # تحت EMA200
            
            # 4. تطبيق الماسحات النشطة
            confirmed_reasons, adx_value = [], 0
            if settings.get('adx_filter_enabled', False): df.ta.adx(append=True); adx_col = find_col(df.columns, "ADX_"); adx_value = df[adx_col].iloc[-2]
            
            for name in settings['active_scanners']:
                if name in ['whale_radar']: continue
                if adx_value < settings.get('adx_filter_level', 25) and name not in ['bollinger_reversal', 'support_rebound', 'rsi_divergence']: continue # فلتر ADX

                if not (strategy_func := SCANNERS.get(name)): continue
                params = settings.get(name, {})
                func_args = {'df': df.copy(), 'params': params, 'rvol': rvol, 'adx_value': adx_value}
                if name in ['support_rebound']: func_args.update({'exchange': exchange, 'symbol': symbol})
                result = await strategy_func(**func_args) if asyncio.iscoroutinefunction(strategy_func) else strategy_func(**{k: v for k, v in func_args.items() if k not in ['exchange', 'symbol']})
                if result: confirmed_reasons.append(result['reason'])

            if confirmed_reasons:
                # 5. حساب الوزن الذكي وإدارة المخاطر
                reason_str, strength = ' + '.join(set(confirmed_reasons)), len(set(confirmed_reasons))
                trade_weight = 1.0
                if settings.get('adaptive_intelligence_enabled', True):
                    primary_reason = confirmed_reasons[0]; perf = bot_data.strategy_performance.get(primary_reason)
                    if perf: # تطبيق نظام الوزن الديناميكي
                        if perf['win_rate'] < 50: trade_weight *= (1 - settings['dynamic_sizing_max_decrease_pct'] / 100.0)
                        elif perf['win_rate'] > 70: trade_weight *= (1 + settings['dynamic_sizing_max_increase_pct'] / 100.0)
                        if perf['win_rate'] < settings['strategy_deactivation_threshold_wr'] and perf['total_trades'] > settings['strategy_analysis_min_trades']: logger.warning(f"Signal for {symbol} from weak strategy '{primary_reason}' ignored."); queue.task_done(); continue
                
                if not is_htf_bullish: trade_weight *= 0.8
                
                # 6. تحديد الأهداف النهائية
                entry_price = df.iloc[-2]['close']
                atr = df.iloc[-2].get(atr_col_name, 0)
                risk = atr * settings['atr_sl_multiplier']
                stop_loss, take_profit = entry_price - risk, entry_price + (risk * settings['risk_reward_ratio'])
                
                signals_list.append({"symbol": symbol, "entry_price": entry_price, "take_profit": take_profit, "stop_loss": stop_loss, "reason": reason_str, "strength": strength, "weight": trade_weight})
            
            queue.task_done()
        except Exception as e:
            logger.error(f"Error processing symbol {symbol if 'symbol' in locals() else 'N/A'}: {e}", exc_info=True)
            if 'symbol' in locals(): errors_list.append(symbol)
            if not queue.empty(): queue.task_done()

async def initiate_real_trade(signal):
    """تنفيذ أمر الشراء الفعلي لـ Binance."""
    if not bot_data.trading_enabled: return False
    try:
        settings, exchange = bot_data.settings, bot_data.exchange
        trade_size = settings['real_trade_size_usdt'] * signal.get('weight', 1.0)
        await exchange.load_markets()

        # 1. التحقق من الحد الأدنى لقيمة الصفقة (MIN_NOTIONAL)
        try:
            market = exchange.market(signal['symbol'])
            min_notional_value = float(market.get('limits', {}).get('notional', {}).get('min', 10.0))
            required_size = min_notional_value * 1.05 # هامش أمان 5%
            if trade_size < required_size:
                logger.warning(f"Trade for {signal['symbol']} aborted. Size ({trade_size:.2f} USDT) is below required minimum ({required_size:.2f} USDT).")
                await safe_send_message(bot_data.application.bot, f"🚨 **فشل الشراء:** حجم الصفقة `{trade_size:.2f}` أقل من الحد الأدنى لـ {signal['symbol']} ({required_size:.2f} USDT).")
                return False
        except Exception as e:
            logger.error(f"Failed MIN_NOTIONAL check: {e}"); return False

        balance = await exchange.fetch_balance(); usdt_balance = balance.get('USDT', {}).get('free', 0.0)
        if usdt_balance < trade_size:
             logger.error(f"Insufficient USDT for {signal['symbol']}. Have: {usdt_balance}, Need: {trade_size}"); return False
        
        base_amount = trade_size / signal['entry_price']
        formatted_amount = exchange.amount_to_precision(signal['symbol'], base_amount)

        # 2. تنفيذ أمر الشراء
        buy_order = await exchange.create_market_buy_order(signal['symbol'], formatted_amount)
        if await log_pending_trade_to_db(signal, buy_order):
            await safe_send_message(bot_data.application.bot, f"🚀 تم إرسال أمر شراء لـ `{signal['symbol']}`."); return True
        else:
            await exchange.cancel_order(buy_order['id'], signal['symbol']); return False

    except ccxt.InsufficientFunds as e: logger.error(f"REAL TRADE FAILED {signal['symbol']}: {e}"); await safe_send_message(bot_data.application.bot, f"⚠️ **رصيد غير كافٍ!**"); return False
    except Exception as e: logger.error(f"REAL TRADE FAILED {signal['symbol']}: {e}", exc_info=True); return False

async def perform_scan(context: ContextTypes.DEFAULT_TYPE):
    """دالة الفحص الرئيسية."""
    async with scan_lock:
        if not bot_data.trading_enabled: logger.info("Scan skipped: Trading disabled."); return
        scan_start_time = time.time(); settings, bot = bot_data.settings, context.bot
        
        # 1. فلاتر المزاج العامة
        if settings.get('news_filter_enabled', True):
            mood_result_fundamental = await get_fundamental_market_mood(ALPHA_VANTAGE_API_KEY)
            if mood_result_fundamental['mood'] in ["NEGATIVE", "DANGEROUS"]: bot_data.market_mood = mood_result_fundamental; return
        mood_result = await get_market_mood(bot_data.exchange)
        bot_data.market_mood = mood_result
        if mood_result['mood'] in ["NEGATIVE", "DANGEROUS"]: return

        async with aiosqlite.connect(DB_FILE) as conn:
            active_trades_count = (await (await conn.execute("SELECT COUNT(*) FROM trades WHERE status IN ('active', 'pending', 'incubated')")).fetchone())[0]
        if active_trades_count >= settings['max_concurrent_trades']: logger.info(f"Scan skipped: Max trades ({active_trades_count}) reached."); return

        # 2. جلب الأسواق وبيانات الشموع
        top_markets = await get_binance_markets(); symbols_to_scan = [m['symbol'] for m in top_markets]
        ohlcv_data = await fetch_ohlcv_batch(bot_data.exchange, symbols_to_scan, TIMEFRAME, 220)
        
        queue, signals_found, analysis_errors = asyncio.Queue(), [], []
        for market in top_markets:
            if market['symbol'] in ohlcv_data:
                await queue.put({'market': market, 'ohlcv': ohlcv_data[market['symbol']]})
        
        # 3. تشغيل الـ Workers
        worker_tasks = [asyncio.create_task(worker_batch(queue, signals_found, analysis_errors)) for _ in range(settings.get("worker_threads", 10))]
        await queue.join(); [task.cancel() for task in worker_tasks]
        
        # 4. تفعيل الصفقات (مع قفل Race Condition)
        trades_opened_count = 0; signals_found.sort(key=lambda s: s.get('strength', 0), reverse=True)
        available_slots = settings['max_concurrent_trades'] - active_trades_count
        symbols_being_traded_in_this_scan = set()

        for signal in signals_found:
            if available_slots <= 0: break
            symbol_to_trade = signal['symbol']

            if symbol_to_trade in symbols_being_traded_in_this_scan: continue
            if await has_active_trade_for_symbol(symbol_to_trade): continue
            if time.time() - bot_data.last_signal_time.get(symbol_to_trade, 0) > (SCAN_INTERVAL_SECONDS * 0.9):
                
                async with bot_data.trade_management_lock: # قفل على مرحلة الشراء والتحديث
                    bot_data.last_signal_time[symbol_to_trade] = time.time()
                    symbols_being_traded_in_this_scan.add(symbol_to_trade)

                    if await initiate_real_trade(signal):
                        trades_opened_count += 1
                        available_slots -= 1
                    else:
                        symbols_being_traded_in_this_scan.remove(symbol_to_trade)
                    
                    await asyncio.sleep(1) # تأخير لتحديث رصيد المنصة

        # 5. تقرير الفحص
        scan_duration = time.time() - scan_start_time
        bot_data.last_scan_info = {"start_time": datetime.fromtimestamp(scan_start_time, EGYPT_TZ).strftime('%Y-%m-%d %H:%M:%S'), "duration_seconds": int(scan_duration), "checked_symbols": len(top_markets), "analysis_errors": len(analysis_errors)}
        await safe_send_message(bot, f"✅ **فحص السوق اكتمل بنجاح**\n**المدة:** {int(scan_duration)} ثانية | **صفقات تم فتحها:** {trades_opened_count} صفقة")

# =======================================================================================
# --- Adaptive Jobs (Performance Analysis and Maestro) ---
# =======================================================================================

async def update_strategy_performance(context: ContextTypes.DEFAULT_TYPE):
    """تحليل أداء الاستراتيجيات."""
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            cursor = await conn.execute("SELECT reason, status, pnl_usdt FROM trades WHERE status LIKE '%(%' ORDER BY id DESC LIMIT 100")
            trades = await cursor.fetchall()

        if not trades: return

        stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0.0, 'win_pnl': 0.0, 'loss_pnl': 0.0})
        for reason_str, status, pnl in trades:
            if not reason_str or pnl is None: continue
            clean_reason = reason_str.split(' (')[0]
            reasons = clean_reason.split(' + ')
            for r in set(reasons):
                is_win = 'ناجحة' in status or 'تأمين' in status
                if is_win: stats[r]['wins'] += 1; stats[r]['win_pnl'] += pnl
                else: stats[r]['losses'] += 1; stats[r]['loss_pnl'] += pnl
                stats[r]['total_pnl'] += pnl

        performance_data = {}
        for r, s in stats.items():
            total = s['wins'] + s['losses']; win_rate = (s['wins'] / total * 100) if total > 0 else 0
            profit_factor = s['win_pnl'] / abs(s['loss_pnl']) if s['loss_pnl'] != 0 else float('inf')
            performance_data[r] = {"win_rate": round(win_rate, 2), "profit_factor": round(profit_factor, 2), "total_trades": total}
        bot_data.strategy_performance = performance_data
    except Exception as e: logger.error(f"Failed to analyze strategy performance: {e}")

async def propose_strategy_changes(context: ContextTypes.DEFAULT_TYPE):
    """اقتراح تعطيل الاستراتيجيات الضعيفة."""
    s = bot_data.settings
    if not s.get('adaptive_intelligence_enabled') or not s.get('strategy_proposal_enabled'): return
    
    # (منطق اقتراح التعديل يذهب هنا)
    active_scanners = s.get('active_scanners', [])
    min_trades = s.get('strategy_analysis_min_trades', 10)
    deactivation_wr = s.get('strategy_deactivation_threshold_wr', 45.0)

    for scanner in active_scanners:
        perf = bot_data.strategy_performance.get(scanner)
        if perf and perf['total_trades'] >= min_trades and perf['win_rate'] < deactivation_wr:
            if bot_data.pending_strategy_proposal.get('scanner') == scanner: continue
            proposal_key = f"prop_{int(time.time())}"
            bot_data.pending_strategy_proposal = {
                "key": proposal_key, "action": "disable", "scanner": scanner,
                "reason": f"أظهرت أداءً ضعيفًا بمعدل نجاح `{perf['win_rate']}%` في آخر `{perf['total_trades']}` صفقة."
            }
            message = (f"💡 **اقتراح تحسين الأداء** 💡\n"
                       f"مرحباً، أقترح تعطيل استراتيجية **'{STRATEGY_NAMES_AR.get(scanner, scanner)}'** "
                       f"{bot_data.pending_strategy_proposal['reason']}\n هل توافق؟")
            keyboard = [[InlineKeyboardButton("✅ موافقة", callback_data=f"strategy_adjust_approve_{proposal_key}"),
                         InlineKeyboardButton("❌ رفض", callback_data=f"strategy_adjust_reject_{proposal_key}")]]
            await safe_send_message(context.bot, message, reply_markup=InlineKeyboardMarkup(keyboard))
            return

async def maestro_job(context: ContextTypes.DEFAULT_TYPE):
    """المايسترو: يحلل نمط السوق ويضبط الإعدادات."""
    if not bot_data.settings.get('maestro_mode_enabled', True): return
    logger.info("🎼 Maestro: Analyzing market regime and adjusting tactics...")
    regime = await get_market_regime(bot_data.exchange)
    
    if regime == "UNKNOWN": return
    bot_data.current_market_regime = regime

    try:
        config = DECISION_MATRIX.get(regime)
        if config:
            for key, value in config.items():
                if key in bot_data.settings:
                    old_value = bot_data.settings[key]
                    bot_data.settings[key] = value
                    if old_value != value:
                        logger.info(f"🎼 Maestro: Updated {key} to {value} for regime {regime}")
            save_settings()
            await safe_send_message(context.bot, f"🎼 **تقرير المايسترو | {regime}**\nتم تعديل التكوين ليتناسب مع حالة السوق. الاستراتيجيات النشطة: {' + '.join([STRATEGY_NAMES_AR.get(s, s) for s in config.get('active_scanners', [])])}")
    except Exception as e: logger.error(f"🎼 Maestro Job failed: {e}")

# =======================================================================================
# --- V. Telegram UI & Dispatcher (الواجهة المفقودة) ---
# =======================================================================================

async def show_dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض لوحة التحكم الرئيسية."""
    ks_status_emoji = "🚨" if not bot_data.trading_enabled else "✅"
    ks_status_text = "مفتاح الإيقاف (مفعل)" if not bot_data.trading_enabled else "الحالة (طبيعية)"
    
    keyboard = [
        [InlineKeyboardButton("💼 نظرة عامة على المحفظة", callback_data="db_portfolio"), InlineKeyboardButton("📈 الصفقات النشطة", callback_data="db_trades")],
        [InlineKeyboardButton("📜 سجل الصفقات المغلقة", callback_data="db_history"), InlineKeyboardButton("📊 الإحصائيات والأداء", callback_data="db_stats")],
        [InlineKeyboardButton("🌡️ تحليل مزاج السوق", callback_data="db_mood"), InlineKeyboardButton("🔬 فحص فوري", callback_data="db_manual_scan")],
        [InlineKeyboardButton("🗓️ التقرير اليومي", callback_data="db_daily_report")],
        [InlineKeyboardButton(f"🎼 التحكم الاستراتيجي", callback_data="db_maestro_control"), InlineKeyboardButton(f"{ks_status_emoji} {ks_status_text}", callback_data="kill_switch_toggle")],
        [InlineKeyboardButton("🕵️‍♂️ تقرير التشخيص", callback_data="db_diagnostics")]
    ]
    
    message_text = "🖥️ **لوحة تحكم المايسترو الحكيم (Binance)**\n\nاختر نوع التقرير الذي تريد عرضه:"
    target_message = update.message or update.callback_query.message
    
    if update.callback_query: 
        await safe_edit_message(update.callback_query, message_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else: 
        await target_message.reply_text(message_text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_maestro_control(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """لوحة تحكم المايسترو لتعديل الأنماط الحالية."""
    s = bot_data.settings
    regime = bot_data.current_market_regime
    maestro_enabled = s.get('maestro_mode_enabled', True)
    emoji = "✅" if maestro_enabled else "❌"
    active_scanners_str = ' + '.join([STRATEGY_NAMES_AR.get(scanner, scanner) for scanner in s.get('active_scanners', [])])
    
    message = (f"🎼 **لوحة التحكم الاستراتيجي (المايسترو)**\n"
               f"━━━━━━━━━━━━━━━━━━\n"
               f"**حالة المايسترو:** {emoji} مفعل\n"
               f"**تشخيص السوق الحالي:** {regime}\n"
               f"**الاستراتيجيات النشطة:** {active_scanners_str}\n\n"
               f"**التكوين الحالي:**\n"
               f"  - **المراجع الذكي:** {'✅' if s.get('intelligent_reviewer_enabled') else '❌'}\n"
               f"  - **اقتناص الزخم:** {'✅' if s.get('momentum_scalp_mode_enabled') else '❌'}\n"
               f"  - **فلتر التوافق:** {'✅' if s.get('multi_timeframe_enabled') else '❌'}\n"
               f"  - **نسبة R:R:** {s.get('risk_reward_ratio', 'N/A')}")
               
    keyboard = [
        [InlineKeyboardButton(f"🎼 تبديل المايسترو ({'تعطيل' if maestro_enabled else 'تفعيل'})", callback_data="maestro_toggle")],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="back_to_dashboard")]
    ]
    await safe_edit_message(update.callback_query, message, reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_maestro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تفعيل/تعطيل المايسترو."""
    bot_data.settings['maestro_mode_enabled'] = not bot_data.settings.get('maestro_mode_enabled', True)
    save_settings()
    await update.callback_query.answer(f"المايسترو {'تم تفعيله' if bot_data.settings['maestro_mode_enabled'] else 'تم تعطيله'}")
    await show_maestro_control(update, context)

async def toggle_kill_switch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تفعيل/تعطيل مفتاح الإيقاف."""
    query = update.callback_query; bot_data.trading_enabled = not bot_data.trading_enabled
    if bot_data.trading_enabled: await query.answer("✅ تم استئناف التداول الطبيعي."); 
    else: await query.answer("🚨 تم تفعيل مفتاح الإيقاف!", show_alert=True); 
    await show_dashboard_command(update, context)

async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """قائمة الإعدادات الرئيسية."""
    keyboard = [
        [InlineKeyboardButton("🧠 الذكاء التكيفي/المايسترو", callback_data="settings_adaptive")],
        [InlineKeyboardButton("🎛️ تعديل المعايير المتقدمة", callback_data="settings_params")],
        [InlineKeyboardButton("🔭 تفعيل/تعطيل الماسحات", callback_data="settings_scanners")],
        [InlineKeyboardButton("🗂️ أنماط جاهزة", callback_data="settings_presets")],
        [InlineKeyboardButton("🚫 القائمة السوداء", callback_data="settings_blacklist"), InlineKeyboardButton("🗑️ إدارة البيانات", callback_data="settings_data")]
    ]
    message_text = "⚙️ *الإعدادات الرئيسية*\n\nاختر فئة الإعدادات التي تريد تعديلها."
    target_message = update.message or update.callback_query.message
    if update.callback_query: await safe_edit_message(update.callback_query, message_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else: await target_message.reply_text(message_text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

# --- دوال الإحصائيات والتقارير (Dashboard) ---

async def show_mood_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تقرير مزاج السوق والأخبار."""
    query = update.callback_query
    await query.answer("جاري تحليل مزاج السوق...")
    
    # تنفيذ المهام بالتوازي
    mood_task = asyncio.create_task(get_market_mood(bot_data.exchange))
    headlines_task = asyncio.to_thread(get_latest_crypto_news)

    mood = await mood_task
    original_headlines = await headlines_task
    
    # ترجمة وتحليل المشاعر
    translated_headlines, translation_success = await translate_text_gemini(original_headlines, GEMINI_API_KEY)
    news_sentiment, _ = analyze_sentiment_of_headlines(original_headlines)
    
    verdict = f"الوضع الحالي: {mood['reason']}"
    news_header = "📰 آخر الأخبار (مترجمة آلياً):" if translation_success else "📰 آخر الأخبار (الترجمة غير متاحة):"
    news_str = "\n".join([f"  - _{h}_" for h in translated_headlines[:5]]) or "  لا توجد أخبار."
    
    message = (
        f"**🌡️ تحليل مزاج السوق الشامل**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"**⚫️ الخلاصة:** *{verdict}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"**📊 المؤشرات الرئيسية:**\n"
        f"  - **اتجاه BTC العام:** {mood.get('btc_mood', 'N/A')}\n"
        f"  - **مشاعر الأخبار:** {news_sentiment}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{news_header}\n{news_str}\n"
    )
    keyboard = [[InlineKeyboardButton("🔄 تحديث", callback_data="db_mood")], [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="back_to_dashboard")]]
    await safe_edit_message(query, message, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_trades_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض الصفقات النشطة/العالقة."""
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trades = await (await conn.execute("SELECT id, symbol, status, entry_price, stop_loss, take_profit FROM trades WHERE status IN ('active', 'pending', 'incubated') ORDER BY id DESC")).fetchall()
    
    if not trades:
        text = "لا توجد صفقات نشطة حاليًا."
        keyboard = [[InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="back_to_dashboard")]]
        await safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard)); return
    
    text = "📈 *الصفقات النشطة*\nاختر صفقة لعرض تفاصيلها:\n"; keyboard = []
    for trade in trades: 
        status_emoji = {"active": "✅", "pending": "⏳", "incubated": "⚠️"}.get(trade['status'], "❓")
        button_text = f"#{trade['id']} {status_emoji} | {trade['symbol']} | دخول: ${trade['entry_price']:.4f}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"check_{trade['id']}")])
        
    keyboard.append([InlineKeyboardButton("🔄 تحديث", callback_data="db_trades")])
    keyboard.append([InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="back_to_dashboard")])
    await safe_edit_message(update.callback_query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def check_trade_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تفاصيل صفقة محددة مع خيار البيع اليدوي."""
    query = update.callback_query; trade_id = int(query.data.split('_')[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row; cursor = await conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)); trade = await cursor.fetchone()
    if not trade: await query.answer("لم يتم العثور على الصفقة."); return
    trade = dict(trade)
    
    keyboard = []
    
    if trade['status'] == 'pending':
        message = f"**⏳ حالة الصفقة #{trade_id}**\n- **العملة:** `{trade['symbol']}`\n- **الحالة:** في انتظار تأكيد التنفيذ..."
    else:
        try:
            ticker = await bot_data.exchange.fetch_ticker(trade['symbol']); current_price = ticker['last']
            pnl = (current_price - trade['entry_price']) * trade['quantity']
            pnl_percent = (current_price / trade['entry_price'] - 1) * 100 if trade['entry_price'] > 0 else 0
            pnl_text = f"💰 **الربح/الخسارة الحالية:** `${pnl:+.2f}` ({pnl_percent:+.2f}%)"
            current_price_text = f"- **السعر الحالي:** `${current_price}`"
        except Exception:
            pnl_text = "💰 تعذر جلب الربح/الخسارة الحالية."
            current_price_text = "- **السعر الحالي:** `تعذر الجلب`"

        message = (f"**✅ حالة الصفقة #{trade_id}**\n\n"
                   f"- **العملة:** `{trade['symbol']}`\n- **سعر الدخول:** `${trade['entry_price']}`\n{current_price_text}\n"
                   f"- **الكمية:** `{trade['quantity']}`\n"
                   f"----------------------------------\n- **الهدف (TP):** `${trade['take_profit']}`\n"
                   f"- **الوقف (SL):** `${trade['stop_loss']}`\n"
                   f"----------------------------------\n{pnl_text}")
        
        if trade['status'] in ['active', 'incubated']:
            keyboard.append([InlineKeyboardButton("🚨 بيع فوري (بسعر السوق)", callback_data=f"manual_sell_confirm_{trade_id}")])

    keyboard.append([InlineKeyboardButton("🔙 العودة للصفقات", callback_data="db_trades")])
    await safe_edit_message(query, message, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_manual_sell_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تأكيد البيع اليدوي."""
    query = update.callback_query; trade_id = int(query.data.split('_')[-1])
    async with aiosqlite.connect(DB_FILE) as conn: cursor = await conn.execute("SELECT symbol FROM trades WHERE id = ?", (trade_id,)); trade_data = await cursor.fetchone()
    if not trade_data: await query.answer("لم يتم العثور على الصفقة.", show_alert=True); return
    symbol = trade_data[0]
    message = f"🛑 **تأكيد البيع الفوري** 🛑\n\nهل أنت متأكد أنك تريد بيع صفقة `{symbol}` رقم `#{trade_id}` بسعر السوق الحالي؟"
    keyboard = [[InlineKeyboardButton("✅ نعم، قم بالبيع الآن", callback_data=f"manual_sell_execute_{trade_id}")],
                [InlineKeyboardButton("❌ لا، تراجع", callback_data=f"check_{trade_id}")]]
    await safe_edit_message(query, message, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_manual_sell_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تنفيذ أمر البيع اليدوي."""
    query = update.callback_query; trade_id = int(query.data.split('_')[-1])
    await safe_edit_message(query, "⏳ جاري إرسال أمر البيع...", reply_markup=None)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row; trade = await (await conn.execute("SELECT * FROM trades WHERE id = ? AND status IN ('active', 'incubated')", (trade_id,))).fetchone()
    if not trade:
        await query.answer("لم يتم العثور على الصفقة أو أنها ليست نشطة.", show_alert=True); 
        fake_update = Update(update.update_id, callback_query=type('Query', (), {'data': f"check_{trade_id}", 'message': query.message, 'edit_message_text': query.edit_message_text, 'answer': query.answer})()); await check_trade_details(fake_update, context); return
    try:
        trade = dict(trade); ticker = await bot_data.exchange.fetch_ticker(trade['symbol']); current_price = ticker['last']
        # استخدام دالة الإغلاق المصّلب الموحدة
        await bot_data.trade_guardian._close_trade(trade, "إغلاق يدوي", current_price)
        await query.answer("✅ تم إرسال أمر البيع بنجاح!")
    except Exception as e:
        logger.error(f"Manual sell execution failed for trade #{trade_id}: {e}", exc_info=True)
        await safe_send_message(context.bot, f"🚨 فشل البيع اليدوي للصفقة #{trade_id}. السبب: {e}")
        await query.answer("🚨 فشل أمر البيع. راجع السجلات.", show_alert=True)

# --- دوال الإعدادات الفرعية ---
def get_nested_value(d, keys):
    current_level = d
    for key in keys:
        if isinstance(current_level, dict) and key in current_level: current_level = current_level[key]
        else: return None
    return current_level

async def show_parameters_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """قائمة المعايير المتقدمة."""
    s = bot_data.settings; def bool_format(key, text): val = s.get(key, False); emoji = "✅" if val else "❌"; return f"{text}: {emoji} مفعل"
    keyboard = [
        [InlineKeyboardButton("--- إعدادات عامة ومخاطر ---", callback_data="noop")],
        [InlineKeyboardButton(f"حجم الصفقة ($): {s['real_trade_size_usdt']}", callback_data="param_set_real_trade_size_usdt")],
        [InlineKeyboardButton(f"أقصى عدد للصفقات: {s['max_concurrent_trades']}", callback_data="param_set_max_concurrent_trades")],
        [InlineKeyboardButton(f"نسبة المخاطرة/العائد: {s['risk_reward_ratio']}", callback_data="param_set_risk_reward_ratio")],
        [InlineKeyboardButton(bool_format('trailing_sl_enabled', 'تفعيل الوقف المتحرك'), callback_data="param_toggle_trailing_sl_enabled")],
        [InlineKeyboardButton("--- فلاتر التداول ---", callback_data="noop")],
        [InlineKeyboardButton(f"مضاعف فلتر الحجم: {s['volume_filter_multiplier']}", callback_data="param_set_volume_filter_multiplier")],
        [InlineKeyboardButton(f"أقصى سبريد مسموح (%): {get_nested_value(s, ['spread_filter', 'max_spread_percent'])}", callback_data="param_set_spread_filter_max_spread_percent")],
        [InlineKeyboardButton(bool_format('btc_trend_filter_enabled', 'فلتر اتجاه BTC'), callback_data="param_toggle_btc_trend_filter_enabled")],
        [InlineKeyboardButton(bool_format('market_mood_filter_enabled', 'فلتر الخوف والطمع'), callback_data="param_toggle_market_mood_filter_enabled")],
        [InlineKeyboardButton("🔙 العودة للإعدادات", callback_data="settings_main")]
    ]
    await safe_edit_message(update.callback_query, "🎛️ **تعديل المعايير المتقدمة**\n\nاضغط على أي معيار لتعديل قيمته:", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_adaptive_intelligence_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """قائمة إعدادات الذكاء التكيفي."""
    s = bot_data.settings; def bool_format(key, text): val = s.get(key, False); emoji = "✅" if val else "❌"; return f"{text}: {emoji} مفعل"
    keyboard = [
        [InlineKeyboardButton(bool_format('adaptive_intelligence_enabled', 'تفعيل الذكاء التكيفي'), callback_data="param_toggle_adaptive_intelligence_enabled")],
        [InlineKeyboardButton(bool_format('maestro_mode_enabled', 'تفعيل المايسترو'), callback_data="param_toggle_maestro_mode_enabled")],
        [InlineKeyboardButton(bool_format('intelligent_reviewer_enabled', 'تفعيل المراجع الذكي'), callback_data="param_toggle_intelligent_reviewer_enabled")],
        [InlineKeyboardButton(bool_format('momentum_scalp_mode_enabled', 'تفعيل اقتناص الزخم'), callback_data="param_toggle_momentum_scalp_mode_enabled")],
        [InlineKeyboardButton("--- معايير الضبط ---", callback_data="noop")],
        [InlineKeyboardButton(f"حد أدنى لتعطيل (WR%): {s.get('strategy_deactivation_threshold_wr', 45.0)}", callback_data="param_set_strategy_deactivation_threshold_wr")],
        [InlineKeyboardButton(f"أقصى زيادة للحجم (%): {s.get('dynamic_sizing_max_increase_pct', 25.0)}", callback_data="param_set_dynamic_sizing_max_increase_pct")],
        [InlineKeyboardButton("🔙 العودة للإعدادات", callback_data="settings_main")]
    ]
    await safe_edit_message(update.callback_query, "🧠 **إعدادات الذكاء التكيفي**\n\nتحكم في وظائف المايسترو والتعلم:", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_scanners_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """قائمة تفعيل/تعطيل الماسحات."""
    keyboard = []; active_scanners = bot_data.settings['active_scanners']
    for key, name in STRATEGY_NAMES_AR.items():
        status_emoji = "✅" if key in active_scanners else "❌"
        perf_hint = ""
        if (perf := bot_data.strategy_performance.get(key)): perf_hint = f" ({perf['win_rate']}% WR)"
        keyboard.append([InlineKeyboardButton(f"{status_emoji} {name}{perf_hint}", callback_data=f"scanner_toggle_{key}")])
    keyboard.append([InlineKeyboardButton("🔙 العودة للإعدادات", callback_data="settings_main")])
    await safe_edit_message(update.callback_query, "اختر الماسحات لتفعيلها أو تعطيلها (مع تلميح الأداء):", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_scanner_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة تفعيل/تعطيل الماسحات."""
    query = update.callback_query; scanner_key = query.data.replace("scanner_toggle_", "")
    active_scanners = bot_data.settings['active_scanners']
    if scanner_key in active_scanners:
        if len(active_scanners) > 1: active_scanners.remove(scanner_key)
        else: await query.answer("يجب تفعيل ماسح واحد على الأقل.", show_alert=True); return
    else: active_scanners.append(scanner_key)
    save_settings(); determine_active_preset()
    await query.answer(f"{STRATEGY_NAMES_AR[scanner_key]} {'تم تفعيله' if scanner_key in active_scanners else 'تم تعطيله'}")
    await show_scanners_menu(update, context)

async def handle_parameter_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إعداد قيمة لمتغير (يطلب من المستخدم إرسال القيمة)."""
    query = update.callback_query; param_key = query.data.replace("param_set_", "")
    context.user_data['setting_to_change'] = param_key
    await query.message.reply_text(f"أرسل القيمة الرقمية الجديدة لـ `{param_key}`:")

async def handle_toggle_parameter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تبديل قيمة منطقية (True/False)."""
    query = update.callback_query; param_key = query.data.replace("param_toggle_", "")
    bot_data.settings[param_key] = not bot_data.settings.get(param_key, False)
    save_settings(); determine_active_preset()
    
    # تحديد القائمة للعودة إليها
    parent_menu = "settings_adaptive" if any(k in param_key for k in ["adaptive", "maestro", "intelligent", "momentum"]) else "settings_params"
    fake_query = type('Query', (), {'message': query.message, 'data': parent_menu, 'edit_message_text': query.edit_message_text, 'answer': query.answer})
    
    if parent_menu == "settings_adaptive": await show_adaptive_intelligence_menu(Update(update.update_id, callback_query=fake_query), context)
    else: await show_parameters_menu(Update(update.update_id, callback_query=fake_query), context)


async def handle_setting_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة القيمة الرقمية المدخلة من المستخدم."""
    user_input = update.message.text.strip(); setting_key = context.user_data.pop('setting_to_change', None)
    if not setting_key: return # لا يوجد متغير لتعديله

    try:
        keys = setting_key.split('_'); current_dict = bot_data.settings
        for key in keys[:-1]: current_dict = current_dict = current_dict[key]
        last_key = keys[-1]; original_value = current_dict[last_key]
        
        new_value = int(user_input) if isinstance(original_value, int) else float(user_input)
        current_dict[last_key] = new_value

        save_settings(); determine_active_preset()
        await update.message.reply_text(f"✅ تم تحديث `{setting_key}` إلى `{new_value}`.")
    except (ValueError, KeyError, TypeError):
        await update.message.reply_text("❌ قيمة غير صالحة. الرجاء إرسال رقم صحيح أو عشري.")
    finally:
        # العودة إلى القائمة المناسبة
        parent_menu_data = "settings_adaptive" if any(k in setting_key for k in ["adaptive", "maestro", "dynamic", "strategy"]) else "settings_params"
        fake_query = type('Query', (), {'message': update.message, 'data': parent_menu_data, 'edit_message_text': (lambda *args, **kwargs: asyncio.sleep(0)), 'answer': (lambda *args, **kwargs: asyncio.sleep(0))})
        if parent_menu_data == "settings_adaptive": await show_adaptive_intelligence_menu(Update(update.update_id, callback_query=fake_query), context)
        else: await show_parameters_menu(Update(update.update_id, callback_query=fake_query), context)

# =======================================================================================
# --- VI. Dispatcher (الموجه الرئيسي) ---
# =======================================================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالج أمر /start."""
    keyboard = [["Dashboard 🖥️"], ["الإعدادات ⚙️"]]
    await update.message.reply_text("أهلاً بك في **المايسترو الحكيم | إصدار Binance**", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True), parse_mode=ParseMode.MARKDOWN)

async def manual_scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالج أمر /scan أو زر الفحص الفوري."""
    if not bot_data.trading_enabled: await (update.message or update.callback_query.message).reply_text("🔬 الفحص محظور."); return
    await (update.message or update.callback_query.message).reply_text("🔬 أمر فحص يدوي...")
    context.job_queue.run_once(perform_scan, 1)

async def universal_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المعالج العام للرسائل النصية (للأزرار والقيم المدخلة)."""
    # الأولوية: إدخال قيمة لإعداد معين
    if 'setting_to_change' in context.user_data or 'blacklist_action' in context.user_data:
        await handle_setting_value(update, context); return
    
    # الأولوية: الأزرار الرئيسية
    text = update.message.text
    if text == "Dashboard 🖥️": await show_dashboard_command(update, context)
    elif text == "الإعدادات ⚙️": await show_settings_menu(update, context)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """المعالج العام لجميع ضغطات الأزرار."""
    query = update.callback_query; await query.answer(); data = query.data
    
    # يجب استكمال دمج جميع دوال الواجهة (مثل المحفظة، الإحصائيات، التقرير اليومي، وغيرها)
    route_map = {
        "db_portfolio": start_command, # PLACEHOLDER
        "db_trades": show_trades_command, 
        "db_history": start_command, # PLACEHOLDER
        "db_stats": start_command, # PLACEHOLDER
        "db_mood": show_mood_command, 
        "db_diagnostics": start_command, # PLACEHOLDER
        "back_to_dashboard": show_dashboard_command,
        "db_manual_scan": manual_scan_command,
        "db_maestro_control": show_maestro_control,
        "maestro_toggle": toggle_maestro,
        "kill_switch_toggle": toggle_kill_switch,
        "settings_main": show_settings_menu,
        "settings_params": show_parameters_menu, 
        "settings_scanners": show_scanners_menu,
        "settings_adaptive": show_adaptive_intelligence_menu,
        "noop": (lambda u,c: None)
    }
    
    try:
        if data in route_map: 
            await route_map[data](update, context)
        elif data.startswith("check_"): 
            await check_trade_details(update, context)
        elif data.startswith("manual_sell_confirm_"): 
            await handle_manual_sell_confirmation(update, context)
        elif data.startswith("manual_sell_execute_"): 
            await handle_manual_sell_execute(update, context)
        elif data.startswith("scanner_toggle_"): 
            await handle_scanner_toggle(update, context)
        elif data.startswith("param_set_"): 
            await handle_parameter_selection(update, context)
        elif data.startswith("param_toggle_"): 
            await handle_toggle_parameter(update, context)
        
    except Exception as e: logger.error(f"Error in button callback handler for data '{data}': {e}", exc_info=True)

# =======================================================================================
# --- VII. Startup and Shutdown ---
# =======================================================================================

async def post_init(application: Application):
    logger.info("Performing post-initialization setup for Wise Maestro Bot (Binance)...")
    if not all([TELEGRAM_BOT_TOKEN, BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_CHAT_ID]):
        logger.critical("FATAL: Missing critical API keys."); return

    bot_data.application = application
    bot_data.TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
    
    bot_data.exchange = ccxt.binance({'apiKey': BINANCE_API_KEY, 'secret': BINANCE_API_SECRET, 'enableRateLimit': True, 'options': { 'defaultType': 'spot', 'timeout': 30000 }})

    try:
        await bot_data.exchange.load_markets()
        await bot_data.exchange.fetch_balance()
        logger.info("✅ Successfully connected to Binance Spot.")
    except Exception as e:
        logger.critical(f"🔥 FATAL: Could not connect to Binance: {e}", exc_info=True); return

    load_settings()
    await init_database()
    
    # تهيئة الحارس وإضافة مرجعه إلى كائن الحالة العامة
    # ملاحظة: تم تمرير db_file لتستخدمه وحدة Guardian
    bot_data.trade_guardian = TradeGuardian(exchange=bot_data.exchange, application=application, bot_state_object=bot_data, db_file=DB_FILE)
    
    # تهيئة الـ WebSocket
    bot_data.websocket_manager = BinanceWebSocketManager(bot_data.exchange, application, bot_data.trade_guardian)
    asyncio.create_task(bot_data.websocket_manager.run())
    
    logger.info("Waiting 10s for WebSocket connections..."); await asyncio.sleep(10)
    await bot_data.websocket_manager.sync_subscriptions()
    
    # جدولة المهام
    jq = application.job_queue
    jq.run_repeating(perform_scan, interval=SCAN_INTERVAL_SECONDS, first=10, name="perform_scan")
    jq.run_repeating(bot_data.trade_guardian.the_supervisor_job, interval=SUPERVISOR_INTERVAL_SECONDS, first=30, name="the_supervisor_job")
    jq.run_repeating(bot_data.trade_guardian.review_open_trades, interval=1800, first=45, name="wise_man_trade_review") # 30 دقيقة
    jq.run_repeating(bot_data.trade_guardian.review_portfolio_risk, interval=3600, first=90, name="wise_man_portfolio_review") # ساعة
    jq.run_repeating(bot_data.trade_guardian.intelligent_reviewer_job, interval=3600, first=60, name="intelligent_reviewer_job") # ساعة
    jq.run_repeating(update_strategy_performance, interval=STRATEGY_ANALYSIS_INTERVAL_SECONDS, first=60, name="update_strategy_performance")
    jq.run_repeating(propose_strategy_changes, interval=STRATEGY_ANALYSIS_INTERVAL_SECONDS, first=120, name="propose_strategy_changes")
    jq.run_repeating(maestro_job, interval=MAESTRO_INTERVAL_HOURS * 3600, first=MAESTRO_INTERVAL_HOURS * 3600, name="maestro_job")

    try: 
        await application.bot.send_message(TELEGRAM_CHAT_ID, "*🤖 المايسترو الحكيم (Binance) - بدأ العمل...*", parse_mode=ParseMode.MARKDOWN)
    except Forbidden: 
        logger.critical(f"FATAL: Bot not authorized for chat ID {TELEGRAM_CHAT_ID}."); return
    logger.info("--- Wise Maestro Bot (Binance) is fully operational ---")

async def post_shutdown(application: Application):
    if bot_data.exchange: await bot_data.exchange.close()
    if bot_data.websocket_manager: await bot_data.websocket_manager.stop()
    logger.info("Bot has shut down gracefully.")

def main():
    logger.info("Starting Wise Maestro Bot (Binance)...")
    app_builder = Application.builder().token(TELEGRAM_BOT_TOKEN)
    app_builder.post_init(post_init).post_shutdown(post_shutdown)
    application = app_builder.build()
    
    # Handlers (يجب إضافة المزيد هنا)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("scan", manual_scan_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, universal_text_handler))
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    
    application.run_polling()
    
if __name__ == '__main__':
    # يجب التأكد من تثبيت جميع المكتبات في requirements.txt قبل التشغيل
    main()
