# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🚀 Wise Maestro Bot - Final Fusion v7.0 (Binance Edition) 🚀 ---
# =======================================================================================
# --- سجل التغييرات للإصدار 7.0 (الدمج النهائي) ---
#   ✅ [دمج] إضافة "وضع المايسترو" لتحليل نظام السوق وتغيير الإعدادات ديناميكيًا.
#   ✅ [دمج] إضافة استراتيجيات جديدة مثل "انعكاس بولينجر".
#   ✅ [دمج] إضافة "فلتر التوافق الزمني المتقدم" لزيادة دقة الإشارات.
#   ✅ [دمج] إضافة "لوحة التحكم الاستراتيجي" للمايسترو في واجهة تليجرام.
#   ✅ [تحديث] تعديل WebSocket Manager ليتصل بالحارس الموحد (wise_maestro_guardian.py).
#   ✅ [تحديث] الاعتماد الكامل على ui_handlers.py لواجهة المستخدم.
#   ✅ [إصلاح] استعادة المنطق الكامل لدوال الفحص (perform_scan, worker_batch).
# =======================================================================================

import os
import logging
import asyncio
import json
import time
import copy
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
import aiosqlite
import pandas as pd
import ccxt.async_support as ccxt
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from dotenv import load_dotenv
import websockets
import websockets.exceptions
import redis.async_io as redis

# --- استيراد الوحدات المنفصلة ---
from settings_config import *
from strategy_scanners import SCANNERS, find_col, filter_whale_radar
from ai_market_brain import get_market_regime, get_market_mood, get_binance_markets
from smart_engine import EvolutionaryEngine
import ui_handlers 
from wise_maestro_guardian import TradeGuardian as MaestroGuardian

load_dotenv()

# --- جلب المتغيرات ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

# --- إعدادات أساسية ---
EGYPT_TZ = ZoneInfo("Africa/Cairo")
DB_FILE = 'wise_maestro_binance.db'
SETTINGS_FILE = 'wise_maestro_binance_settings.json'

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("BINANCE_MAESTRO_FUSION")

# --- الحالة العامة للبوت ---
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
        self.guardian = None
        self.smart_brain = None
        self.TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
        self.current_market_regime = "UNKNOWN"
        self.redis_client = None
        self.trade_management_lock = asyncio.Lock()

bot_data = BotState()
scan_lock = asyncio.Lock()

# --- WebSocket Manager (Binance Edition) ---
class BinanceWebSocketManager:
    def __init__(self, exchange, application):
        self.exchange = exchange
        self.application = application
        self.listen_key = None
        self.public_subscriptions = set()
        self.ws = None
        self.is_running = False
        self.keep_alive_task = None

    async def _get_listen_key(self):
        try:
            self.listen_key = (await self.exchange.publicPostUserDataStream())['listenKey']
            logger.info("WebSocket Manager: New listen key obtained.")
            return True
        except Exception as e:
            logger.error(f"WebSocket Manager: Failed to get listen key: {e}")
            return False

    async def _keep_alive_listen_key(self):
        while self.is_running:
            await asyncio.sleep(1800) # 30 minutes
            if self.listen_key:
                try:
                    await self.exchange.publicPutUserDataStream({'listenKey': self.listen_key})
                except Exception:
                    logger.warning("WebSocket Manager: Failed to keep listen key alive.")
                    self.listen_key = None 

    async def run(self):
        self.is_running = True
        self.keep_alive_task = asyncio.create_task(self._keep_alive_listen_key())
        while self.is_running:
            if not self.listen_key and not await self._get_listen_key():
                await asyncio.sleep(60); continue
            
            streams = [f"{s.lower().replace('/', '')}@ticker" for s in self.public_subscriptions]
            if self.listen_key: streams.append(self.listen_key)
            if not streams:
                await asyncio.sleep(10); continue

            uri = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
            try:
                async with websockets.connect(uri, ping_interval=180, ping_timeout=60) as ws:
                    self.ws = ws
                    logger.info(f"✅ [Binance WS] Connected. Watching {len(self.public_subscriptions)} symbols.")
                    async for message in ws:
                        await self._handle_message(message)
            except (websockets.exceptions.ConnectionClosed, Exception) as e:
                if self.is_running:
                    logger.warning(f"Binance WS: Connection lost: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)

    async def _handle_message(self, message):
        try:
            data = json.loads(message)
            payload = data.get('data', data)
            event_type = payload.get('e')

            if event_type == '24hrTicker':
                if bot_data.guardian:
                    standard_ticker = {
                        'symbol': payload['s'].replace('USDT', '/USDT'),
                        'price': float(payload['c'])
                    }
                    await bot_data.guardian.handle_ticker_update(standard_ticker)

            elif event_type == 'executionReport':
                if payload.get('x') == 'TRADE' and payload.get('S') == 'BUY' and payload.get('X') == 'FILLED':
                    await handle_filled_buy_order(payload)
            
            elif event_type == 'listenKeyExpired':
                logger.warning("Listen key expired. Getting a new one.")
                self.listen_key = None
                if self.ws: await self.ws.close()
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}", exc_info=True)

    async def sync_subscriptions(self):
        async with aiosqlite.connect(DB_FILE) as conn:
            active_symbols = {row[0] for row in await (await conn.execute("SELECT DISTINCT symbol FROM trades WHERE status = 'active'")).fetchall()}
        
        if active_symbols != self.public_subscriptions:
            logger.info(f"WebSocket: Syncing subscriptions. New count: {len(active_symbols)}")
            self.public_subscriptions = active_symbols
            if self.ws and not self.ws.closed:
                try: await self.ws.close(code=1000, reason='Subscription change')
                except Exception: pass
    
    async def stop(self):
        self.is_running = False
        if self.keep_alive_task: self.keep_alive_task.cancel()
        if self.ws and not self.ws.closed: await self.ws.close()

# --- Helper, Settings & DB Management ---
def load_settings():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f: bot_data.settings = json.load(f)
        else: bot_data.settings = copy.deepcopy(DEFAULT_SETTINGS)
    except Exception: bot_data.settings = copy.deepcopy(DEFAULT_SETTINGS)
    for key, value in DEFAULT_SETTINGS.items():
        bot_data.settings.setdefault(key, value)
    save_settings()
    logger.info("Settings loaded successfully.")

def save_settings():
    with open(SETTINGS_FILE, 'w') as f: json.dump(bot_data.settings, f, indent=4)

async def init_database():
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute('CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, entry_price REAL, take_profit REAL, stop_loss REAL, quantity REAL, status TEXT, reason TEXT, order_id TEXT, highest_price REAL DEFAULT 0, trailing_sl_active BOOLEAN DEFAULT 0, close_price REAL, pnl_usdt REAL, last_profit_notification_price REAL DEFAULT 0, trade_weight REAL DEFAULT 1.0)')
            await conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e: logger.critical(f"Database initialization failed: {e}")

async def safe_send_message(bot, text, **kwargs):
    try:
        await bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        logger.error(f"Telegram Send Error: {e}.")

# --- Core Trading Logic ---
async def log_pending_trade_to_db(signal, buy_order):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("INSERT INTO trades (timestamp, symbol, reason, order_id, status, entry_price, take_profit, stop_loss, trade_weight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (datetime.now(EGYPT_TZ).isoformat(), signal['symbol'], signal['reason'], buy_order['id'], 'pending', signal['entry_price'], signal['take_profit'], signal['stop_loss'], signal.get('weight', 1.0)))
            await conn.commit()
            return True
    except Exception as e:
        logger.error(f"DB Log Pending Error for {signal['symbol']}: {e}")
        return False

async def activate_trade(order_id, symbol):
    bot = bot_data.application.bot
    try:
        order_details = await bot_data.exchange.fetch_order(order_id, symbol)
        filled_price = float(order_details.get('average', 0.0))
        net_filled_quantity = float(order_details.get('filled', 0.0))
        if net_filled_quantity <= 0 or filled_price <= 0: return
    except Exception as e:
        logger.error(f"Could not fetch order details for activation of {order_id}: {e}"); return

    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trade = await (await conn.execute("SELECT * FROM trades WHERE order_id = ? AND status = 'pending'", (order_id,))).fetchone()
        if not trade: return
        trade = dict(trade)
        risk = filled_price - trade['stop_loss']
        new_take_profit = filled_price + (risk * bot_data.settings['risk_reward_ratio'])
        await conn.execute("UPDATE trades SET status = 'active', entry_price = ?, quantity = ?, take_profit = ?, last_profit_notification_price = ? WHERE id = ?", (filled_price, net_filled_quantity, new_take_profit, filled_price, trade['id']))
        active_trades_count = (await (await conn.execute("SELECT COUNT(*) FROM trades WHERE status = 'active'")).fetchone())[0]
        await conn.commit()

    await bot_data.websocket_manager.sync_subscriptions()
    tp_percent = (new_take_profit / filled_price - 1) * 100
    sl_percent = (1 - trade['stop_loss'] / filled_price) * 100
    reasons_ar = ' + '.join([STRATEGY_NAMES_AR.get(r.strip(), r.strip()) for r in trade['reason'].split(' + ')])
    msg = (f"✅ **تم تأكيد شراء Binance | {symbol}**\n"
           f"**الاستراتيجية:** {reasons_ar}\n"
           f"**رقم:** `#{trade['id']}` | **سعر:** `${filled_price:,.4f}`\n"
           f"**الهدف:** `${new_take_profit:,.4f}` `({tp_percent:+.2f}%)`\n"
           f"**الوقف:** `${trade['stop_loss']:,.4f}` `({sl_percent:.2f}%)`\n"
           f"**الصفقات النشطة:** `{active_trades_count}`")
    await safe_send_message(bot, msg)

async def handle_filled_buy_order(order_data):
    if order_data.get('X') == 'FILLED' and order_data.get('S') == 'BUY':
        await activate_trade(order_data['i'], order_data['s'].replace('USDT', '/USDT'))

async def has_active_trade_for_symbol(symbol: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as conn:
        return (await (await conn.execute("SELECT 1 FROM trades WHERE symbol = ? AND status IN ('active', 'pending') LIMIT 1", (symbol,))).fetchone()) is not None

async def initiate_real_trade(signal):
    if not bot_data.trading_enabled: return False
    try:
        settings, exchange = bot_data.settings, bot_data.exchange
        trade_size = settings['real_trade_size_usdt']
        market = exchange.market(signal['symbol'])
        min_notional = float(market.get('limits', {}).get('notional', {}).get('min', '0'))
        if trade_size < min_notional * 1.05: return False
        balance = await exchange.fetch_balance()
        if balance.get('USDT', {}).get('free', 0.0) < trade_size: return False
        base_amount = trade_size / signal['entry_price']
        formatted_amount = exchange.amount_to_precision(signal['symbol'], base_amount)
        buy_order = await exchange.create_market_buy_order(signal['symbol'], formatted_amount)
        if await log_pending_trade_to_db(signal, buy_order):
            await safe_send_message(bot_data.application.bot, f"🚀 تم إرسال أمر شراء لـ `{signal['symbol']}`...")
            return True
        else:
            await exchange.cancel_order(buy_order['id'], signal['symbol']); return False
    except Exception as e:
        logger.error(f"REAL TRADE FAILED {signal['symbol']}: {e}", exc_info=True); return False

# --- Scanner Worker and Main Scan Function ---
async def worker_batch(queue, signals_list, errors_list):
    settings, exchange = bot_data.settings, bot_data.exchange
    while not queue.empty():
        symbol = ""
        try:
            market = await queue.get()
            symbol = market['symbol']
            ohlcv = await exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=220)
            if len(ohlcv) < 50: queue.task_done(); continue
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # --- [الدمج] فلتر التوافق الزمني المتقدم من بوت OKX ---
            if settings.get('multi_timeframe_confluence_enabled', True):
                try:
                    ohlcv_1h, ohlcv_4h = await asyncio.gather(
                        exchange.fetch_ohlcv(symbol, '1h', limit=100),
                        exchange.fetch_ohlcv(symbol, '4h', limit=201) # 201 to be safe for EMA 200
                    )
                    df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_1h.ta.macd(append=True); df_1h.ta.sma(length=50, append=True)
                    is_1h_bullish = (df_1h[find_col(df_1h.columns, "MACD_")].iloc[-1] > df_1h[find_col(df_1h.columns, "MACDs_")].iloc[-1]) and \
                                    (df_1h['close'].iloc[-1] > df_1h[find_col(df_1h.columns, "SMA_50")].iloc[-1])
                    
                    df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_4h.ta.ema(length=200, append=True)
                    is_4h_bullish = df_4h['close'].iloc[-1] > df_4h[find_col(df_4h.columns, "EMA_200")].iloc[-1]
                    
                    if not (is_1h_bullish and is_4h_bullish):
                        queue.task_done(); continue
                except Exception: pass
            
            # --- بقية الفلاتر والماسحات ---
            confirmed_reasons = []
            if 'whale_radar' in settings['active_scanners']:
                if await filter_whale_radar(exchange, symbol, settings):
                    confirmed_reasons.append("whale_radar")

            for name in settings['active_scanners']:
                if name == 'whale_radar': continue
                if not (strategy_func := SCANNERS.get(name)): continue
                params = settings.get(name, {})
                func_args = {'df': df.copy(), 'params': params}
                if name in ['support_rebound']: func_args.update({'exchange': exchange, 'symbol': symbol})
                
                result = await strategy_func(**func_args) if asyncio.iscoroutinefunction(strategy_func) else strategy_func(**func_args)
                if result: confirmed_reasons.append(result['reason'])

            if confirmed_reasons:
                reason_str = ' + '.join(set(confirmed_reasons))
                entry_price = df.iloc[-1]['close']
                df.ta.atr(length=14, append=True)
                atr_col = find_col(df.columns, "ATRr_14")
                atr = df[atr_col].iloc[-1] if atr_col and pd.notna(df[atr_col].iloc[-1]) else (df['high'].iloc[-1] - df['low'].iloc[-1])
                risk = atr * settings['atr_sl_multiplier']
                signals_list.append({"symbol": symbol, "entry_price": entry_price, "take_profit": entry_price + (risk * settings['risk_reward_ratio']), "stop_loss": entry_price - risk, "reason": reason_str})
            queue.task_done()
        except Exception as e:
            errors_list.append(symbol if symbol else 'Unknown')
            if not queue.empty(): queue.task_done()

async def perform_scan(context: ContextTypes.DEFAULT_TYPE):
    async with scan_lock:
        if not bot_data.trading_enabled: return
        scan_start_time = time.time(); logger.info("--- Starting new Fused Maestro scan (Binance)... ---")
        settings = bot_data.settings
        
        mood_result = await get_market_mood(bot_data)
        bot_data.market_mood = mood_result
        if mood_result['mood'] in ["NEGATIVE", "DANGEROUS"]: 
            logger.warning(f"Scan skipped due to market mood: {mood_result['reason']}")
            return

        async with aiosqlite.connect(DB_FILE) as conn:
            active_trades_count = (await (await conn.execute("SELECT COUNT(*) FROM trades WHERE status IN ('active', 'pending')")).fetchone())[0]
        if active_trades_count >= settings['max_concurrent_trades']: 
            logger.info(f"Scan skipped: Max concurrent trades ({active_trades_count}) reached.")
            return
        
        top_markets = await get_binance_markets(bot_data)
        if not top_markets: 
            logger.warning("Scan could not retrieve any markets.")
            return
            
        queue, signals_found, analysis_errors = asyncio.Queue(), [], []
        for market in top_markets:
            await queue.put(market)
        
        worker_tasks = [asyncio.create_task(worker_batch(queue, signals_found, analysis_errors)) for _ in range(settings.get("worker_threads", 10))]
        await queue.join()
        for task in worker_tasks: task.cancel()
        
        trades_opened_count = 0
        for signal in signals_found:
            if active_trades_count >= settings['max_concurrent_trades']: break
            if not await has_active_trade_for_symbol(signal['symbol']):
                # await broadcast_signal_to_redis(signal) # You can uncomment this if you use Redis
                if await initiate_real_trade(signal):
                    trades_opened_count += 1
                    active_trades_count += 1
                    await asyncio.sleep(2)
        
        scan_duration = time.time() - scan_start_time
        bot_data.last_scan_info = {"duration_seconds": int(scan_duration), "checked_symbols": len(top_markets)}
        logger.info(f"Scan complete in {int(scan_duration)}s. Found {len(signals_found)} signals, opened {trades_opened_count} trades.")


# --- Maestro Job ---
async def maestro_job(context: ContextTypes.DEFAULT_TYPE):
    if not bot_data.settings.get('maestro_mode_enabled', True): return
    logger.info("🎼 Maestro (Binance): Analyzing market regime...")
    regime = await get_market_regime(bot_data.exchange)
    
    if regime != "UNKNOWN" and regime != bot_data.current_market_regime:
        bot_data.current_market_regime = regime
        config = DECISION_MATRIX.get(regime, {})
        if not config: return
        
        changes_report = []
        for key, value in config.items():
            if key in bot_data.settings and bot_data.settings[key] != value:
                old_value = bot_data.settings[key]
                bot_data.settings[key] = value
                changes_report.append(f"- `{key}` from `{old_value}` to `{value}`")
        
        save_settings()
        if changes_report:
            report_text = "\n".join(changes_report)
            active_scanners_str = ' + '.join([STRATEGY_NAMES_AR.get(s, s) for s in config.get('active_scanners', [])])
            report = (f"🎼 **تقرير المايسترو (Binance) | {regime}**\n"
                      f"تم تعديل الإعدادات لتناسب حالة السوق.\n\n"
                      f"**أهم التغييرات:**\n{report_text}\n\n"
                      f"**الاستراتيجيات النشطة الآن:**\n{active_scanners_str}")
            await safe_send_message(context.bot, report)


# --- Bot Startup ---
async def post_init(application: Application):
    logger.info("Performing post-initialization for Fused Maestro Bot [Binance Edition]...")
    if not all([TELEGRAM_BOT_TOKEN, BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_CHAT_ID]):
        logger.critical("FATAL: Missing critical environment variables."); return
    
    bot_data.application = application
    bot_data.exchange = ccxt.binance({'apiKey': BINANCE_API_KEY, 'secret': BINANCE_API_SECRET, 'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
    try:
        await bot_data.exchange.load_markets()
    except Exception as e:
        logger.critical(f"🔥 FATAL: Could not connect to Binance: {e}"); return

    load_settings()
    await init_database()

    bot_data.guardian = MaestroGuardian(bot_data.exchange, application, bot_data, DB_FILE)
    bot_data.smart_brain = EvolutionaryEngine(bot_data.exchange, application, DB_FILE)
    bot_data.websocket_manager = BinanceWebSocketManager(bot_data.exchange, application)
    
    asyncio.create_task(bot_data.websocket_manager.run())
    await bot_data.websocket_manager.sync_subscriptions()
    
    jq = application.job_queue
    jq.run_repeating(perform_scan, interval=SCAN_INTERVAL_SECONDS, first=10, name="perform_scan")
    jq.run_repeating(bot_data.guardian.the_supervisor_job, interval=SUPERVISOR_INTERVAL_SECONDS, first=30, name="supervisor_job")
    jq.run_repeating(bot_data.guardian.intelligent_reviewer_job, interval=3600, first=60, name="intelligent_reviewer")
    jq.run_repeating(bot_data.guardian.review_open_trades, interval=14400, first=120, name="wise_man_review")
    jq.run_repeating(bot_data.guardian.review_portfolio_risk, interval=86400, first=180, name="portfolio_risk_review")
    jq.run_repeating(maestro_job, interval=MAESTRO_INTERVAL_HOURS * 3600, first=5, name="maestro_job")
    jq.run_daily(bot_data.smart_brain.run_pattern_discovery, time=dt_time(hour=22, minute=0, tzinfo=EGYPT_TZ), name='pattern_discovery_job')
    
    logger.info(f"All Fused jobs scheduled for Binance Bot.")
    await safe_send_message(application.bot, "*🤖 Wise Maestro Bot (Binance Fused Edition) - بدأ العمل...*")
    logger.info("--- Fused Maestro Bot (Binance Edition) is now fully operational ---")

async def post_shutdown(application: Application):
    if bot_data.exchange: await bot_data.exchange.close()
    if bot_data.websocket_manager: await bot_data.websocket_manager.stop()
    if bot_data.redis_client: await bot_data.redis_client.close()
    logger.info("Bot has shut down gracefully.")

def main():
    logger.info("Starting Fused Maestro Bot - Binance Edition...")
    app_builder = Application.builder().token(TELEGRAM_BOT_TOKEN)
    app_builder.post_init(post_init).post_shutdown(post_shutdown)
    application = app_builder.build()
    
    application.bot_data = bot_data
    
    application.add_handler(CommandHandler("start", ui_handlers.start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, ui_handlers.universal_text_handler))
    application.add_handler(CallbackQueryHandler(ui_handlers.button_callback_handler))
    
    application.run_polling()

if __name__ == '__main__':
    main()
