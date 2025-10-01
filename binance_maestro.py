# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🚀 Wise Maestro Bot - Final Fusion v2.0 🚀 ---
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
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from telegram.constants import ParseMode
from telegram.error import Forbidden, BadRequest, TimedOut
from dotenv import load_dotenv
import websockets
import websockets.exceptions
import redis.asyncio as redis

# استيراد الوحدات المنفصلة
from _settings_config import *
import _strategy_scanners as scanners
import _ai_market_brain as brain
from _smart_engine import EvolutionaryEngine
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
logger = logging.getLogger("BINANCE_MAESTRO")

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
        self.strategy_performance = {}
        self.pending_strategy_proposal = {}
        self.last_deep_analysis_time = defaultdict(float)
        self.guardian = None
        self.smart_brain = None
        self.TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
        self.current_market_regime = "UNKNOWN"
        self.redis_client = None

bot_data = BotState()
scan_lock = asyncio.Lock()
trade_management_lock = asyncio.Lock()

# --- WebSocket Manager ---
class WebSocketManager:
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
            self.listen_key = None
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
                    logger.info(f"✅ [WebSocket] Connected. Watching {len(self.public_subscriptions)} symbols.")
                    async for message in ws:
                        await self._handle_message(message)
            except (websockets.exceptions.ConnectionClosed, Exception) as e:
                if self.is_running:
                    logger.warning(f"WebSocket: Connection lost: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)
                else: break

    async def _handle_message(self, message):
        try:
            data = json.loads(message)
            payload = data.get('data', data)
            event_type = payload.get('e')

            if event_type == '24hrTicker':
                if bot_data.guardian:
                    await bot_data.guardian.handle_ticker_update(payload)
            elif event_type == 'executionReport':
                if payload.get('x') == 'TRADE' and payload.get('S') == 'BUY' and payload.get('X') == 'FILLED':
                    await handle_order_update(payload)
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
            logger.info(f"WebSocket: Syncing subscriptions. New: {len(active_symbols)}")
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
    determine_active_preset(); save_settings()
    logger.info(f"Settings loaded. Active preset: {bot_data.active_preset_name}")

def determine_active_preset():
    current_settings_for_compare = {k: v for k, v in bot_data.settings.items() if k in SETTINGS_PRESETS['professional']}
    for name, preset_settings in SETTINGS_PRESETS.items():
        if all(current_settings_for_compare.get(key) == value for key, value in preset_settings.items()):
            bot_data.active_preset_name = PRESET_NAMES_AR.get(name, "مخصص"); return
    bot_data.active_preset_name = "مخصص"

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
    for _ in range(3):
        try:
            await bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
            return
        except (TimedOut, Forbidden) as e:
            logger.error(f"Telegram Send Error: {e}.")
            if isinstance(e, Forbidden): return
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Unknown Telegram Send Error: {e}.")
            await asyncio.sleep(2)

async def broadcast_signal_to_redis(signal):
    if not bot_data.redis_client: return
    try:
        signal_to_broadcast = {k: (v.isoformat() if isinstance(v, (datetime, pd.Timestamp)) else v) for k, v in signal.items()}
        await bot_data.redis_client.publish("trade_signals", json.dumps(signal_to_broadcast))
        logger.info(f"📡 Broadcasted signal for {signal['symbol']} to Redis.")
    except Exception as e:
        logger.error(f"Redis Broadcast Error: {e}", exc_info=True)

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
    msg = (f"✅ **تم تأكيد الشراء | {symbol}**\n"
           f"**الاستراتيجية:** {reasons_ar}\n"
           f"**رقم:** `#{trade['id']}` | **سعر:** `${filled_price:,.4f}`\n"
           f"**الهدف:** `${new_take_profit:,.4f}` `({tp_percent:+.2f}%)`\n"
           f"**الوقف:** `${trade['stop_loss']:,.4f}` `({sl_percent:.2f}%)`\n"
           f"**الصفقات النشطة:** `{active_trades_count}`")
    await safe_send_message(bot, msg)

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

async def handle_order_update(order_data):
    if order_data.get('X') == 'FILLED' and order_data.get('S') == 'BUY':
        await activate_trade(order_data['i'], order_data['s'].replace('USDT', '/USDT'))

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
            
            # --- Confluence Filter ---
            if settings.get('multi_timeframe_confluence_enabled', True):
                try:
                    ohlcv_1h_task = exchange.fetch_ohlcv(symbol, '1h', limit=100)
                    ohlcv_4h_task = exchange.fetch_ohlcv(symbol, '4h', limit=100)
                    ohlcv_1h, ohlcv_4h = await asyncio.gather(ohlcv_1h_task, ohlcv_4h_task)
                    df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_1h.ta.macd(append=True); df_1h.ta.sma(length=50, append=True)
                    is_1h_bullish = (df_1h[scanners.find_col(df_1h.columns, "MACD_")].iloc[-1] > df_1h[scanners.find_col(df_1h.columns, "MACDs_")].iloc[-1]) and \
                                    (df_1h['close'].iloc[-1] > df_1h[scanners.find_col(df_1h.columns, "SMA_50")].iloc[-1])
                    df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df_4h.ta.ema(length=200, append=True)
                    is_4h_bullish = df_4h['close'].iloc[-1] > df_4h[scanners.find_col(df_4h.columns, "EMA_200")].iloc[-1]
                    if not (is_1h_bullish and is_4h_bullish):
                        queue.task_done(); continue
                except Exception: pass
            
            # --- Other Filters (Spread, Volume, etc.) would be added here ---

            # --- Scanners ---
            confirmed_reasons = []
            if 'whale_radar' in settings['active_scanners']:
                if await scanners.filter_whale_radar(exchange, symbol, settings):
                    confirmed_reasons.append("whale_radar")
            for name in settings['active_scanners']:
                if name == 'whale_radar': continue
                if not (strategy_func := scanners.SCANNERS.get(name)): continue
                params = settings.get(name, {})
                func_args = {'df': df.copy(), 'params': params, 'rvol': 0, 'adx_value': 0}
                if name == 'support_rebound': func_args.update({'exchange': exchange, 'symbol': symbol})
                result = await strategy_func(**func_args) if asyncio.iscoroutinefunction(strategy_func) else strategy_func(**{k: v for k, v in func_args.items() if k not in ['exchange', 'symbol']})
                if result: confirmed_reasons.append(result['reason'])

            if confirmed_reasons:
                reason_str = ' + '.join(set(confirmed_reasons))
                entry_price = df.iloc[-1]['close']
                df.ta.atr(length=14, append=True)
                atr_col = scanners.find_col(df.columns, "ATRr_14")
                atr = df[atr_col].iloc[-1] if atr_col and pd.notna(df[atr_col].iloc[-1]) else df['high'].iloc[-1] - df['low'].iloc[-1]
                risk = atr * settings['atr_sl_multiplier']
                signals_list.append({"symbol": symbol, "entry_price": entry_price, "take_profit": entry_price + (risk * settings['risk_reward_ratio']), "stop_loss": entry_price - risk, "reason": reason_str})
            queue.task_done()
        except Exception as e:
            errors_list.append(symbol if symbol else 'Unknown')
            if not queue.empty(): queue.task_done()

async def perform_scan(context: ContextTypes.DEFAULT_TYPE):
    async with scan_lock:
        if not bot_data.trading_enabled: return
        scan_start_time = time.time(); logger.info("--- Starting new Maestro scan... ---")
        settings = bot_data.settings
        async with aiosqlite.connect(DB_FILE) as conn:
            active_trades_count = (await (await conn.execute("SELECT COUNT(*) FROM trades WHERE status IN ('active', 'pending')")).fetchone())[0]
        if active_trades_count >= settings['max_concurrent_trades']: return
        
        top_markets = await brain.get_binance_markets(bot_data)
        if not top_markets: return
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
                await broadcast_signal_to_redis(signal)
                if await initiate_real_trade(signal):
                    trades_opened_count += 1; active_trades_count += 1
                    await asyncio.sleep(2)
        
        scan_duration = time.time() - scan_start_time
        bot_data.last_scan_info = {"duration_seconds": int(scan_duration), "checked_symbols": len(top_markets)}
        logger.info(f"Scan complete in {int(scan_duration)}s. Found {len(signals_found)} signals, opened {trades_opened_count} trades.")

# --- Scheduled Jobs ---
async def maestro_job(context: ContextTypes.DEFAULT_TYPE):
    if not bot_data.settings.get('maestro_mode_enabled', True): return
    logger.info("🎼 Maestro: Analyzing market regime and adjusting tactics...")
    regime = await brain.get_market_regime(bot_data.exchange)
    
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
            report = (f"🎼 **تقرير المايسترو | {regime}**\n"
                      f"تم تعديل التكوين ليتناسب مع حالة السوق.\n\n"
                      f"**أهم التغييرات:**\n{report_text}\n\n"
                      f"**الاستراتيجيات النشطة الآن:**\n{active_scanners_str}")
            await safe_send_message(context.bot, report)

# --- Telegram UI & Bot Startup ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["Dashboard 🖥️"], ["الإعدادات ⚙️"]]
    await update.message.reply_text("أهلاً بك في **Wise Maestro Bot**", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True), parse_mode=ParseMode.MARKDOWN)

async def universal_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'setting_to_change' in context.user_data or 'blacklist_action' in context.user_data:
        await handle_setting_value(update, context); return
    text = update.message.text
    if text == "Dashboard 🖥️": await ui_handlers.show_dashboard_command(update, context)
    elif text == "الإعدادات ⚙️": await ui_handlers.show_settings_menu(update, context)

async def handle_setting_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()
    # Handle blacklist
    if 'blacklist_action' in context.user_data:
        action = context.user_data.pop('blacklist_action')
        blacklist = bot_data.settings.get('asset_blacklist', [])
        symbol = user_input.upper().replace("/USDT", "")
        if action == 'add':
            if symbol not in blacklist: blacklist.append(symbol)
        elif action == 'remove':
            if symbol in blacklist: blacklist.remove(symbol)
        bot_data.settings['asset_blacklist'] = blacklist
        save_settings()
        await update.message.reply_text(f"✅ تم تحديث القائمة السوداء.")
        # Refresh menu
        dummy_query = type('Query', (), {'message': update.message, 'data': 'settings_blacklist', 'edit_message_text': (lambda *args, **kwargs: asyncio.sleep(0)), 'answer': (lambda *args, **kwargs: asyncio.sleep(0))})
        await ui_handlers.show_blacklist_menu(Update(update.update_id, callback_query=dummy_query), context)
        return

    # Handle other parameters
    if not (setting_key := context.user_data.get('setting_to_change')): return
    try:
        keys = setting_key.split('_'); current_level = bot_data.settings
        for key in keys[:-1]: current_level = current_level[key]
        last_key = keys[-1]; original_value = current_level[last_key]
        new_value = type(original_value)(user_input)
        current_level[last_key] = new_value
        save_settings(); determine_active_preset()
        await update.message.reply_text(f"✅ تم تحديث `{setting_key}` إلى `{new_value}`.")
    except (ValueError, KeyError):
        await update.message.reply_text("❌ قيمة غير صالحة.")
    finally:
        if 'setting_to_change' in context.user_data: del context.user_data['setting_to_change']

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer(); data = query.data
    route_map = {
        "db_stats": ui_handlers.show_stats_command, 
        "db_trades": ui_handlers.show_trades_command, 
        "db_history": ui_handlers.show_trade_history_command,
        "db_mood": ui_handlers.show_mood_command, 
        "db_diagnostics": ui_handlers.show_diagnostics_command, 
        "back_to_dashboard": ui_handlers.show_dashboard_command,
        "db_portfolio": ui_handlers.show_portfolio_command, 
        "db_manual_scan": (lambda u,c: context.job_queue.run_once(perform_scan, 1)),
        "settings_main": ui_handlers.show_settings_menu, 
        "settings_params": ui_handlers.show_parameters_menu, 
        "settings_scanners": ui_handlers.show_scanners_menu,
        "settings_presets": ui_handlers.show_presets_menu, 
        "settings_blacklist": ui_handlers.show_blacklist_menu, 
        "settings_data": ui_handlers.show_data_management_menu,
        "settings_adaptive": ui_handlers.show_adaptive_intelligence_menu,
        "noop": (lambda u,c: None)
    }
    
    if data in route_map: await route_map[data](update, context)
    elif data.startswith("check_"): await ui_handlers.check_trade_details(update, context)
    elif data.startswith("manual_sell_confirm_"): await ui_handlers.handle_manual_sell_confirmation(update, context)
    elif data.startswith("manual_sell_execute_"): await ui_handlers.handle_manual_sell_execute(update, context)
    elif data == "kill_switch_toggle":
        bot_data.trading_enabled = not bot_data.trading_enabled
        await query.answer("✅ Trading Resumed" if bot_data.trading_enabled else "🚨 Kill Switch Activated", show_alert=not bot_data.trading_enabled)
        await ui_handlers.show_dashboard_command(update, context)
    elif data.startswith("scanner_toggle_"):
        key = data.replace("scanner_toggle_", "")
        scanners_list = bot_data.settings['active_scanners']
        if key in scanners_list:
            if len(scanners_list) > 1: scanners_list.remove(key)
        else: scanners_list.append(key)
        save_settings(); determine_active_preset()
        await ui_handlers.show_scanners_menu(update, context)
    elif data.startswith("param_set_"):
        context.user_data['setting_to_change'] = data.replace("param_set_", "")
        await query.message.reply_text(f"Enter new value for `{context.user_data['setting_to_change']}`:")
    elif data.startswith("param_toggle_"):
        key = data.replace("param_toggle_", "")
        bot_data.settings[key] = not bot_data.settings.get(key, False)
        save_settings(); determine_active_preset()
        if "adaptive" in key or "strategy" in key: await ui_handlers.show_adaptive_intelligence_menu(update, context)
        else: await ui_handlers.show_parameters_menu(update, context)

async def post_init(application: Application):
    logger.info("Performing post-initialization for Wise Maestro Bot...")
    if not all([TELEGRAM_BOT_TOKEN, BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_CHAT_ID]):
        logger.critical("FATAL: Missing critical environment variables."); return
    try:
        bot_data.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await bot_data.redis_client.ping()
        logger.info("✅ Successfully connected to Redis server.")
    except Exception as e:
        logger.warning(f"Could not connect to Redis: {e}. Broadcasting disabled.")
        bot_data.redis_client = None

    bot_data.application = application
    bot_data.exchange = ccxt.binance({'apiKey': BINANCE_API_KEY, 'secret': BINANCE_API_SECRET, 'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
    try:
        await bot_data.exchange.load_markets()
    except Exception as e:
        logger.critical(f"🔥 FATAL: Could not connect to Binance: {e}"); return

    logger.info("Reconciling SPOT trading state with Binance exchange...")
    try:
        balance = await bot_data.exchange.fetch_balance()
        owned_assets = {asset for asset, data in balance.items() if isinstance(data, dict) and data.get('total', 0) > 0.00001}
        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            trades_in_db = await (await conn.execute("SELECT * FROM trades WHERE status = 'active'")).fetchall()
            for trade in trades_in_db:
                if trade['symbol'].split('/')[0] not in owned_assets:
                    logger.warning(f"Reconcile: Trade #{trade['id']} found active in DB, but asset not in wallet. Marking as 'Manually Closed'.")
                    await conn.execute("UPDATE trades SET status = 'مغلقة يدوياً' WHERE id = ?", (trade['id'],))
            await conn.commit()
        logger.info("State reconciliation complete.")
    except Exception as e:
        logger.error(f"Failed to reconcile state with exchange: {e}")
    
    load_settings()
    await init_database()

    bot_data.websocket_manager = WebSocketManager(bot_data.exchange, application)
    asyncio.create_task(bot_data.websocket_manager.run())
    await bot_data.websocket_manager.sync_subscriptions()
    
    bot_data.guardian = MaestroGuardian(bot_data.exchange, application, bot_data, DB_FILE)
    bot_data.smart_brain = EvolutionaryEngine(bot_data.exchange, application, DB_FILE)

    jq = application.job_queue
    jq.run_repeating(perform_scan, interval=SCAN_INTERVAL_SECONDS, first=10, name="perform_scan")
    jq.run_repeating(bot_data.guardian.the_supervisor_job, interval=SUPERVISOR_INTERVAL_SECONDS, first=30, name="supervisor_job")
    jq.run_repeating(bot_data.guardian.intelligent_reviewer_job, interval=3600, first=60, name="intelligent_reviewer")
    jq.run_repeating(bot_data.guardian.review_open_trades, interval=14400, first=120, name="wise_man_review")
    jq.run_repeating(bot_data.guardian.review_portfolio_risk, interval=86400, first=180, name="portfolio_risk_review")
    jq.run_repeating(maestro_job, interval=MAESTRO_INTERVAL_HOURS * 3600, first=5, name="maestro_job")
    jq.run_daily(bot_data.smart_brain.run_pattern_discovery, time=dt_time(hour=22, minute=0, tzinfo=EGYPT_TZ), name='pattern_discovery_job')
    
    logger.info(f"All jobs scheduled. Maestro is now fully active.")
    try: 
        await application.bot.send_message(TELEGRAM_CHAT_ID, "*🤖 Wise Maestro Bot (Final Fusion) - بدأ العمل...*", parse_mode=ParseMode.MARKDOWN)
    except Forbidden: 
        logger.critical(f"FATAL: Bot not authorized for chat ID {TELEGRAM_CHAT_ID}."); return
    logger.info("--- Wise Maestro Bot is now fully operational ---")

async def post_shutdown(application: Application):
    if bot_data.exchange: await bot_data.exchange.close()
    if bot_data.websocket_manager: await bot_data.websocket_manager.stop()
    if bot_data.redis_client: await bot_data.redis_client.close()
    logger.info("Bot has shut down gracefully.")

def main():
    logger.info("Starting Wise Maestro Bot...")
    app_builder = Application.builder().token(TELEGRAM_BOT_TOKEN)
    app_builder.post_init(post_init).post_shutdown(post_shutdown)
    application = app_builder.build()
    
    application.bot_data = bot_data
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, universal_text_handler))
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    application.run_polling()

if __name__ == '__main__':
    main()
