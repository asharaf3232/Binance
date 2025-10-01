# -*- coding: utf-8 -*-
# وحدة العقل التطوري - مسؤولة عن توثيق الصفقات وتحليل "ماذا لو؟" واكتشاف الأنماط.

import logging
import aiosqlite
import asyncio
import json
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
from telegram.ext import Application

logger = logging.getLogger(__name__)
ANALYSIS_PERIOD_CANDLES = 24 # عدد الشموع التي سيتم تحليلها بعد الخروج (24 شمعة * 15 دقيقة = 6 ساعات)

class EvolutionaryEngine:
    def __init__(self, exchange: ccxt.Exchange, application: Application, db_file: str):
        self.exchange = exchange
        self.application = application
        self.DB_FILE = db_file
        self.telegram_chat_id = application.bot_data.get('TELEGRAM_CHAT_ID')
        logger.info("🧬 Advanced Evolutionary Engine Initialized (with What-If Analysis).")
        asyncio.create_task(self._init_journal_table())

    async def _init_journal_table(self):
        """ينشئ أو يعدل جدول Journaling إذا لزم الأمر."""
        try:
            async with aiosqlite.connect(self.DB_FILE) as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS trade_journal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id INTEGER UNIQUE,
                        entry_timestamp TEXT,
                        entry_strategy TEXT,
                        entry_indicators_snapshot TEXT,
                        exit_reason TEXT,
                        final_pnl REAL,
                        exit_quality_score INTEGER,
                        post_exit_performance TEXT,
                        notes TEXT
                    )
                ''')
                # التأكد من وجود كل الأعمدة
                cursor = await conn.execute("PRAGMA table_info(trade_journal)")
                columns = [row[1] for row in await cursor.fetchall()]
                if 'exit_quality_score' not in columns:
                    await conn.execute("ALTER TABLE trade_journal ADD COLUMN exit_quality_score INTEGER")
                if 'post_exit_performance' not in columns:
                    await conn.execute("ALTER TABLE trade_journal ADD COLUMN post_exit_performance TEXT")
                if 'notes' not in columns:
                    await conn.execute("ALTER TABLE trade_journal ADD COLUMN notes TEXT")

                await conn.commit()
            logger.info("Journal table verified for What-If analysis.")
        except Exception as e:
            logger.error(f"Failed to initialize trade_journal table: {e}")

    async def _capture_market_snapshot(self, symbol: str) -> dict:
        """يلتقط لقطة للمؤشرات الرئيسية عند نقطة معينة."""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, '15m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            rsi = ta.rsi(df['close'], length=14).iloc[-1]
            adx_data = ta.adx(df['high'], df['low'], df['close'])
            adx = adx_data['ADX_14'].iloc[-1] if adx_data is not None and not adx_data.empty else None
            
            snapshot = {
                "rsi_14": round(rsi, 2) if pd.notna(rsi) else "N/A", 
                "adx_14": round(adx, 2) if pd.notna(adx) else "N/A"
            }
            return snapshot
        except Exception as e:
            logger.error(f"Smart Engine: Could not capture snapshot for {symbol}: {e}")
            return {}

    async def add_trade_to_journal(self, trade_details: dict):
        """يوثق تفاصيل الصفقة المغلقة ويبدأ تحليل 'ماذا لو؟'."""
        trade_id, symbol = trade_details.get('id'), trade_details.get('symbol')
        if not trade_id or not symbol or trade_details.get('pnl_usdt') is None: return

        logger.info(f"🧬 Journaling closed trade #{trade_id} for {symbol}...")
        try:
            snapshot = await self._capture_market_snapshot(symbol)
            
            async with aiosqlite.connect(self.DB_FILE) as conn:
                await conn.execute(
                    "INSERT OR IGNORE INTO trade_journal (trade_id, entry_timestamp, entry_strategy, exit_reason, final_pnl, entry_indicators_snapshot) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        trade_id, 
                        trade_details.get('timestamp'),
                        trade_details.get('reason'), 
                        trade_details.get('status'),
                        trade_details.get('pnl_usdt'),
                        json.dumps(snapshot)
                    )
                )
                await conn.commit()
            asyncio.create_task(self._perform_what_if_analysis(trade_details))
        except Exception as e:
            logger.error(f"Smart Engine: Failed to journal trade #{trade_id}: {e}", exc_info=True)

    async def _perform_what_if_analysis(self, trade_details: dict):
        """يحلل سلوك العملة بعد الخروج لتقييم جودة القرار."""
        trade_id = trade_details.get('id')
        symbol = trade_details.get('symbol')
        exit_reason = trade_details.get('status', '')
        original_tp = trade_details.get('take_profit')
        original_sl = trade_details.get('stop_loss')
        entry_price = trade_details.get('entry_price')
        
        await asyncio.sleep(60) # انتظار دقيقة قبل بدء التحليل
        logger.info(f"🔬 Smart Engine: Performing 'What-If' analysis for closed trade #{trade_id} ({symbol})...")
        try:
            future_ohlcv = await self.exchange.fetch_ohlcv(symbol, '15m', limit=ANALYSIS_PERIOD_CANDLES)
            df_future = pd.DataFrame(future_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            highest_price_after = df_future['high'].max()
            lowest_price_after = df_future['low'].min()
            
            score = 0
            notes = ""

            if 'SL' in exit_reason or 'فاشلة' in exit_reason:
                if highest_price_after >= entry_price * 1.01: # إذا ارتد السعر 1% فوق سعر الدخول
                    score = -5
                    notes = f"Stop Loss Regret: Price recovered and reached {highest_price_after}."
                else:
                    score = 10
                    notes = f"Good Save: Price continued to drop to {lowest_price_after} after SL."
            elif 'TP' in exit_reason or 'ناجحة' in exit_reason:
                missed_profit_pct = ((highest_price_after / original_tp) - 1) * 100 if original_tp > 0 else 0
                if missed_profit_pct > 5.0: # إذا كان الربح الفائت أكثر من 5%
                    score = -5
                    notes = f"Missed Opportunity: Price rallied an additional {missed_profit_pct:.2f}% after TP."
                elif missed_profit_pct > 1.0:
                    score = 5
                    notes = "Good Exit: Price rallied slightly more."
                else:
                    score = 10
                    notes = "Perfect Exit: Price dropped or stalled after TP."
            
            post_performance_data = {
                "highest_price_after": highest_price_after,
                "lowest_price_after": lowest_price_after,
                "analysis_period_hours": (ANALYSIS_PERIOD_CANDLES * 15) / 60
            }
            
            async with aiosqlite.connect(self.DB_FILE) as conn:
                await conn.execute(
                    "UPDATE trade_journal SET exit_quality_score = ?, post_exit_performance = ?, notes = ? WHERE trade_id = ?",
                    (score, json.dumps(post_performance_data), notes, trade_id)
                )
                await conn.commit()
            logger.info(f"🔬 Analysis complete for trade #{trade_id}. Exit Quality Score: {score}. Notes: {notes}")
        except Exception as e:
            logger.error(f"Smart Engine: 'What-If' analysis failed for trade #{trade_id}: {e}", exc_info=True)

    async def run_pattern_discovery(self, context: object = None):
        """
        تقوم بتحليل كل البيانات في السجل لاكتشاف أنماط وتقديم تقرير.
        """
        logger.info("🧬 Evolutionary Engine: Starting pattern discovery analysis...")
        report_lines = ["🧠 **تقرير الذكاء الاستراتيجي** 🧠\n"]
        try:
            async with aiosqlite.connect(self.DB_FILE) as conn:
                journal_df = pd.read_sql_query("SELECT * FROM trade_journal WHERE notes IS NOT NULL", conn)
                trades_df = pd.read_sql_query("SELECT id, symbol, pnl_usdt FROM trades", conn)
                
                if journal_df.empty or len(journal_df) < 5:
                    logger.info("🧬 Pattern Discovery: Not enough journaled trades to create a report.")
                    return

                full_df = pd.merge(journal_df, trades_df, left_on='trade_id', right_on='id')

            # التحليل 1: أداء الاستراتيجيات بناءً على جودة القرار
            strategy_quality = full_df.groupby('entry_strategy')['exit_quality_score'].mean().sort_values(ascending=False)
            report_lines.append("--- **أداء الاستراتيجيات (حسب جودة الخروج)** ---")
            for strategy, score in strategy_quality.items():
                if strategy:
                    report_lines.append(f"- `{strategy.split(' (')[0]}`: متوسط درجة الجودة **{score:+.2f}**")

            # التحليل 2: الأسباب الأكثر شيوعًا للندم أو النجاح
            common_notes = full_df['notes'].value_counts().nlargest(3)
            report_lines.append("\n--- **أهم الملاحظات المتكررة من 'ماذا لو؟'** ---")
            for note, count in common_notes.items():
                if note:
                    report_lines.append(f"- '{note}' (تكررت **{count}** مرات)")

            # إرسال التقرير النهائي إلى تليجرام
            final_report = "\n".join(report_lines)
            await self.application.bot.send_message(self.telegram_chat_id, final_report, parse_mode='Markdown')
            logger.info("🧬 Pattern Discovery analysis complete and report sent.")

        except Exception as e:
            logger.error(f"Smart Engine: Pattern discovery failed: {e}", exc_info=True)
