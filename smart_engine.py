# -*- coding: utf-8 -*-
# وحدة العقل التطوري - مسؤولة عن توثيق الصفقات لبناء ذاكرة الأداء (Journaling).

import logging
import aiosqlite
import asyncio
import json
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt

logger = logging.getLogger(__name__)
# يتم تعيين DB_FILE فعليًا في الملف الرئيسي (binance_maestro) عبر الحارس

class EvolutionaryEngine:
    def __init__(self, exchange: ccxt.Exchange, db_file: str):
        self.exchange = exchange
        self.DB_FILE = db_file
        logger.info("🧬 Evolutionary Engine Initialized. Ready to build memory.")
        # نضمن إنشاء جدول trade_journal
        asyncio.create_task(self._init_journal_table())

    async def _init_journal_table(self):
        """ينشئ جدول Journaling إذا لم يكن موجودًا."""
        try:
            async with aiosqlite.connect(self.DB_FILE) as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS trade_journal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id INTEGER,
                        entry_timestamp TEXT,
                        entry_strategy TEXT,
                        entry_indicators_snapshot TEXT,
                        exit_reason TEXT,
                        final_pnl REAL
                    )
                ''')
                await conn.commit()
            logger.info("Journal table verified.")
        except Exception as e:
            logger.error(f"Failed to initialize trade_journal table: {e}")

    async def _capture_market_snapshot(self, symbol: str) -> dict:
        """يلتقط لقطة للمؤشرات الرئيسية عند نقطة الإغلاق/الدخول."""
        try:
            # نستخدم 15m كما هو محدد في إعدادات البوت
            ohlcv = await self.exchange.fetch_ohlcv(symbol, '15m', limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # نستخدم وظيفة find_col هنا، ولكن بما أنها في ملف مشترك، سنحتاج لتعريفها محليًا أو افتراض استيرادها
            def find_col(df_columns, prefix):
                try: return next(col for col in df_columns if col.startswith(prefix))
                except StopIteration: return None
            
            df.ta.rsi(length=14, append=True)
            df.ta.adx(append=True)

            rsi_col = find_col(df.columns, "RSI_14")
            adx_col = find_col(df.columns, "ADX_14")
            
            last_rsi = df[rsi_col].iloc[-1] if rsi_col and pd.notna(df[rsi_col].iloc[-1]) else None
            last_adx = df[adx_col].iloc[-1] if adx_col and pd.notna(df[adx_col].iloc[-1]) else None
            
            snapshot = {
                "rsi_14": round(last_rsi, 2) if last_rsi is not None else "N/A", 
                "adx_14": round(last_adx, 2) if last_adx is not None else "N/A"
            }
            return snapshot
        except Exception as e:
            logger.error(f"Smart Engine: Could not capture snapshot for {symbol}: {e}")
            return {}

    async def add_trade_to_journal(self, trade_details: dict):
        """يوثق تفاصيل الصفقة المغلقة في جدول Journaling."""
        trade_id, symbol = trade_details.get('id'), trade_details.get('symbol')
        if not trade_id or not symbol: return
        
        # التأكد من أن الصفقة مغلقة (لديها PNL)
        if trade_details.get('pnl_usdt') is None: return

        logger.info(f"🧬 Journaling closed trade #{trade_id} for {symbol}...")
        try:
            # هنا نلتقط اللقطة عند الإغلاق
            snapshot = await self._capture_market_snapshot(symbol)
            
            async with aiosqlite.connect(self.DB_FILE) as conn:
                await conn.execute(
                    "INSERT INTO trade_journal (trade_id, entry_timestamp, entry_strategy, exit_reason, final_pnl, entry_indicators_snapshot) VALUES (?, ?, ?, ?, ?, ?)",
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
            logger.info(f"Successfully journaled trade #{trade_id}.")
        except Exception as e:
            logger.error(f"Smart Engine: Failed to journal trade #{trade_id}: {e}", exc_info=True)
