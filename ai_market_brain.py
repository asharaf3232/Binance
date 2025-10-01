# -*- coding: utf-8 -*-
import logging
import asyncio
import json
import time
import httpx
import pandas as pd
import pandas_ta as ta
import feedparser
from collections import defaultdict
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta, time as dt_time

# --- افتراض توافر المكتبات ---
try:
    import nltk
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

# --- الثوابت ---
EGYPT_TZ = ZoneInfo("Africa/Cairo")
logger = logging.getLogger(__name__)

# =======================================================================================
# --- A. وظائف تحليل مزاج السوق العام (Fundamental & Sentiment) ---
# =======================================================================================

def analyze_sentiment_of_headlines(headlines):
    """يحلل مشاعر العناوين الإخبارية."""
    if not headlines or not NLTK_AVAILABLE: return "N/A", 0.0
    sia = SentimentIntensityAnalyzer()
    score = sum(sia.polarity_scores(h)['compound'] for h in headlines) / len(headlines)
    if score > 0.15: mood = "إيجابية"
    elif score < -0.15: mood = "سلبية"
    else: mood = "محايدة"
    return mood, score

def get_latest_crypto_news(limit=15):
    """يجلب آخر عناوين أخبار العملات المشفرة."""
    urls = ["https://cointelegraph.com/rss", "https://www.coindesk.com/arc/outboundfeeds/rss/"]
    headlines = [entry.title for url in urls for entry in feedparser.parse(url).entries[:7]]
    return list(set(headlines))[:limit]

async def get_alpha_vantage_economic_events(api_key: str):
    """يجلب الأحداث الاقتصادية عالية التأثير اليوم (يتطلب مفتاح)."""
    if not api_key or api_key == 'YOUR_AV_KEY_HERE': return []
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    params = {'function': 'ECONOMIC_CALENDAR', 'horizon': '3month', 'apikey': api_key}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get('https://www.alphavantage.co/query', params=params)
            response.raise_for_status()
            data_str = response.text
            if "premium" in data_str.lower(): return []
            lines = data_str.strip().split('\r\n')
            if len(lines) < 2: return []
            header = [h.strip() for h in lines[0].split(',')]
            events = [dict(zip(header, [v.strip() for v in line.split(',')])) for line in lines[1:]]
            
            high_impact_events = [e.get('event', 'Unknown Event') for e in events if e.get('releaseDate', '') == today_str and e.get('impact', '').lower() == 'high' and e.get('country', '') in ['USD', 'EUR']]
            return high_impact_events
    except Exception as e:
        logger.error(f"Failed to fetch economic calendar: {e}")
        return None

async def get_fear_and_greed_index():
    """يجلب مؤشر الخوف والطمع."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get("https://api.alternative.me/fng/?limit=1", timeout=10)
            return int(r.json()['data'][0]['value'])
    except Exception: return None

# =======================================================================================
# --- B. وظيفة المايسترو: تحليل نمط السوق (Market Regime Analysis) ---
# =======================================================================================

def find_col(df_columns, prefix):
    """يجد اسم العمود الذي يبدأ ببادئة معينة (لمشاكل Pandas-TA)."""
    try: return next(col for col in df_columns if col.startswith(prefix))
    except StopIteration: return None


async def get_market_regime(exchange):
    """
    يحلل نمط السوق الحالي لـ BTC بناءً على الاتجاه والتقلب.
    الأنماط: TRENDING_HIGH_VOLATILITY, SIDEWAYS_LOW_VOLATILITY, إلخ.
    """
    try:
        # نحتاج إلى 1h بيانات لـ BTC
        ohlcv = await exchange.fetch_ohlcv('BTC/USDT', '1h', limit=100)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # 1. تحليل الاتجاه (Trend - ADX)
        df.ta.adx(append=True)
        adx_col = find_col(df.columns, "ADX_14")
        adx = df[adx_col].iloc[-1] if adx_col and pd.notna(df[adx_col].iloc[-1]) else 0
        
        if adx > 25:
            trend = "TRENDING"
        else:
            trend = "SIDEWAYS"
            
        # 2. تحليل التقلب (Volatility - ATR%)
        df.ta.atr(append=True)
        atr_col = find_col(df.columns, "ATRr_14")
        current_close = df['close'].iloc[-1]
        
        if atr_col and current_close > 0:
            atr = df[atr_col].iloc[-1]
            atr_percent = (atr / current_close) * 100
        else:
            atr_percent = 0.0

        # تعريف مستويات التقلب
        if atr_percent > 1.5: # إذا كان ATR% > 1.5% يعتبر تقلب عالي
            vol = "HIGH_VOLATILITY"
        else:
            vol = "LOW_VOLATILITY"
            
        regime = f"{trend}_{vol}"
        logger.info(f"Maestro Regime Analysis: {regime} (ADX: {adx:.1f}, ATR%: {atr_percent:.2f}%)")
        return regime
        
    except Exception as e:
        logger.error(f"Market Regime Analysis failed: {e}")
        return "UNKNOWN"
