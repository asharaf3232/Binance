# -*- coding: utf-8 -*-
import pandas as pd
import pandas_ta as ta
import asyncio
import logging
from scipy.signal import find_peaks

# يتم استيرادها من الملف المشترك الأول
from _settings_config import TIMEFRAME

logger = logging.getLogger(__name__)

# --- وظيفة مساعدة ---
def find_col(df_columns, prefix):
    """يجد اسم العمود الذي يبدأ ببادئة معينة (لمشاكل Pandas-TA)."""
    try: return next(col for col in df_columns if col.startswith(prefix))
    except StopIteration: return None
# --- نهاية وظيفة مساعدة ---


# =======================================================================================
# --- A. الماسحات الأساسية (Core Scanners) ---
# =======================================================================================

def analyze_momentum_breakout(df, params, rvol, adx_value):
    """اختراق الزخم: تقاطع MACD صاعد، اختراق BB-Upper، وفوق VWAP."""
    df.ta.vwap(append=True); df.ta.bbands(length=20, append=True); df.ta.macd(append=True); df.ta.rsi(append=True)
    last, prev = df.iloc[-2], df.iloc[-3]
    macd_col, macds_col, bbu_col, rsi_col = find_col(df.columns, "MACD_"), find_col(df.columns, "MACDs_"), find_col(df.columns, "BBU_"), find_col(df.columns, "RSI_")
    if not all([macd_col, macds_col, bbu_col, rsi_col]): return None
    if (prev[macd_col] <= prev[macds_col] and last[macd_col] > last[macds_col] and last['close'] > last[bbu_col] and last['close'] > last["VWAP_D"] and last[rsi_col] < 68):
        return {"reason": "momentum_breakout"}
    return None

def analyze_breakout_squeeze_pro(df, params, rvol, adx_value):
    """اختراق الانضغاط: خروج من انضغاط Bollinger/Keltner مع زخم حجمي."""
    df.ta.bbands(length=20, append=True); df.ta.kc(length=20, scalar=1.5, append=True); df.ta.obv(append=True)
    bbu_col, bbl_col, kcu_col, kcl_col = find_col(df.columns, "BBU_"), find_col(df.columns, "BBL_"), find_col(df.columns, "KCUe_"), find_col(df.columns, "KCLEe_")
    if not all([bbu_col, bbl_col, kcu_col, kcl_col]): return None
    last, prev = df.iloc[-2], df.iloc[-3]
    is_in_squeeze = prev[bbl_col] > prev[kcl_col] and prev[bbu_col] < prev[kcu_col]
    if is_in_squeeze and (last['close'] > last[bbu_col]) and (last['volume'] > df['volume'].rolling(20).mean().iloc[-2] * 1.5) and (df['OBV'].iloc[-2] > df['OBV'].iloc[-3]):
        return {"reason": "breakout_squeeze_pro"}
    return None

async def analyze_support_rebound(df, params, rvol, adx_value, exchange, symbol):
    """ارتداد الدعم: ارتداد من منطقة دعم ساعية على الإطار الزمني الأصغر (يتطلب بيانات إضافية)."""
    try:
        # نحتاج إلى بيانات إطار زمني أكبر لتحديد الدعم
        ohlcv_1h = await exchange.fetch_ohlcv(symbol, '1h', limit=100)
        if len(ohlcv_1h) < 50: return None
        df_1h = pd.DataFrame(ohlcv_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        current_price = df_1h['close'].iloc[-1]
        
        # تحديد أقرب دعم رئيسي
        recent_lows = df_1h['low'].rolling(window=20, center=True).min()
        supports = recent_lows[recent_lows.notna()]
        closest_support = max([s for s in supports if s < current_price], default=None)
        
        # يجب أن يكون الارتداد قريبًا من الدعم (< 1%)
        if not closest_support or ((current_price - closest_support) / closest_support * 100 > 1.0): return None
        
        last_candle_15m = df.iloc[-2]
        # شمعة ارتداد قوية على إطار الـ 15 دقيقة
        if last_candle_15m['close'] > last_candle_15m['open'] and last_candle_15m['volume'] > df['volume'].rolling(window=20).mean().iloc[-2] * 1.5:
            return {"reason": "support_rebound"}
    except Exception: return None
    return None

def analyze_sniper_pro(df, params, rvol, adx_value):
    """القناص المحترف: اختراق بعد انضغاط سعري حاد (تذبذب منخفض)."""
    try:
        compression_candles = 24
        if len(df) < compression_candles + 2: return None
        compression_df = df.iloc[-compression_candles-1:-1]
        highest_high, lowest_low = compression_df['high'].max(), compression_df['low'].min()
        if lowest_low <= 0: return None
        volatility = (highest_high - lowest_low) / lowest_low * 100
        
        if volatility < 12.0: # تحقق من انضغاط التذبذب
            last_candle = df.iloc[-2]
            # اختراق مع حجم مرتفع
            if last_candle['close'] > highest_high and last_candle['volume'] > compression_df['volume'].mean() * 2:
                return {"reason": "sniper_pro"}
    except Exception: return None
    return None

def analyze_rsi_divergence(df, params, rvol, adx_value):
    """دايفرجنس RSI: دايفرجنس صاعد مع اختراق تأكيدي (يتطلب scipy)."""
    # نفترض أن scipy متاح.
    df.ta.rsi(length=14, append=True)
    rsi_col = find_col(df.columns, f"RSI_14")
    if not rsi_col or df[rsi_col].isnull().all(): return None
    
    subset = df.iloc[-35:].copy()
    
    # تحديد القيعان السعرية وقيعان RSI
    price_troughs_idx, _ = find_peaks(-subset['low'], distance=5)
    rsi_troughs_idx, _ = find_peaks(-subset[rsi_col], distance=5)
    
    if len(price_troughs_idx) >= 2 and len(rsi_troughs_idx) >= 2:
        p_low1_idx, p_low2_idx = price_troughs_idx[-2], price_troughs_idx[-1]
        r_low1_idx, r_low2_idx = rsi_troughs_idx[-2], rsi_troughs_idx[-1]
        
        # تحقق من الدايفرجنس الصاعد (السعر قاع أدنى، RSI قاع أعلى)
        is_divergence = (subset.iloc[p_low2_idx]['low'] < subset.iloc[p_low1_idx]['low'] and subset.iloc[r_low2_idx][rsi_col] > subset.iloc[r_low1_idx][rsi_col])
        
        if is_divergence:
            # التحقق من التأكيد السعري (كسر آخر قمة داخل الدايفرجنس)
            confirmation_price = subset.iloc[p_low2_idx:]['high'].max()
            price_confirmed = df.iloc[-2]['close'] > confirmation_price
            
            if price_confirmed:
                return {"reason": "rsi_divergence"}
    return None

def analyze_supertrend_pullback(df, params, rvol, adx_value):
    """انعكاس سوبرترند: تغير اتجاه Supertrend من هابط إلى صاعد مع اختراق سوينغ هاي."""
    df.ta.supertrend(length=10, multiplier=3.0, append=True)
    st_dir_col = find_col(df.columns, f"SUPERTd_10_3.0")
    
    if not st_dir_col: return None
    last, prev = df.iloc[-2], df.iloc[-3]
    
    # تغير من هابط (-1) إلى صاعد (1)
    if prev[st_dir_col] == -1 and last[st_dir_col] == 1:
        recent_swing_high = df['high'].iloc[-10:-2].max() # آخر 10 شمعات باستثناء آخر شمعتين
        # التأكيد بالاختراق
        if last['close'] > recent_swing_high:
            return {"reason": "supertrend_pullback"}
    return None

# --- [جديد] استراتيجية الانعكاس (Reversal Strategy) ---
def analyze_bollinger_reversal(df, params, rvol, adx_value):
    """انعكاس بولينجر: إغلاق شمعة تحت BB ثم إغلاق التالية فوقها (ارتداد)."""
    df.ta.bbands(length=20, append=True)
    df.ta.rsi(append=True)
    
    bbl_col, bbm_col = find_col(df.columns, "BBL_20_2.0"), find_col(df.columns, "BBM_20_2.0")
    rsi_col = find_col(df.columns, "RSI_14")
    
    if not all([bbl_col, bbm_col, rsi_col]): return None
    last, prev = df.iloc[-2], df.iloc[-3]
    
    # تحقق: الشمعة السابقة أغلقت تحت Lower Band (تجاوز بيعي)، والشمعة الحالية ارتدت وأغلقت فوقه (نقطة دخول)
    if prev['close'] < prev[bbl_col] and last['close'] > last[bbl_col] and last['close'] < last[bbm_col] and last[rsi_col] < 35:
        return {"reason": "bollinger_reversal"}
    return None


# =======================================================================================
# --- B. فلاتر التوافق والعمق (Confluence & Depth Filters) ---
# =======================================================================================

async def filter_whale_radar(exchange, symbol):
    """رادار الحيتان: يبحث عن أوامر شراء كبيرة في عمق السوق (Order Book)."""
    try:
        ob = await exchange.fetch_order_book(symbol, limit=20)
        # البحث عن طلبات شراء كبيرة (كمثال: أكثر من 30 ألف دولار في أفضل 10 طلبات)
        if not ob or not ob.get('bids'): return False
        
        # حساب إجمالي قيمة أفضل 10 عروض شراء
        bids_value = sum(float(price) * float(qty) for price, qty in ob['bids'][:10])
        
        if bids_value > 30000:
            return True
    except Exception: return False
    return False

# =======================================================================================
# --- C. دالة التجميع (Main Dictionary) ---
# =======================================================================================

SCANNERS = {
    "momentum_breakout": analyze_momentum_breakout,
    "breakout_squeeze_pro": analyze_breakout_squeeze_pro,
    "support_rebound": analyze_support_rebound, # يتطلب exchange, symbol
    "sniper_pro": analyze_sniper_pro,
    # "whale_radar" يُفحص بشكل منفصل لتقليل الحمل الحسابي
    "rsi_divergence": analyze_rsi_divergence,
    "supertrend_pullback": analyze_supertrend_pullback,
    "bollinger_reversal": analyze_bollinger_reversal,
}
