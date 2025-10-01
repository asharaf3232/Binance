# -*- coding: utf-8 -*-
import copy
from collections import defaultdict

# =======================================================================================
# --- ⚙️ Core Configuration Defaults ⚙️ ---
# =======================================================================================

TIMEFRAME = '15m'
SCAN_INTERVAL_SECONDS = 900
SUPERVISOR_INTERVAL_SECONDS = 180
TIME_SYNC_INTERVAL_SECONDS = 3600
STRATEGY_ANALYSIS_INTERVAL_SECONDS = 21600 # 6 hours
MAESTRO_INTERVAL_HOURS = 1 # 1 hour for market regime check

# --- إعدادات البوت الافتراضية ---
DEFAULT_SETTINGS = {
    "real_trade_size_usdt": 15.0,
    "max_concurrent_trades": 5,
    "top_n_symbols_by_volume": 300,
    "worker_threads": 10,
    "atr_sl_multiplier": 2.5,
    "risk_reward_ratio": 2.0,
    # Trailing Stop
    "trailing_sl_enabled": True,
    "trailing_sl_activation_percent": 2.0,
    "trailing_sl_callback_percent": 1.5,
    # Strategy & Scanners
    "active_scanners": ["momentum_breakout", "breakout_squeeze_pro", "support_rebound", "sniper_pro", "whale_radar", "rsi_divergence", "supertrend_pullback", "bollinger_reversal"],
    # Filters
    "market_mood_filter_enabled": True,
    "fear_and_greed_threshold": 30,
    "adx_filter_enabled": True,
    "adx_filter_level": 25,
    "btc_trend_filter_enabled": True,
    "news_filter_enabled": True,
    "asset_blacklist": ["USDC", "DAI", "TUSD", "FDUSD", "USDD", "PYUSD", "USDT", "BNB", "BTC", "ETH", "OKB"],
    "liquidity_filters": {"min_quote_volume_24h_usd": 1000000, "min_rvol": 1.5},
    "volatility_filters": {"atr_period_for_filter": 14, "min_atr_percent": 0.8},
    "trend_filters": {"ema_period": 200, "htf_period": 50, "enabled": True},
    "spread_filter": {"max_spread_percent": 0.5},
    "volume_filter_multiplier": 2.0,
    "whale_radar_threshold_usd": 30000.0, # <-- السطر الجديد
    # Closure & Notifications
    "close_retries": 3,
    "incremental_notifications_enabled": True,
    "incremental_notification_percent": 2.0,
    # Adaptive Intelligence
    "adaptive_intelligence_enabled": True,
    "dynamic_trade_sizing_enabled": True,
    "strategy_proposal_enabled": True,
    "strategy_analysis_min_trades": 10,
    "strategy_deactivation_threshold_wr": 45.0,
    "dynamic_sizing_max_increase_pct": 25.0,
    "dynamic_sizing_max_decrease_pct": 50.0,
    # Maestro & Reviewer
    "intelligent_reviewer_enabled": True, 
    "momentum_scalp_mode_enabled": False, 
    "momentum_scalp_target_percent": 0.5,
    "multi_timeframe_confluence_enabled": True, 
    "maestro_mode_enabled": True, 
}

# --- الأسماء العربية للاستراتيجيات ---
STRATEGY_NAMES_AR = {
    "momentum_breakout": "زخم اختراقي", "breakout_squeeze_pro": "اختراق انضغاطي",
    "support_rebound": "ارتداد الدعم", "sniper_pro": "القناص المحترف", "whale_radar": "رادار الحيتان",
    "rsi_divergence": "دايفرجنس RSI", "supertrend_pullback": "انعكاس سوبرترند",
    "bollinger_reversal": "انعكاس بولينجر"
}

# --- مصفوفة قرارات المايسترو (Maestro Decision Matrix) ---
DECISION_MATRIX = {
    "TRENDING_HIGH_VOLATILITY": {"risk_reward_ratio": 1.5, "volume_filter_multiplier": 2.5},
    "TRENDING_LOW_VOLATILITY": {"risk_reward_ratio": 2.5, "volume_filter_multiplier": 1.5},
    "SIDEWAYS_HIGH_VOLATILITY": {"risk_reward_ratio": 2.0, "volume_filter_multiplier": 2.0},
    "SIDEWAYS_LOW_VOLATILITY": {"risk_reward_ratio": 3.0, "volume_filter_multiplier": 1.0}
}

# --- قوالب الإعدادات الجاهزة (Presets) ---
SETTINGS_PRESETS = {
    "professional": copy.deepcopy(DEFAULT_SETTINGS),
    "strict": {**copy.deepcopy(DEFAULT_SETTINGS), "max_concurrent_trades": 3, "risk_reward_ratio": 2.5, "fear_and_greed_threshold": 40, "adx_filter_level": 28},
    "lenient": {**copy.deepcopy(DEFAULT_SETTINGS), "max_concurrent_trades": 8, "risk_reward_ratio": 1.8, "fear_and_greed_threshold": 25, "adx_filter_level": 20},
}

# --- قواعد إدارة مخاطر المحفظة (للرجل الحكيم) ---
PORTFOLIO_RISK_RULES = {
    "max_asset_concentration_pct": 30.0,
    "max_sector_concentration_pct": 50.0,
}

# --- تصنيف العملات حسب القطاع (للرجل الحكيم) ---
SECTOR_MAP = {
    'RNDR': 'AI', 'FET': 'AI', 'AGIX': 'AI', 'NEAR': 'AI',
    'UNI': 'DeFi', 'AAVE': 'DeFi', 'LDO': 'DeFi', 'MKR': 'DeFi',
    'SOL': 'Layer 1', 'AVAX': 'Layer 1', 'ADA': 'Layer 1',
    'DOGE': 'Memecoin', 'PEPE': 'Memecoin', 'SHIB': 'Memecoin',
    'LINK': 'Oracle', 'BAND': 'Oracle',
}
