"""
Telegram Bot Service — Dashboard-Style Notifications v2.0
Per-desk channel routing with System channel for health/risk alerts.

Routing:
  - Trade entries/exits/skips/OniAI -> desk-specific channel
  - Entry/exit summaries, daily briefs, weekly memos -> Portfolio channel
  - Drawdown, kill switch, health, diagnostics -> System channel
  - Firm drawdown, kill switch -> ALL channels
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_DESK_CHANNELS,
    TELEGRAM_PORTFOLIO_CHAT,
    TELEGRAM_SYSTEM_CHAT,
    DESKS,
    get_pip_info,
)

logger = logging.getLogger("TradingSystem.Telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}"


def _sanitize_text(text: str) -> str:
    """Strip angle brackets that Telegram may interpret as HTML tags.
    Replaces < and > with Unicode look-alikes so messages always render
    as plain text, even if the bot has a default parse_mode set via BotFather.
    """
    import re
    # Replace HTML-like tags (e.g. <class 'Foo'>, <Response [400]>) with cleaned text
    text = re.sub(r'<([^>]*)>', r'[\1]', text)
    return text

BAR = "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
SCORE_FILLED = "\U0001f7ea"
SCORE_EMPTY = "\u2b1c"

DESK_EMOJI = {
    "DESK1_SCALPER": "\U0001f7e2", "DESK2_INTRADAY": "\U0001f7e1",
    "DESK3_SWING": "\U0001f535", "DESK4_GOLD": "\U0001f534",
    "DESK5_ALTS": "\u26ab", "DESK6_EQUITIES": "\u26aa",
}
DESK_LABEL = {
    "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
    "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
    "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
}

TF_MAP = {
    "1": "1M", "5": "5M", "15": "15M", "30": "30M",
    "60": "1H", "240": "4H", "1440": "D",
    "M1": "1M", "M5": "5M", "M15": "15M", "M30": "30M",
    "H1": "1H", "H4": "4H", "D1": "D", "W1": "W",
    "1M": "1M", "5M": "5M", "15M": "15M", "30M": "30M",
    "1H": "1H", "4H": "4H", "D": "D", "W": "W",
}

TREND_EMOJI = {
    "STRONG_UP": "\U0001f7e2\U0001f4c8", "UP": "\U0001f7e2\u2197\ufe0f",
    "WEAK_UP": "\U0001f7e1\u2197\ufe0f",
    "STRONG_DOWN": "\U0001f534\U0001f4c9", "DOWN": "\U0001f534\u2198\ufe0f",
    "WEAK_DOWN": "\U0001f7e1\u2198\ufe0f",
    "RANGING": "\u26aa\u2194\ufe0f", "UNKNOWN": "\u2753",
}


def _score_bar(score, max_score=10):
    filled = min(int(float(score) if score else 0), max_score)
    return SCORE_FILLED * filled + SCORE_EMPTY * (max_score - filled)


def _score_label(score):
    s = int(float(score)) if score else 0
    if s >= 8: return "ELITE"
    if s >= 7: return "STRONG"
    if s >= 5: return "MODERATE"
    return "WEAK"


def _pnl_emoji(pnl): return "\u2705" if pnl >= 0 else "\u274c"
def _desk_dot(pnl): return "\U0001f7e2" if pnl > 0 else ("\U0001f534" if pnl < 0 else "\u26aa")
def _format_tf(tf): return TF_MAP.get(str(tf).strip().upper(), str(tf))


def _gold_sub(desk, timeframe):
    if desk != "DESK4_GOLD": return ""
    tf = str(timeframe).upper()
    if tf in ["5", "5M", "M5"]: return " \u26a1 SCALP"
    if tf in ["60", "1H", "H1"]: return " \U0001f4c8 INTRA"
    if tf in ["240", "4H", "H4"]: return " \U0001f3db SWING"
    return ""


def _trend_align(direction, trend):
    is_long = direction.upper() in ["LONG", "BUY"]
    up = trend in ["STRONG_UP", "UP", "WEAK_UP"]
    down = trend in ["STRONG_DOWN", "DOWN", "WEAK_DOWN"]
    if (is_long and up) or (not is_long and down): return "\u2705 WITH TREND"
    if (is_long and down) or (not is_long and up): return "\u26a0\ufe0f COUNTER-TREND"
    return "\u2194\ufe0f NEUTRAL"


def _conf_bar(confidence):
    pct = float(confidence) * 100 if confidence else 0
    filled = int(pct / 10)
    return "\u2593" * filled + "\u2591" * (10 - filled) + f" {pct:.0f}%"


class TelegramBot:
    def __init__(self):
        self.token = TELEGRAM_BOT_TOKEN
        self.portfolio_chat = TELEGRAM_PORTFOLIO_CHAT
        self.system_chat = TELEGRAM_SYSTEM_CHAT
        self.desk_channels = TELEGRAM_DESK_CHANNELS
        self.fallback_chat = TELEGRAM_CHAT_ID
        self.client = httpx.AsyncClient(timeout=10.0)
        self.enabled = bool(self.token)
        if not self.enabled:
            logger.warning("Telegram not configured. Set TELEGRAM_BOT_TOKEN.")
        else:
            logger.info(f"Telegram enabled | {len(self.desk_channels)} desks + Portfolio + System")

    def _get_desk_chat(self, desk_id):
        return self.desk_channels.get(desk_id) or self.portfolio_chat or self.fallback_chat

    def _format_timeframe(self, tf):
        return _format_tf(tf)

    async def send_message(self, text, chat_id=None):
        if not self.enabled:
            logger.debug(f"[TG-DISABLED] {text[:100]}")
            return False
        target = chat_id or self.portfolio_chat or self.fallback_chat
        if not target:
            return False
        text = _sanitize_text(text)
        try:
            resp = await self.client.post(
                f"{TELEGRAM_API.format(token=self.token)}/sendMessage",
                json={"chat_id": target, "text": text, "disable_web_page_preview": True},
            )
            if resp.status_code == 200:
                return True
            logger.error(f"Telegram API error: {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    async def _send_to_desk(self, desk_id, text):
        return await self.send_message(text, chat_id=self._get_desk_chat(desk_id))

    async def _send_to_portfolio(self, text):
        return await self.send_message(text, chat_id=self.portfolio_chat)

    async def _send_to_system(self, text):
        return await self.send_message(text, chat_id=self.system_chat)

    async def _send_to_all(self, text):
        for chat_id in self.desk_channels.values():
            await self.send_message(text, chat_id=chat_id)
        await self._send_to_portfolio(text)
        await self._send_to_system(text)

    # ━━━ TRADE ENTRY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_trade_entry(self, trade_params: Dict, decision: Dict):
        symbol = trade_params.get("symbol", "?")
        direction = trade_params.get("direction", "?")
        desk = trade_params.get("desk_id", "?")
        price = trade_params.get("price", trade_params.get("entry_price", 0))
        risk = trade_params.get("risk_dollars", 0)
        risk_pct = trade_params.get("risk_pct", 0)
        sl = trade_params.get("stop_loss", 0)
        tp1 = trade_params.get("take_profit_1", 0)
        tp2 = trade_params.get("take_profit_2", 0)
        confidence = decision.get("confidence", 0)
        reasoning = decision.get("reasoning", "")[:200]
        timeframe = trade_params.get("timeframe", "?")
        trend = trade_params.get("trend", "UNKNOWN")
        rsi_val = trade_params.get("rsi")
        exec_time = trade_params.get("exec_time_s", 0)

        dl = DESK_LABEL.get(desk, desk)
        de = DESK_EMOJI.get(desk, "\U0001f4ca")
        tf = _format_tf(timeframe)
        gold = _gold_sub(desk, timeframe)
        te = TREND_EMOJI.get(trend, "\u2753")
        tl = trend.replace("_", " ")
        align = _trend_align(direction, trend)
        dir_e = "\U0001f7e2" if direction.upper() in ["LONG", "BUY"] else "\U0001f534"

        sl_d, tp1_d, tp2_d = str(sl), str(tp1), ""
        if tp2 and str(tp2) not in ["0", "None", ""]:
            tp2_d = str(tp2)
        proj = ""
        rr_line = ""
        rr_val = 0

        try:
            ef = float(price) if price else 0
            sf = float(sl) if sl else 0
            t1f = float(tp1) if tp1 else 0
            t2f = float(tp2) if tp2 else 0
            if ef > 0:
                ps, pv = get_pip_info(str(symbol))
                sp = abs(ef - sf) / ps if sf > 0 else 0
                t1p = abs(t1f - ef) / ps if t1f > 0 else 0
                t2p = abs(t2f - ef) / ps if t2f > 0 else 0
                rr_val = round(t1p / sp, 1) if sp > 0 else 0
                rr_line = f"   \u2696\ufe0f  RR             1:{rr_val}\n"
                sl_d = f"{sl}  (\u2212{sp:.0f}p)"
                tp1_d = f"{tp1}  (+{t1p:.0f}p)"
                if t2f > 0:
                    tp2_d = f"{tp2}  (+{t2p:.0f}p)"
                lots = [0.01, 0.1, 1.0]
                sl_l = [round(sp * pv * l, 0) for l in lots]
                t1_l = [round(t1p * pv * l, 0) for l in lots]
                t2_l = [round(t2p * pv * l, 0) for l in lots]
                proj = "\n\u2501\u2501\u2501 PROJECTED P&L \u2501\u2501\u2501\u2501\u2501\u2501\n\n"
                if t2p > 0:
                    proj += f"\U0001f4b5 0.01    -${sl_l[0]:.0f}  \u2192  +${t1_l[0]:.0f}  \u2192  +${t2_l[0]:.0f}\n\U0001f4b5 0.10    -${sl_l[1]:.0f}  \u2192  +${t1_l[1]:.0f}  \u2192  +${t2_l[1]:.0f}\n\U0001f4b5 1.00    -${sl_l[2]:,.0f} \u2192  +${t1_l[2]:,.0f} \u2192  +${t2_l[2]:,.0f}"
                else:
                    proj += f"\U0001f4b5 0.01    -${sl_l[0]:.0f}  \u2192  +${t1_l[0]:.0f}\n\U0001f4b5 0.10    -${sl_l[1]:.0f}  \u2192  +${t1_l[1]:.0f}\n\U0001f4b5 1.00    -${sl_l[2]:,.0f} \u2192  +${t1_l[2]:,.0f}"
        except Exception as e:
            logger.debug(f"Projection calc failed: {e}")

        score = int(float(confidence) * 10) if confidence else 0
        sb = _score_bar(score)
        sl_lbl = _score_label(score)
        cb = _conf_bar(confidence)
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        rsi_line = f"   \U0001f4c9  RSI            {rsi_val:.0f}\n" if rsi_val is not None else ""
        ex = f"\u23f1 {exec_time:.1f}s  \u00b7  " if exec_time and exec_time > 0 else ""

        text = (
            f"\u27d0 OniQuant\n"
            f"{BAR}\n"
            f"{dir_e} {symbol}  \u00b7  {direction}  \u00b7  {price}\n"
            f"{de} {dl}{gold} \u00b7 {tf} \u00b7 {te} {tl}\n"
            f"\U0001f9ed {align}\n\n"
            f"   \U0001f534  SL     {sl_d}\n"
            f"   \U0001f3af  TP1    {tp1_d}\n"
        )
        if tp2_d:
            text += f"   \U0001f3af  TP2    {tp2_d}\n"
        text += (
            f"\n   \U0001f4b0  ${risk:.2f}  \u00b7  {risk_pct:.2f}%\n"
            f"{rr_line}{rsi_line}"
            f"{BAR}\n"
            f"{sb}  {score}/10  {sl_lbl}\n"
            f"{proj}\n\n"
            f"{reasoning}\n\n"
            f"{ex}{dl}{gold}  \u00b7  {now}\n"
            f"\U0001f50b {cb}"
        )
        await self._send_to_desk(desk, text)

        summary = (
            f"\u27d0 {dir_e} {symbol} {direction} \u00b7 ${risk:.0f} \u00b7 RR {rr_val}\n"
            f"     {sb} {score}/10 \u00b7 {dl}{gold}"
        )
        await self._send_to_portfolio(summary)

    # ━━━ TRADE EXIT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_trade_exit(self, symbol, desk_id, pnl, reason):
        is_win = pnl >= 0
        re = "\u2705" if is_win else "\u274c"
        dl = DESK_LABEL.get(desk_id, desk_id)
        de = DESK_EMOJI.get(desk_id, "\U0001f4ca")
        dn = DESKS.get(desk_id, {}).get("name", desk_id)

        src = ""
        cr = reason
        if "SRV_" in reason: src = " \U0001f5a5\ufe0f"; cr = reason.replace("SRV_", "")
        elif "SIM_" in reason: src = " \U0001f4df"; cr = reason.replace("SIM_", "")
        elif "ONIAI_" in reason: src = " \U0001f916"; cr = reason.replace("ONIAI_", "")

        text = (
            f"\u27d0 OniQuant\n"
            f"{BAR}\n"
            f"{re} {symbol}  \u00b7  CLOSED  \u00b7  ${pnl:+,.2f}{src}\n\n"
            f"   {de}  Desk     {dl}\n"
            f"   \U0001f4cb  Reason   {cr}\n"
            f"{BAR}\n"
            f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}"
        )
        await self._send_to_desk(desk_id, text)
        await self._send_to_portfolio(f"\u27d0 {re} {symbol} ${pnl:+.2f} \u00b7 {cr} \u00b7 {dl}")

    # ━━━ OniAI VIRTUAL TRADE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_oniai_signal(self, trade_params: Dict, decision: Dict):
        symbol = trade_params.get("symbol", "?")
        direction = trade_params.get("direction", "?")
        desk = trade_params.get("desk_id", "?")
        price = trade_params.get("price", 0)
        sl = trade_params.get("stop_loss", 0)
        tp1 = trade_params.get("take_profit_1", 0)
        confidence = decision.get("confidence", 0)
        reasoning = decision.get("reasoning", "")[:150]
        block_reason = trade_params.get("block_reason", "Unknown")
        timeframe = trade_params.get("timeframe", "?")
        trend = trade_params.get("trend", "UNKNOWN")

        dl = DESK_LABEL.get(desk, desk)
        de = DESK_EMOJI.get(desk, "\U0001f4ca")
        tf = _format_tf(timeframe)
        gold = _gold_sub(desk, timeframe)
        te = TREND_EMOJI.get(trend, "\u2753")
        tl = trend.replace("_", " ")
        align = _trend_align(direction, trend)
        arrow = "\u2191" if direction.upper() in ["LONG", "BUY"] else "\u2193"

        proj = ""
        try:
            ef, sf, t1f = float(price or 0), float(sl or 0), float(tp1 or 0)
            if ef > 0 and sf > 0 and t1f > 0:
                ps, pv = get_pip_info(str(symbol))
                sp = abs(ef - sf) / ps
                t1p = abs(t1f - ef) / ps
                rr = round(t1p / sp, 1) if sp > 0 else 0
                proj = f"\n   \u2696\ufe0f  RR 1:{rr} \u00b7 SL {sp:.0f}p \u00b7 TP1 {t1p:.0f}p"
        except:
            pass

        text = (
            f"\u27d0 OniQuant\n"
            f"{BAR}\n"
            f"\U0001f916 OniAI SIGNAL  \u00b7  {symbol} {arrow} {direction}\n"
            f"{de} {dl}{gold} \u00b7 {tf} \u00b7 {te} {tl}\n"
            f"\U0001f9ed {align}\n\n"
            f"   \U0001f4b2  Entry    {price}\n"
            f"   \U0001f534  SL      {sl}\n"
            f"   \U0001f3af  TP1     {tp1}\n"
            f"   \U0001f4ca  Conf    {confidence}{proj}\n\n"
            f"   \u26a0\ufe0f  Not executed: {block_reason}\n"
            f"   \U0001f52c  Tracking as virtual trade\n"
            f"{BAR}\n"
            f"{reasoning}"
        )
        await self._send_to_desk(desk, text)
        await self._send_to_portfolio(
            f"\u27d0 \U0001f916 OniAI \u00b7 {dl}{gold} \u00b7 {symbol} {arrow} {direction} @ {price} \u00b7 \u26a0\ufe0f {block_reason}"
        )

    # ━━━ PARTIAL CLOSE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_partial_close(self, symbol, desk_id, pnl, pct_closed=50, be_price=0):
        text = (
            f"\u27d0 OniQuant\n"
            f"{BAR}\n"
            f"\U0001f4d0 {symbol}  \u00b7  PARTIAL CLOSE\n\n"
            f"   \u2705  {pct_closed:.0f}% closed at TP1    ${pnl:+.2f}\n"
            f"   \U0001f6e1\ufe0f  SL \u2192 breakeven       {be_price}\n"
            f"{BAR}\n"
            f"Remaining {100 - pct_closed:.0f}% running to TP2"
        )
        await self._send_to_desk(desk_id, text)

    # ━━━ SIGNAL SKIP ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_signal_skip(self, symbol, desk_id, reason, score, timeframe="?"):
        dl = DESK_LABEL.get(desk_id, desk_id)
        de = DESK_EMOJI.get(desk_id, "\U0001f4ca")
        tf = _format_tf(timeframe)
        gold = _gold_sub(desk_id, timeframe)
        sb = _score_bar(score)
        sl = _score_label(score)

        text = (
            f"\u27d0 OniQuant\n"
            f"{BAR}\n"
            f"\u2298 {symbol}  \u00b7  SKIPPED\n"
            f"{de} {dl}{gold} \u00b7 {tf}\n\n"
            f"   {sb}  {score}/10  {sl}\n"
            f"{reason[:200]}"
        )
        await self._send_to_desk(desk_id, text)

    # ━━━ RISK ALERTS -> System Channel ━━━━━━━━━━━━━━━━━━━━━━━━

    async def alert_drawdown(self, desk_id, daily_loss, level):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\u26a0\ufe0f DRAWDOWN  \u00b7  {dl}\n\n"
            f"   \U0001f4c9  Daily loss     \u2212${abs(daily_loss):.2f}\n"
            f"   \U0001f4ca  Level          {level}\n"
            f"{BAR}\nAction required if loss continues."
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_consecutive_losses(self, desk_id, count, action):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\U0001f534 LOSS STREAK  \u00b7  {dl}\n\n"
            f"   \U0001f53b  Streak         {count} consecutive\n"
            f"   \u2699\ufe0f  Action         {action}\n{BAR}"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_desk_paused(self, desk_id, reason):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\u23f8\ufe0f DESK PAUSED  \u00b7  {dl}\n\n"
            f"   \U0001f53b  {reason}\n   \U0001f512  Manual resume required\n{BAR}"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_desk_resumed(self, desk_id):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\u25b6\ufe0f DESK RESUMED  \u00b7  {dl}\n\n"
            f"   \u2705  Size reset to 100%\n   \u2705  Loss streak cleared\n{BAR}"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_firm_drawdown(self, total_loss, level):
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\U0001f6a8 FIRM DRAWDOWN\n\n"
            f"   \U0001f4c9  Total loss     \u2212${abs(total_loss):.2f}\n"
            f"   \U0001f4ca  Halt limit     {level}\n"
            f"   \u2699\ufe0f  All desks      \u2192 50% size\n{BAR}"
        )
        await self._send_to_all(text)

    async def alert_kill_switch(self, scope, triggered_by):
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\u26d4 KILL SWITCH  \u00b7  {scope}\n\n"
            f"   \U0001f4cb  {triggered_by}\n"
            f"   \U0001f512  Closing all positions\n"
            f"   \U0001f6ab  Trading halted\n{BAR}"
        )
        if scope == "ALL":
            await self._send_to_all(text)
        else:
            await self._send_to_system(text)
            await self._send_to_desk(scope, text)

    # ━━━ SYSTEM HEALTH -> System Channel ━━━━━━━━━━━━━━━━━━━━━━

    async def send_health_status(self, components: Dict):
        all_ok = all(v.get("status") == "ok" for v in components.values())
        he = "\U0001f6e0" if all_ok else "\u26a0\ufe0f"
        hl = "SYSTEM STATUS" if all_ok else "SYSTEM DEGRADED"
        lines = []
        for name, info in components.items():
            dot = "\U0001f7e2" if info.get("status") == "ok" else "\U0001f534"
            lines.append(f"   {name:<12} {dot}  {info.get('detail', '')}")
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        text = f"\u27d0 OniQuant\n{BAR}\n{he} {hl}\n\n" + "\n".join(lines) + f"\n{BAR}\n{now}"
        await self._send_to_system(text)

    async def send_auto_repair(self, failed_service, fallback_service, detail=""):
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\U0001f527 AUTO-REPAIR\n\n"
            f"   \u274c  {failed_service:<16} timeout\n"
            f"   \u2705  {fallback_service:<16} active (fallback)\n"
            f"{BAR}\n{detail}  \u00b7  {now}"
        )
        await self._send_to_system(text)

    async def send_heartbeat(self, uptime="", trades_today=0, errors=0, sim_trades=0):
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\U0001f493 HEARTBEAT  \u00b7  \u2705\n\n"
            f"   \U0001f550  Uptime         {uptime}\n"
            f"   \U0001f4ca  Trades today   {trades_today}\n"
            f"   \u274c  Errors         {errors}\n"
            f"   \U0001f504  Sim trades     {sim_trades}\n{BAR}"
        )
        await self._send_to_system(text)

    # ━━━ DAILY BRIEF -> Portfolio + Desk Summaries ━━━━━━━━━━━━

    async def send_daily_report(self, report: Dict):
        date = report.get("date", "N/A")
        tp = report.get("total_pnl", 0)
        tt = report.get("total_trades", 0)
        wr = report.get("win_rate", 0)
        pe = _pnl_emoji(tp)
        best = report.get("best_trade", {})
        worst = report.get("worst_trade", {})

        text = (
            f"\u27d0 OniQuant  \u00b7  Daily Brief\n{BAR}\n"
            f"\U0001f4c5 {date}\n\n"
            f"   \U0001f4b0  Firm PnL       {pe} ${tp:+.2f}\n"
            f"   \U0001f4ca  Trades         {tt}\n"
            f"   \U0001f3af  Win Rate       {wr:.0%}\n"
            f"{BAR}\nDESK BREAKDOWN\n\n"
        )
        for did, dd in report.get("desks", {}).items():
            n = DESKS.get(did, {}).get("name", did)
            p = dd.get("pnl", 0)
            t = dd.get("trades", 0)
            text += f"   {_desk_dot(p)} {n:<14} ${p:+.2f}    {t} trades\n"
        text += f"{BAR}\n"
        if best: text += f"\U0001f3c6 Best   {best.get('symbol', '?')}  ${best.get('pnl', 0):+.2f}\n"
        if worst: text += f"\U0001f480 Worst  {worst.get('symbol', '?')}  ${worst.get('pnl', 0):+.2f}"
        await self._send_to_portfolio(text)

        for did, dd in report.get("desks", {}).items():
            n = DESKS.get(did, {}).get("name", did)
            p = dd.get("pnl", 0)
            t = dd.get("trades", 0)
            w = dd.get("win_rate", 0)
            dt = (
                f"\u27d0 {n}  \u00b7  Daily\n{BAR}\n"
                f"\U0001f4c5 {date}\n\n"
                f"   \U0001f4b0  PnL            {_pnl_emoji(p)} ${p:+.2f}\n"
                f"   \U0001f4ca  Trades         {t}\n"
                f"   \U0001f3af  Win Rate       {w:.0%}\n{BAR}"
            )
            await self._send_to_desk(did, dt)

    # ━━━ WEEKLY MEMO -> Portfolio ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def send_weekly_memo(self, memo):
        header = f"\u27d0 OniQuant  \u00b7  Weekly Memo\n{BAR}\n\n"
        if len(header + memo) <= 4000:
            await self._send_to_portfolio(header + memo)
        else:
            await self._send_to_portfolio(header + memo[:3900] + "\n\n(continued...)")
            rem = memo[3900:]
            while rem:
                await self._send_to_portfolio(rem[:4000])
                rem = rem[4000:]

    # ━━━ STATUS -> System Channel ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def send_status(self, dashboard: Dict):
        text = (
            f"\u27d0 OniQuant\n{BAR}\n"
            f"\U0001f4c8 FIRM STATUS\n\n"
            f"   Status:     {dashboard.get('firm_status', '?')}\n"
            f"   Signals:    {dashboard.get('total_signals_today', 0)}\n"
            f"   Trades:     {dashboard.get('total_trades_today', 0)}\n"
            f"   Daily PnL:  ${dashboard.get('total_daily_pnl', 0):+.2f}\n"
            f"{BAR}\n"
        )
        for d in dashboard.get("desks", []):
            dot = "\U0001f7e2" if d.get("is_active") else "\U0001f534"
            text += f"   {dot} {d.get('name', '?'):<14} {d.get('trades_today', 0)} trades  ${d.get('daily_pnl', 0):+.2f}  L:{d.get('consecutive_losses', 0)}\n"
        await self._send_to_system(text)

    async def close(self):
        await self.client.aclose()
