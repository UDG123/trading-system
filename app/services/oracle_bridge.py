class OracleBridge:
    @staticmethod
    def get_strength_bar(score):
        # Generates a dynamic blue bar (1-4 score)
        val = int(score) if score else 0
        return "🟦" * val + "⬜️" * (4 - val)

    @staticmethod
    def format_strike_message(data: dict):
        bar = OracleBridge.get_strength_bar(data.get('ml_score', 0))
        regime = "🟢 TRENDING" if data.get('hurst', 0) > 0.55 else "🟡 NEUTRAL"

        return (
            f"🤖 <b>ONIQ v6.0 | {data['desk_id']}</b>\n\n"
            f"📶 <b>STRENGTH:</b> {bar} ({data['ml_score']}/4)\n"
            f"🌊 <b>REGIME:</b> {regime} (H: {data.get('hurst', 0):.2f})\n"
            f"🚀 <b>ACTION:</b> <b>{data['direction']}</b> @ <code>{data['price']}</code>\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 <b>ALPHA METRICS</b>\n"
            f"• RVOL: <code>{data.get('rvol', 0):.1f}x</code> | VWAP Z: <code>{data.get('vwap_z', 0):.1f}</code>\n"
            f"• ML Conf: <code>{data.get('ml_conf', 0)}%</code>\n"
            f"━━━━━━━━━━━━━━\n"
            f"📝 <b>LEVELS</b>\n"
            f"🛑 <b>STOP:</b> <code>{data['sl']}</code>\n"
            f"✅ <b>TARGET:</b> <code>{data['tp1']}</code>\n"
            f"━━━━━━━━━━━━━━\n"
            f"🔗 <a href=\"{data['tv_link']}\">OPEN STRIKE CHART</a>"
        )
