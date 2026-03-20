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
            f"🤖 **ONIQ v5.9 | {data['desk_id']}**\n\n"
            f"📶 **STRENGTH:** {bar} ({data['ml_score']}/4)\n"
            f"🌊 **REGIME:** {regime} (H: {data.get('hurst', 0):.2f})\n"
            f"🚀 **ACTION:** **{data['direction']}** @ `{data['price']}`\n"
            f"━━━━━━━━━━━━━━\n"
            f"📊 **ALPHA METRICS**\n"
            f"• RVOL: `{data.get('rvol', 0):.1f}x` | VWAP Z: `{data.get('vwap_z', 0):.1f}`\n"
            f"• ML Conf: `{data.get('ml_conf', 0)}%`\n"
            f"━━━━━━━━━━━━━━\n"
            f"📝 **LEVELS**\n"
            f"🛑 **STOP:** `{data['sl']}`\n"
            f"✅ **TARGET:** `{data['tp1']}`\n"
            f"━━━━━━━━━━━━━━\n"
            f"🔗 [OPEN STRIKE CHART]({data['tv_link']})"
        )
