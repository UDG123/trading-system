class OracleBridge:
    @staticmethod
    def get_strength_bar(score):
        # Generates a dynamic blue bar (1-4 score)
        val = int(score) if score else 0
        return "🟦" * val + "⬜️" * (4 - val)

    @staticmethod
    def format_strike_message(data: dict):
        bar = OracleBridge.get_strength_bar(data.get('ml_score', 0))
        regime = "TRENDING" if data.get('hurst', 0) > 0.55 else "NEUTRAL"
        hurst_val = data.get('hurst', 0)

        return (
            f"OniQuant v6.0 | {data['desk_id']}\n"
            f"━━━━━━━━━━━━━━\n"
            f"STRENGTH: {bar} ({data['ml_score']}/4)\n"
            f"REGIME: {regime} (H: {hurst_val:.2f})\n"
            f"ACTION: {data['direction']} @ {data['price']}\n"
            f"━━━━━━━━━━━━━━\n"
            f"RVOL: {data.get('rvol', 0):.1f}x | VWAP Z: {data.get('vwap_z', 0):.1f}\n"
            f"ML Conf: {data.get('ml_conf', 0)}%\n"
            f"━━━━━━━━━━━━━━\n"
            f"STOP: {data['sl']}\n"
            f"TARGET: {data['tp1']}\n"
        )
