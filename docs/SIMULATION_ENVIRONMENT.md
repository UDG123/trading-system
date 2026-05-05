# Simulation Environment

## Signal flow
DeskScanner -> Pipeline v2 -> ShadowLogger -> VirtualBroker (paper only).

## Metadata
Preserved fields: desk_mode, desk_role, strategy_mode, mode_reason, quality_hints, cross_desk_bias, bias_alignment, bias_action, bias_size_mult, stack_id, regime, strategy_id.

## Rejected signal tracking
Rejected/blocked signals should be retained in raw payload with rejection_reason, blocked_layer, would_have_simulated=true.

## Telegram routing
Desk channels TG_DESK1..TG_DESK6, portfolio TG_PORTFOLIO, system TG_SYSTEM, fallback TELEGRAM_CHAT_ID.

## Smoke test
Run: `python app/scripts/smoke_simulation_flow.py`

## Not implemented
No live broker routing, no market order placement, no dashboard requirements.
