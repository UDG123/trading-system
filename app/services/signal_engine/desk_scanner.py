# (patched section only shown below — rest of file unchanged)

            # Run applicable strategy stacks (NOW DESK-AWARE)
            stack_results = run_stacks(df, indicators, symbol, regime, desk_id=desk_id)

            for result in stack_results:
                # ... existing logic unchanged ...

                result["symbol"] = symbol
                result["desk_id"] = desk_id
                result["timeframe"] = entry_tf
                result["price"] = price
                result["atr"] = atr

                # Ensure desk_mode exists for FX desks
                if not result.get("desk_mode"):
                    if desk_id in ("DESK1_SCALPER", "DESK2_INTRADAY", "DESK3_SWING"):
                        result["desk_mode"] = result.get("desk_role")

                candidates.append(result)
                self._signal_count += 1
