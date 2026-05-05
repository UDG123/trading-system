from app.config import settings
MAJORS={'EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','USDCHF','NZDUSD'}
MINORS={'EURGBP','EURAUD','EURJPY','GBPJPY','AUDJPY'}
EXOTICS={'USDTRY','USDZAR','USDMXN'}

def classify_symbol(symbol:str)->str|None:
    s=symbol.upper()
    if s in {'XAUUSD','MGC','GC'}: return 'gold'
    if s in MAJORS: return 'fx_desk_1'
    if s in MINORS: return 'fx_desk_2'
    if s in EXOTICS and settings.enable_fx_exotics: return 'fx_desk_3'
    if (s.endswith('USDT') or s in {'BTCUSD','ETHUSD'}) and settings.enable_crypto_desk: return 'crypto'
    return None
