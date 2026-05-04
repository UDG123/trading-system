from app.services.pipeline_v2 import PipelineV2

def test_step2_score_size_multiplier_bounds():
    p=PipelineV2()
    q=p._step2_score({'price':1.1,'direction':'LONG','sl1':1.0,'tp1':1.3,'alert_type':'bullish_confirmation'}, {'adx':30,'rsi':55,'ema_aligned_bull':True,'ema200':1.0}, 'DESK1_SCALPER')
    assert 0.25 <= q['size_multiplier'] <= 1.0
