from app.core.redis_bus import consume_group, ack_event, publish_event, STREAMS
import json
from app.core.desk_scanner import classify_symbol

def run():
    msgs=consume_group(STREAMS['raw'],'scanner','scanner-1')
    for _,entries in msgs:
        for mid,data in entries:
            p=json.loads(data['payload'])
            desk=classify_symbol(p['symbol'])
            if desk:
                p['desk']=desk
                publish_event(STREAMS['validated'],p)
            else:
                publish_event(STREAMS['rejected'],{'event':p,'reason':'unsupported_symbol'})
            ack_event(STREAMS['raw'],'scanner',mid)

if __name__=='__main__': run()
