import logging
logger=logging.getLogger(__name__)

def safe_call(tool_name:str, payload:dict):
    logger.info('mcp_call %s %s', tool_name, payload)
    return {'tool':tool_name,'ok':True,'data':{}}
