from pydantic import BaseModel
from typing import Literal

class MCPTool(BaseModel):
    name:str
    category:str
    description:str
    enabled:bool=True
    transport:Literal['http','stdio','sse']='http'
    endpoint:str|None=None
    env_command:str|None=None
    risk_level:Literal['read_only','simulation','execution']='read_only'
    allowed_in_paper:bool=True
    allowed_in_live:bool=False
