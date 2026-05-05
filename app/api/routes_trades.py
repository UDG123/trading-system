from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db.models import Trade

router=APIRouter(prefix='/trades')

@router.get('')
def trades(db:Session=Depends(get_db)):
    return db.query(Trade).order_by(Trade.id.desc()).limit(50).all()

@router.get('/{trade_id}')
def trade_detail(trade_id:int, db:Session=Depends(get_db)):
    t=db.get(Trade, trade_id)
    if not t: raise HTTPException(404,'trade not found')
    return t
