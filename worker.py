import uvloop
uvloop.install()
import asyncio
from app.worker import main
asyncio.run(main())
