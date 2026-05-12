import asyncio

from app.scripts.smoke_internal_engine import main as smoke_main


if __name__ == "__main__":
    asyncio.run(smoke_main())
