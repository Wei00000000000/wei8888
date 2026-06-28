from __future__ import annotations

import asyncio

from backend.app.database import SessionFactory, create_schema
from backend.app.importer import import_legacy_history, import_market_file


async def main() -> None:
    await create_schema()
    async with SessionFactory() as session:
        result = await import_legacy_history(session)
        result["markets"] = await import_market_file(session)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())

