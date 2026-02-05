import asyncio
import aiosqlite
import config

async def cleanup():
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        cursor = await conn.execute(
            "DELETE FROM synthetic_queries WHERE query_type IN ('specific', 'vague')"
        )
        deleted = cursor.rowcount
        await conn.commit()
        print(f"✓ Deleted {deleted} synthetic queries")
        print("✓ Real requirements preserved")
    finally:
        await conn.close()

if __name__ == '__main__':
    asyncio.run(cleanup())