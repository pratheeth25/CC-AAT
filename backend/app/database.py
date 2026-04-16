import logging
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import settings

logger = logging.getLogger(__name__)

_client: Optional[AsyncIOMotorClient] = None


async def connect_to_mongo() -> None:
    global _client
    logger.info("Connecting to MongoDB at %s ...", settings.MONGO_URI)
    _client = AsyncIOMotorClient(settings.MONGO_URI)
    await _client.admin.command("ping")
    logger.info("MongoDB connection established.")


async def close_mongo_connection() -> None:
    global _client
    if _client is not None:
        _client.close()
        logger.info("MongoDB connection closed.")


def get_database() -> AsyncIOMotorDatabase:
    if _client is None:
        raise RuntimeError(
            "MongoDB client is not initialised. Ensure connect_to_mongo() ran at startup."
        )
    return _client[settings.MONGO_DB]
