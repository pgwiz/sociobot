"""Handlers package for Sociobot."""

from aiogram import Dispatcher
from bot.handlers.onboarding import router as onboarding_router
from bot.handlers.delete import router as delete_router
from bot.handlers.admin import router as admin_router
from bot.handlers.search import router as search_router
from bot.handlers.download import router as download_router


def register_all_handlers(dp: Dispatcher) -> None:
    """Register all modular routers in priority order."""
    # 1. Onboarding and channel lifecycle first (captures my_chat_member updates)
    dp.include_router(onboarding_router)
    # 2. Deletion callbacks and commands
    dp.include_router(delete_router)
    # 3. Admin dashboard and maintenance
    dp.include_router(admin_router)
    # 4. Search and interactive catalog navigation
    dp.include_router(search_router)
    # 5. Media download and peer replication engine
    dp.include_router(download_router)
