"""
Global application context for settings and configuration.
Set once at startup, used throughout the application.
"""
from typing import Optional
from .settings import Settings

# Global application settings - set once at startup
_global_settings: Optional[Settings] = None


def set_global_settings(settings: Settings) -> None:
    """Set global settings for the application. Call once at startup."""
    global _global_settings
    _global_settings = settings


def get_global_settings() -> Optional[Settings]:
    """Get the global settings instance."""
    return _global_settings


def require_global_settings() -> Settings:
    """Get global settings, raising an error if not set."""
    if _global_settings is None:
        raise RuntimeError("Global settings not initialized. Call set_global_settings() at startup.")
    return _global_settings
