"""
Server-side session management for Familiez.

This module handles long-lived sessions independent of OAuth token lifetimes.
Sessions are created after successful OAuth authentication and can be renewed
through keepalive heartbeats.

Feature can be disabled via USE_SERVER_SESSIONS=false in .env
"""

import os
import time
import uuid
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# In-memory session store (in production, use Redis or database)
_SESSIONS: Dict[str, Dict[str, Any]] = {}

# Session configuration
SESSION_LIFETIME_HOURS = 24  # How long a session lasts without activity
SESSION_KEEPALIVE_MINUTES = 5  # Heartbeat interval
CLEANUP_INTERVAL_MINUTES = 60  # How often to clean expired sessions


def _is_enabled() -> bool:
    """Check if server-side sessions are enabled via .env"""
    value = os.getenv("USE_SERVER_SESSIONS", "false").strip().lower()
    enabled = value in {"1", "true", "yes", "on"}
    logger.warning(f"[Sessions DEBUG] USE_SERVER_SESSIONS='{value}' -> enabled={enabled}")
    return enabled


def create_session(user_info: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    """
    Create a new server-side session after OAuth login.
    
    Args:
        user_info: User information from OAuth (username, role, groups, etc.)
    
    Returns:
        Tuple of (session_id, cookie_dict) where cookie_dict is for FastAPI response
    """
    logger.warning(f"[Sessions DEBUG] create_session called for user {user_info.get('username')}")
    
    if not _is_enabled():
        logger.warning("[Sessions DEBUG] Sessions NOT enabled, returning empty session")
        return "", {}
    
    _cleanup_expired_sessions()
    
    session_id = str(uuid.uuid4())
    now = time.time()
    expiry = now + (SESSION_LIFETIME_HOURS * 3600)
    
    _SESSIONS[session_id] = {
        "user_info": user_info,
        "created_at": now,
        "last_activity": now,
        "expires_at": expiry,
    }
    
    username = user_info.get('username', 'unknown')
    logger.info(f"[Sessions] Created session {session_id[:8]}... for user {username}")
    
    # Return cookie configuration for FastAPI set_cookie()
    # Use SameSite=None for production (cross-site) and Lax for dev (same-site)
    is_prod = _is_production()
    cookie_config = {
        "key": "familiez_session",
        "value": session_id,
        "max_age": SESSION_LIFETIME_HOURS * 3600,
        "httponly": True,
        "secure": is_prod,
        "samesite": "None" if is_prod else "Lax",
    }
    
    return session_id, cookie_config


def validate_session(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Validate a session cookie and update lastactivity.
    
    Args:
        session_id: Session ID from cookie
    
    Returns:
        User info dict if valid, None if expired/invalid
    """
    if not _is_enabled():
        return None
    
    session = _SESSIONS.get(session_id)
    
    if not session:
        logger.debug(f"[Sessions] Session not found: {session_id[:8]}...")
        return None
    
    now = time.time()
    if now > session["expires_at"]:
        logger.warning(f"[Sessions] Session expired: {session_id[:8]}...")
        del _SESSIONS[session_id]
        return None
    
    # Update last activity (keepalive)
    session["last_activity"] = now

    logger.debug(f"[Sessions] Session {session_id[:8]}... validated and updated")
    
    return session.get("user_info")


def renew_session(session_id: str) -> bool:
    """
    Renew a session's expiry time (called by keepalive heartbeat).
    
    Args:
        session_id: Session ID from cookie
    
    Returns:
        True if renewed, False if invalid/expired
    """
    if not _is_enabled():
        return False
    
    session = _SESSIONS.get(session_id)
    
    if not session:
        return False
    
    now = time.time()
    if now > session["expires_at"]:
        del _SESSIONS[session_id]
        return False
    
    # Extend expiry
    session["expires_at"] = now + (SESSION_LIFETIME_HOURS * 3600)
    session["last_activity"] = now
    
    logger.debug(f"[Sessions] Renewed session {session_id[:8]}...")
    
    return True


def destroy_session(session_id: str) -> bool:
    """
    Destroy a session (logout).
    
    Args:
        session_id: Session ID to destroy
    
    Returns:
        True if destroyed, False if not found
    """
    if not _is_enabled():
        return False
    
    if session_id in _SESSIONS:
        username = _SESSIONS[session_id].get("user_info", {}).get("username", "unknown")
        del _SESSIONS[session_id]
        logger.info(f"[Sessions] Destroyed session for user {username}")
        return True
    
    return False


def _cleanup_expired_sessions() -> None:
    """Remove expired sessions from memory."""
    now = time.time()
    expired_ids = [
        sid for sid, sess in _SESSIONS.items()
        if now > sess["expires_at"]
    ]
    
    if expired_ids:
        for sid in expired_ids:
            del _SESSIONS[sid]
        logger.debug(f"[Sessions] Cleaned up {len(expired_ids)} expired sessions")


def _is_production() -> bool:
    """Check if running in production."""
    env = os.getenv("ENVIRONMENT", "development").lower()
    return env in {"prod", "production"}


def get_session_info() -> Dict[str, Any]:
    """Debug: Get current session count and stats."""
    now = time.time()
    active_sessions = [
        s for s in _SESSIONS.values()
        if now <= s["expires_at"]
    ]
    
    return {
        "active_sessions": len(active_sessions),
        "total_sessions": len(_SESSIONS),
        "sessions_enabled": _is_enabled(),
        "session_lifetime_hours": SESSION_LIFETIME_HOURS,
        "keepalive_minutes": SESSION_KEEPALIVE_MINUTES,
    }
