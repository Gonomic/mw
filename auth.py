import os
import time
import logging
from typing import Any, Dict, Optional

import jwt
import requests
from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)

_DISCOVERY_CACHE: Dict[str, Any] = {}
_JWKS_CACHE: Dict[str, Any] = {}


def _get_env_bool(name: str, default: str = "true") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _get_discovery_url() -> str:
    url = os.getenv("SYNOLOGY_OIDC_DISCOVERY_URL", "").strip()
    if not url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SYNOLOGY_OIDC_DISCOVERY_URL is not configured",
        )
    return url


def _get_discovery_cache_ttl() -> int:
    return int(os.getenv("SYNOLOGY_OIDC_DISCOVERY_TTL", "3600"))


def _get_jwks_cache_ttl() -> int:
    return int(os.getenv("SYNOLOGY_JWKS_TTL", "3600"))


def _fetch_discovery() -> Dict[str, Any]:
    url = _get_discovery_url()
    verify_ssl = _get_env_bool("SYNOLOGY_OIDC_VERIFY_SSL", "true")
    response = requests.get(url, timeout=10, verify=verify_ssl)
    response.raise_for_status()
    return response.json()


def _get_discovery() -> Dict[str, Any]:
    now = time.time()
    ttl = _get_discovery_cache_ttl()
    cached = _DISCOVERY_CACHE.get("data")
    cached_at = _DISCOVERY_CACHE.get("timestamp", 0)
    if cached and (now - cached_at) < ttl:
        return cached
    discovery = _fetch_discovery()
    _DISCOVERY_CACHE["data"] = discovery
    _DISCOVERY_CACHE["timestamp"] = now
    return discovery


def _fetch_jwks(jwks_uri: str) -> Dict[str, Any]:
    verify_ssl = _get_env_bool("SYNOLOGY_OIDC_VERIFY_SSL", "true")
    response = requests.get(jwks_uri, timeout=10, verify=verify_ssl)
    response.raise_for_status()
    return response.json()


def _get_jwks() -> Dict[str, Any]:
    now = time.time()
    ttl = _get_jwks_cache_ttl()
    cached = _JWKS_CACHE.get("data")
    cached_at = _JWKS_CACHE.get("timestamp", 0)
    if cached and (now - cached_at) < ttl:
        return cached
    discovery = _get_discovery()
    jwks_uri = discovery.get("jwks_uri")
    if not jwks_uri:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="jwks_uri is missing from discovery document",
        )
    jwks = _fetch_jwks(jwks_uri)
    _JWKS_CACHE["data"] = jwks
    _JWKS_CACHE["timestamp"] = now
    return jwks


def _find_jwk(kid: str, jwks: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    keys = jwks.get("keys", [])
    for key in keys:
        if key.get("kid") == kid:
            return key
    return None


def verify_sso_token(token: str) -> Dict[str, Any]:
    discovery = _get_discovery()
    issuer = discovery.get("issuer")
    jwks = _get_jwks()

    try:
        unverified_header = jwt.get_unverified_header(token)
    except jwt.PyJWTError as exc:
        logger.warning("Invalid JWT header: %s", exc)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    jwk = _find_jwk(kid, jwks)
    if not jwk:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    try:
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(jwk)
    except Exception as exc:
        logger.warning("Failed to parse JWK: %s", exc)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    audience = os.getenv("SYNOLOGY_CLIENT_ID", "").strip()
    if not audience:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SYNOLOGY_CLIENT_ID is not configured",
        )

    try:
        payload = jwt.decode(
            token,
            public_key,
            algorithms=[unverified_header.get("alg", "RS256")],
            audience=audience,
            issuer=issuer,
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.PyJWTError as exc:
        logger.warning("Token validation failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


def _extract_bearer_token(request: Request) -> str:
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return auth_header.split(" ", 1)[1].strip()


def require_sso_auth(request: Request) -> Dict[str, Any]:
    """FastAPI dependency that returns verified token claims.

    Usage:
        @app.get("/protected")
        def protected_route(user=Depends(require_sso_auth)):
            return {"user": user}
    """
    token = _extract_bearer_token(request)
    return verify_sso_token(token)


def exchange_authorization_code(code: str, code_verifier: str) -> str:
    """Exchange OAuth authorization code for JWT token.
    
    Args:
        code: Authorization code from OAuth provider
        code_verifier: PKCE code verifier
    
    Returns:
        JWT access token
    """
    discovery = _get_discovery()
    token_endpoint = discovery.get("token_endpoint")
    if not token_endpoint:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="token_endpoint is missing from discovery document",
        )
    
    client_id = os.getenv("SYNOLOGY_CLIENT_ID", "")
    if not client_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SYNOLOGY_CLIENT_ID is not configured",
        )
    
    # Get redirect URI from environment (must match what's configured on Synology)
    redirect_uri = os.getenv("SYNOLOGY_REDIRECT_URI", "http://localhost:5173/auth/callback")
    
    # Prepare token exchange request
    token_request = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }
    
    # Optional: Add client secret if configured (may be required by Synology)
    client_secret = os.getenv("SYNOLOGY_CLIENT_SECRET", "").strip()
    if client_secret:
        token_request["client_secret"] = client_secret
    
    verify_ssl = _get_env_bool("SYNOLOGY_OIDC_VERIFY_SSL", "true")
    
    try:
        response = requests.post(token_endpoint, json=token_request, timeout=10, verify=verify_ssl)
        response.raise_for_status()
        token_data = response.json()
        
        if "access_token" not in token_data:
            logger.error(f"Token response missing access_token: {token_data}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to obtain access token from OAuth provider",
            )
        
        return token_data["access_token"]
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Token exchange failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token exchange failed",
        )
