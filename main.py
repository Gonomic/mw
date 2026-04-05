import os
import logging
from datetime import datetime, date
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from fastapi import FastAPI, Query, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from sqlalchemy import create_engine, text
from PIL import Image
import io
import json
import mimetypes
from pathlib import Path

from auth import verify_sso_token, exchange_authorization_code, resolve_ldap_role_from_claims, require_admin_role
from session_manager import create_session, validate_session, destroy_session, renew_session, get_session_info
from file_utils import (
    slugify,
    generate_filename,
    get_person_path,
    get_family_path,
    ensure_directory_exists,
    get_storage_base_path
)

logger = logging.getLogger(__name__)

VALID_ENVIRONMENTS = {"development", "dev", "test", "staging", "prod", "production"}
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").strip().lower()
if ENVIRONMENT not in VALID_ENVIRONMENTS:
    logger.warning(
        "Unknown ENVIRONMENT='%s'. Expected one of %s. Defaulting to non-production behavior.",
        ENVIRONMENT,
        sorted(VALID_ENVIRONMENTS),
    )
elif ENVIRONMENT in {"development", "dev"}:
    logger.info("Running in development mode (ENVIRONMENT=%s)", ENVIRONMENT)

# Configuration from environment variables
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://HumansService:XHHxECL54EjvhhPSBLMU@localhost:3306/humans"
)
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://localhost:3310"
).split(",")

# File storage configuration
STORAGE_ENVIRONMENT = os.getenv("STORAGE_ENVIRONMENT", "development")
STORAGE_BASE_PATH = os.getenv(
    "STORAGE_BASE_PATH",
    "/home/frans/Documenten/Dev/Familiez/BESTANDEN"
)
MAX_FILE_UPLOAD_SIZE = int(os.getenv("MAX_FILE_UPLOAD_SIZE", "52428800"))  # 50MB default

PUBLIC_PATHS = {
    "/",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/auth/callback",
    "/auth/discovery",
    "/auth/logout",      # NEW: Allow logout without token
    "/auth/keepalive",   # NEW: Allow session keepalive without token (uses session cookie)
    "/GetReleases",
    "/pingAPI",
    "/pingDB",
}

# Initialize database engine once at startup
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Test connections before using them
    pool_recycle=3600,   # Recycle connections every hour
)

def format_result(results: List[Any]) -> List[Dict[str, Any]]:
    """Format database results with record count header."""
    if not results:
        return [{"numberOfRecords": 0}]
    result_dicts = [row._asdict() for row in results]
    return [{"numberOfRecords": len(result_dicts)}, *result_dicts]

def fetch_releases(component: str) -> List[Dict[str, Any]]:
    if component not in {"fe", "mw", "be"}:
        raise HTTPException(status_code=400, detail="Invalid component. Use fe, mw, or be.")

    with engine.connect() as connection:
        rows = connection.execute(
            text("call GetReleasesByComponent(:componentIn)"),
            {"componentIn": component}
        ).fetchall()

    releases: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        release_id = row.ReleaseID
        if release_id not in releases:
            releases[release_id] = {
                "ReleaseID": release_id,
                "ReleaseNumber": row.ReleaseNumber,
                "ReleaseDate": row.ReleaseDate,
                "Description": row.Description,
                "Component": component,
                "Changes": [],
            }
        if row.ChangeID is not None:
            releases[release_id]["Changes"].append({
                "ChangeID": row.ChangeID,
                "ChangeDescription": row.ChangeDescription,
                "ChangeType": row.ChangeType,
            })

    return list(releases.values())

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

def create_cors_json_response(status_code: int, content: dict, origin: str = None) -> JSONResponse:
    """Create a JSONResponse with CORS headers for error responses from middleware."""
    response = JSONResponse(status_code=status_code, content=content)
    # Add CORS headers - use provided origin or allow all ALLOWED_ORIGINS
    if origin and origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
    elif ALLOWED_ORIGINS:
        # Use first allowed origin as fallback
        response.headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGINS[0]
        response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

@app.middleware("http")
async def require_sso_middleware(request: Request, call_next):
    if request.method == "OPTIONS" or request.url.path in PUBLIC_PATHS:
        return await call_next(request)

    # Get origin from request for CORS
    origin = request.headers.get("origin")
    session_id = (request.cookies.get("familiez_session") or "").strip()
    session_user = validate_session(session_id) if session_id else None
    
    auth_header = request.headers.get("authorization", "")
    token = ""

    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
    elif request.method == "GET" and request.url.path.startswith("/api/files/"):
        # Browser-based preview/image requests (window.open/img src) cannot attach custom auth headers.
        token = (request.query_params.get("token") or "").strip()

    if not token:
        if session_user:
            request.state.user = {}
            request.state.user_access = session_user
            return await call_next(request)

        logger.warning(f"[Auth] Missing auth token and no valid session for {request.url.path}. Header: {auth_header[:50] if auth_header else 'NONE'}")
        return create_cors_json_response(401, {"detail": "Missing or invalid token"}, origin)

    try:
        claims = verify_sso_token(token)
        request.state.user = claims
        request.state.user_access = resolve_ldap_role_from_claims(claims)
    except HTTPException as exc:
        if session_user:
            request.state.user = {}
            request.state.user_access = session_user
            logger.info(f"[Auth] JWT invalid ({exc.detail}) for {request.url.path}; falling back to server session")
            return await call_next(request)

        logger.warning(f"[Auth] Token validation failed for {request.url.path}: {exc.detail}")
        return create_cors_json_response(exc.status_code, {"detail": exc.detail}, origin)

    return await call_next(request)

@app.get("/")
def read_root() -> Dict[str, str]:
    return {"message": "Familiez API", "status": "OK"}

@app.get("/auth/discovery")
def get_oidc_discovery() -> Dict[str, Any]:
    """Proxy OIDC discovery document to avoid CORS issues in frontend."""
    import requests
    
    discovery_url = os.getenv(
        "SYNOLOGY_OIDC_DISCOVERY_URL",
        "https://sso.dekknet.com/webman/sso/.well-known/openid-configuration"
    )
    verify_ssl = os.getenv("SYNOLOGY_OIDC_VERIFY_SSL", "true").strip().lower() in {"1", "true", "yes", "on"}
    
    try:
        response = requests.get(discovery_url, timeout=10, verify=verify_ssl)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch OIDC discovery: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch OIDC configuration")

@app.post("/auth/callback")
def oauth_callback(request_data: Dict[str, str]) -> Dict[str, str]:
    """Exchange OAuth authorization code for JWT token.
    
    Expected request body:
    {
        "code": "authorization_code_from_oauth"
    }
    Note: codeVerifier is not needed as Synology doesn't support PKCE
    
    Returns:
    - access_token: JWT token (still needed for subsequent API calls)
    - Sets session cookie if USE_SERVER_SESSIONS=true
    """
    code = request_data.get("code", "").strip()
    
    if not code:
        logger.error("OAuth callback missing authorization code")
        raise HTTPException(status_code=400, detail="Missing code")
    
    try:
        access_token, user_access = exchange_authorization_code(code)
        response = JSONResponse({"access_token": access_token})
        
        # Create server-side session if enabled
        session_id, cookie_config = create_session(user_access)
        if session_id:
            logger.info(f"Setting session cookie for user {user_access.get('username')}")
            response.set_cookie(**cookie_config)
        
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during token exchange: {e}")
        raise HTTPException(status_code=500, detail="Token exchange failed")


@app.get("/auth/me")
def get_authenticated_user(request: Request) -> Dict[str, Any]:
    claims = getattr(request.state, "user", {}) or {}
    access = getattr(request.state, "user_access", {}) or {}
    return {
        "username": access.get("username") or claims.get("preferred_username") or claims.get("username") or claims.get("sub") or "",
        "role": access.get("role", "none"),
        "groups": access.get("groups", []),
        "is_admin": bool(access.get("is_admin", False)),
        "is_user": bool(access.get("is_user", False)),
    }

@app.post("/auth/keepalive")
def session_keepalive(request: Request) -> Dict[str, Any]:
    """Heartbeat endpoint to keep session alive.
    
    Called periodically by client to extend session expiry.
    Requires valid session cookie (if USE_SERVER_SESSIONS=true).
    """
    session_id = request.cookies.get("familiez_session", "")
    
    if session_id and renew_session(session_id):
        return {"status": "renewed"}
    
    return {"status": "no_session"}

@app.post("/auth/logout")
def logout(request: Request) -> Dict[str, str]:
    """Logout endpoint to destroy the session.
    
    Clears both the session cookie and invalidates the JWT token.
    """
    session_id = request.cookies.get("familiez_session", "")
    
    if session_id:
        destroy_session(session_id)
        logger.info(f"User logged out")
    
    response = JSONResponse({"status": "logged_out"})
    response.delete_cookie("familiez_session")
    
    return response

@app.get("/auth/session-info")
def get_session_info_debug(request: Request) -> Dict[str, Any]:
    """Debug endpoint: session statistics for authenticated admins only (non-production)."""
    if ENVIRONMENT in {"prod", "production"}:
        raise HTTPException(status_code=404, detail="Not found")

    require_admin_role(request)
    return get_session_info()

@app.get("/pingAPI")
def ping_api(timestampFE: datetime) -> List[Dict[str, str]]:
    return [{
        "FE request time": timestampFE.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "MW request time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    }]

@app.get("/pingDB")
def ping_db(timestampFE: datetime) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            timestampMWrequest = datetime.now()
            results_proxy = connection.execute(
                text("call PingedDbServer(:timestampFErequest, :timestampMWrequest)"),
                {"timestampFErequest": timestampFE, "timestampMWrequest": timestampMWrequest}
            )
            results = results_proxy.fetchall()
            result = [row._asdict() for row in results]
            if result:
                result[-1]['datetimeMWanswer'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            return result
    except Exception as e:
        logger.error(f"Error pinging database: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/GetPersonsLike")
def get_persons_like(
    stringToSearchFor: str = Query(..., description="(Part of)Name to search for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call GetPersonsLike(:stringToSearchFor)"),
                {"stringToSearchFor": stringToSearchFor}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_persons_like: {e}")
        raise HTTPException(status_code=500, detail="Query failed")

@app.get("/GetSiblings")
def get_siblings(
    parentID: int = Query(..., description="Person ID of the father to lookup the childs for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call GetAllChildrenWithoutPartnerFromOneParent(:ParentIdToSearchFor)"),
                {"ParentIdToSearchFor": parentID}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_siblings: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetFather")
def get_father(
    childID: int = Query(..., description="Person ID of the child to lookup the father for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call GetFather(:childId)"),
                {"childId": childID}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_father: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetPersonDetails")
def get_person_details(
    personID: int = Query(..., description="Person ID to get details for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call GetPersonDetails_v2(:personId)"),
                {"personId": personID}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_person_details: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetMother")
def get_mother(
    childID: int = Query(..., description="Person ID of the child to lookup the mother for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call GetMother(:childId)"),
                {"childId": childID}
            )
            results = results_proxy.fetchall()
            # GetMother returns MotherID like GetFather returns FatherID
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_mother: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetChildren")
def get_children(
    personID: int = Query(..., description="Person ID to get children for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            # Use the correct stored procedure name
            results_proxy = connection.execute(
                text("call GetAllChildrenWithoutPartnerFromOneParent(:parentId)"),
                {"parentId": personID}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_children: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetPartners")
def get_partners(
    personID: int = Query(..., description="Person ID to get partners for")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call GetPartnerForPerson(:personId)"),
                {"personId": personID}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_partners: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetReleases")
def get_releases(
    component: str = Query(..., description="Component to fetch releases for: fe, mw, be")
) -> List[Dict[str, Any]]:
    try:
        normalized_component = component.strip().lower()
        return fetch_releases(normalized_component)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_releases: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetPossibleMothersBasedOnAge")
def get_possible_mothers_based_on_age(
    personDateOfBirth: date = Query(..., description="Birth date of the child (YYYY-MM-DD)")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call getPossibleMothersBasedOnAge(:personAgeIn)"),
                {"personAgeIn": personDateOfBirth}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_possible_mothers_based_on_age: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetPossibleFathersBasedOnAge")
def get_possible_fathers_based_on_age(
    personDateOfBirth: date = Query(..., description="Birth date of the child (YYYY-MM-DD)")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call getPossibleFathersBasedOnAge(:personAgeIn)"),
                {"personAgeIn": personDateOfBirth}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_possible_fathers_based_on_age: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/GetPossiblePartnersBasedOnAge")
def get_possible_partners_based_on_age(
    personDateOfBirth: date = Query(..., description="Birth date of the person (YYYY-MM-DD)")
) -> List[Dict[str, Any]]:
    try:
        with engine.connect() as connection:
            results_proxy = connection.execute(
                text("call getPossiblePartnersBasedOnAge(:personAgeIn)"),
                {"personAgeIn": personDateOfBirth}
            )
            results = results_proxy.fetchall()
            return format_result(results)
    except Exception as e:
        logger.error(f"Error in get_possible_partners_based_on_age: {e}")
        raise HTTPException(status_code=500, detail="Query failed")


@app.post("/UpdatePerson")
def update_person(
    request: Request,
    person_data: Dict[str, Any]
) -> Dict[str, Any]:
    # Check if user has admin role
    require_admin_role(request)
    
    try:
        with engine.connect() as connection:
            person_id = person_data.get('personId')
            birth_status = person_data.get('PersonDateOfBirthStatus')
            death_status = person_data.get('PersonDateOfDeathStatus')
            
            # Use relation IDs from request (frontend now sends these)
            person_is_male = person_data.get('PersonIsMale')
            mother_id = person_data.get('MotherId')
            father_id = person_data.get('FatherId')
            partner_id = person_data.get('PartnerId')
            
            # Call ChangePerson_v2 so the DB keeps existing status values when not provided.
            results_proxy = connection.execute(
                text("""call ChangePerson_v2(
                    :personId, 
                    :givvenName, 
                    :familyName, 
                    :dateOfBirth,
                    :placeOfBirth,
                    :dateOfDeath,
                    :placeOfDeath,
                    :isMale,
                    :motherId,
                    :fatherId,
                    :partnerId,
                    :birthStatus,
                    :deathStatus
                )"""),
                {
                    "personId": person_id,
                    "givvenName": person_data.get('PersonGivvenName', ''),
                    "familyName": person_data.get('PersonFamilyName', ''),
                    "dateOfBirth": person_data.get('PersonDateOfBirth'),
                    "placeOfBirth": person_data.get('PersonPlaceOfBirth'),
                    "dateOfDeath": person_data.get('PersonDateOfDeath'),
                    "placeOfDeath": person_data.get('PersonPlaceOfDeath'),
                    "isMale": person_is_male,
                    "motherId": mother_id,
                    "fatherId": father_id,
                    "partnerId": partner_id,
                    "birthStatus": birth_status,
                    "deathStatus": death_status
                }
            )
            results = results_proxy.fetchall()
            if results and len(results) > 0:
                result_dict = results[0]._asdict() if hasattr(results[0], '_asdict') else dict(results[0])
                completed_ok = result_dict.get('CompletedOk')
                result_code = result_dict.get('Result')
                error_message = result_dict.get('ErrorMessage')
                if completed_ok is not None and completed_ok != 0:
                    logger.warning(f"ChangePerson returned CompletedOk: {completed_ok}")
                    connection.rollback()
                    if completed_ok == 1 and result_code == 404:
                        return {"success": False, "error": error_message or "Persoon niet gevonden"}
                    return {"success": False, "error": "Wijziging mislukt - controleer database logs"}

            connection.commit()
            return {"success": True}
    except Exception as e:
        logger.error(f"Error in update_person: {e}")
        raise HTTPException(status_code=500, detail="Update failed")


@app.post("/AddPerson")
def add_person(
    request: Request,
    person_data: Dict[str, Any]
) -> Dict[str, Any]:
    # Check if user has admin role
    require_admin_role(request)
    
    try:
        with engine.connect() as connection:
            is_male = person_data.get('PersonIsMale')
            
            # Call AddPerson_v2 so the new PersonID is returned in the first result set.
            try:
                results_proxy = connection.execute(
                    text("""call AddPerson_v2(
                        NULL,
                        :givvenName, 
                        :familyName, 
                        :dateOfBirth,
                        :placeOfBirth,
                        :dateOfDeath,
                        :placeOfDeath,
                        :isMale,
                        :motherId,
                        :fatherId,
                        :partnerId,
                        :birthStatus,
                        :deathStatus
                    )"""),
                    {
                        "givvenName": person_data.get('PersonGivvenName', ''),
                        "familyName": person_data.get('PersonFamilyName', ''),
                        "dateOfBirth": person_data.get('PersonDateOfBirth') or None,
                        "placeOfBirth": person_data.get('PersonPlaceOfBirth') or None,
                        "dateOfDeath": person_data.get('PersonDateOfDeath') or None,
                        "placeOfDeath": person_data.get('PersonPlaceOfDeath') or None,
                        "isMale": is_male,
                        "motherId": person_data.get('MotherId') or None,
                        "fatherId": person_data.get('FatherId') or None,
                        "partnerId": person_data.get('PartnerId') or None,
                        "birthStatus": person_data.get('PersonDateOfBirthStatus', 0),
                        "deathStatus": person_data.get('PersonDateOfDeathStatus', 0),
                    }
                )
                results = results_proxy.fetchall()
                
                if results and len(results) > 0:
                    result_dict = results[0]._asdict() if hasattr(results[0], '_asdict') else dict(results[0])

                    # Only enforce CompletedOk when the procedure returns it explicitly.
                    if 'CompletedOk' in result_dict and result_dict.get('CompletedOk') != 0:
                        logger.error(f"AddPerson procedure failed with CompletedOk={result_dict.get('CompletedOk')}")
                        connection.rollback()
                        return {"success": False, "error": "Database procedure mislukt"}

                    if 'PersonID' in result_dict and result_dict.get('PersonID') is not None:
                        connection.commit()
                        return {
                            "success": True,
                            "personId": result_dict.get('PersonID')
                        }
                
                connection.commit()
                
            except Exception as proc_error:
                logger.error(f"AddPerson procedure error: {proc_error}")
                connection.rollback()
                error_msg = str(proc_error)
                if 'Incorrect date value' in error_msg:
                    return {"success": False, "error": "Datumfout - controleer datum formaat"}
                elif 'foreign key' in error_msg.lower():
                    return {"success": False, "error": "Vader/Moeder ID niet gevonden"}
                return {"success": False, "error": f"Database fout: {error_msg[:50]}"}
            
            logger.error("AddPerson did not return a PersonID in the first result set")
            return {"success": False, "error": "Persoon opgeslaan maar kon niet worden opgehaald"}
                
    except Exception as e:
        logger.error(f"Error in add_person: {e}", exc_info=True)
        error_msg = str(e)
        return {
            "success": False, 
            "error": f"Onverwachte fout: {error_msg[:60]}"
        }






@app.post("/DeletePerson")
def delete_person(
    request: Request,
    person_data: Dict[str, Any]
) -> Dict[str, Any]:
    # Check if user has admin role
    require_admin_role(request)
    
    try:
        with engine.connect() as connection:
            person_id = person_data.get('personId')
            mother_id = person_data.get('MotherId') or None
            father_id = person_data.get('FatherId') or None
            partner_id = person_data.get('PartnerId') or None
            timestamp = person_data.get('Timestamp')
            
            mother_id = person_data.get('MotherId')
            father_id = person_data.get('FatherId')
            partner_id = person_data.get('PartnerId')
            
            if not timestamp:
                logger.error(f"No Timestamp provided for DeletePerson with personId: {person_id}")
                return {"success": False, "error": "Timestamp is vereist voor verwijdering"}
            
            logger.info(f"DeletePerson called for personId: {person_id}, timestamp: {timestamp}")
            logger.info(f"MotherId: {mother_id}, FatherId: {father_id}, PartnerId: {partner_id}")
            
            results_proxy = connection.execute(
                text("""call deletePerson(
                    :personId,
                    :motherId,
                    :fatherId,
                    :partnerId,
                    :timestamp
                )"""),
                {
                    "personId": person_id,
                    "motherId": mother_id,
                    "fatherId": father_id,
                    "partnerId": partner_id,
                    "timestamp": timestamp
                }
            )
            results = results_proxy.fetchall()
            
            logger.info(f"DeletePerson results: {results}")
            
            # Check for CompletedOk status
            if results and len(results) > 0:
                result_dict = results[0]._asdict() if hasattr(results[0], '_asdict') else dict(results[0])
                logger.info(f"Result dict: {result_dict}")
                
                completed_ok = result_dict.get('CompletedOk')
                if completed_ok == 0:
                    connection.commit()
                    return {"success": True}
                else:
                    logger.warning(f"DeletePerson returned CompletedOk: {completed_ok}")
                    return {"success": False, "error": "Verwijdering mislukt - controleer database logs"}
            
            connection.commit()
            return {"success": True}
            
    except Exception as e:
        logger.error(f"Error in delete_person: {e}", exc_info=True)
        error_msg = str(e)
        return {
            "success": False, 
            "error": f"Verwijdering mislukt: {error_msg[:80]}"
        }


# ============================================================================
# FILE MANAGEMENT ENDPOINTS
# ============================================================================

@app.post("/api/files/upload")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    scope: str = Form(...),  # "person" or "family"
    entity_id: str = Form(...),  # person_id or "father_id_mother_id"
    document_type: str = Form(...),
    year: int = Form(None),
    person_data: str = Form(None)  # JSON string with name info
) -> Dict[str, Any]:
    """
    Upload a file and store it with metadata.
    
    Scope determines storage location:
    - "person": stored in person-specific directory
    - "family": stored in family-specific directory
    
    Args:
        file: Uploaded file
        scope: Either "person" or "family"
        entity_id: Person ID or "father_id_mother_id" for family
        document_type: Type of document (e.g., 'portret', 'geboorteakte')
        year: Optional year associated with the document
        person_data: JSON string with name information for path generation
        
    Returns:
        Dict with file_id and success status
    """
    written_file_path = None

    try:
        original_filename = file.filename or "unknown"
        logger.info(f"File upload: scope={scope}, entity={entity_id}, type={document_type}")
        logger.info(f"  Original filename: '{original_filename}'")
        logger.info(f"  Content-Type: '{file.content_type}'")
        
        # Read file contents
        contents = await file.read()
        file_size = len(contents)
        
        # Check file size
        if file_size > MAX_FILE_UPLOAD_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Maximum size is {MAX_FILE_UPLOAD_SIZE} bytes"
            )
        
        # Parse person_data if provided
        names_data = json.loads(person_data) if person_data else {}
        
        # Determine file extension
        file_ext = Path(original_filename).suffix.lstrip('.') or 'bin'
        logger.info(f"  Detected extension: '{file_ext}'")
        
        # Get MIME type
        mime_type = file.content_type or 'application/octet-stream'
        
        # Generate storage path and filename based on scope
        base_path = STORAGE_BASE_PATH
        
        if scope == "person":
            person_id = int(entity_id)
            first_name = names_data.get('first_name', 'unknown')
            last_name = names_data.get('last_name', 'unknown')
            
            storage_dir = get_person_path(base_path, person_id, first_name, last_name)
            filename = generate_filename(person_id, document_type, year, file_ext)
            
        elif scope == "family":
            # entity_id format: "father_id_mother_id"
            parts = entity_id.split('_')
            if len(parts) != 2:
                raise HTTPException(status_code=400, detail="Invalid family entity_id format")
            
            father_id = int(parts[0])
            mother_id = int(parts[1])
            
            father_first = names_data.get('father_first_name', 'unknown')
            father_last = names_data.get('father_last_name', 'unknown')
            mother_first = names_data.get('mother_first_name', 'unknown')
            mother_last = names_data.get('mother_last_name', 'unknown')
            
            storage_dir = get_family_path(
                base_path, father_id, father_first, father_last,
                mother_id, mother_first, mother_last
            )
            family_id = f"{father_id}_{mother_id}"
            filename = generate_filename(family_id, document_type, year, file_ext)
        else:
            raise HTTPException(status_code=400, detail="Invalid scope. Must be 'person' or 'family'")
        
        # Ensure directory exists
        ensure_directory_exists(storage_dir)
        
        # Full file path
        file_path = storage_dir / filename
        relative_path = str(file_path.relative_to(base_path))
        
        # Write file to disk
        with open(file_path, 'wb') as f:
            f.write(contents)
        written_file_path = file_path
        
        logger.info(f"File written to disk: {file_path}")
        
        # Get username for uploaded_by
        user_claims = getattr(request.state, 'user', {})
        uploaded_by = user_claims.get('preferred_username') or user_claims.get('sub', 'unknown')
        
        # Insert metadata into database via sprocs
        with engine.connect() as conn:
            sproc_params = {
                'path': relative_path,
                'filename': filename,
                'original': original_filename,
                'doctype': document_type,
                'year': year if year else None,
                'size': file_size,
                'mime': mime_type,
                'uploaded_by': uploaded_by
            }

            if scope == "person":
                sproc_result = conn.execute(
                    text("""
                        call AddFileForPerson(
                            :path,
                            :filename,
                            :original,
                            :doctype,
                            :year,
                            :size,
                            :mime,
                            :uploaded_by,
                            :person_id
                        )
                    """),
                    {**sproc_params, 'person_id': person_id}
                ).fetchone()
            else:  # family
                sproc_result = conn.execute(
                    text("""
                        call AddFileForFamily(
                            :path,
                            :filename,
                            :original,
                            :doctype,
                            :year,
                            :size,
                            :mime,
                            :uploaded_by,
                            :father_id,
                            :mother_id
                        )
                    """),
                    {**sproc_params, 'father_id': father_id, 'mother_id': mother_id}
                ).fetchone()

            if not sproc_result:
                raise RuntimeError("No result returned from file upload stored procedure")

            result_dict = sproc_result._asdict() if hasattr(sproc_result, '_asdict') else dict(sproc_result)
            completed_ok = result_dict.get('CompletedOk')
            result_code = result_dict.get('Result')
            file_id = result_dict.get('FileID')

            if completed_ok != 0 or file_id is None:
                raise RuntimeError(
                    f"Stored procedure failed (CompletedOk={completed_ok}, Result={result_code}, FileID={file_id})"
                )
        
        logger.info(f"File metadata saved to database: file_id={file_id}")
        
        return {
            "success": True,
            "file_id": file_id,
            "filename": filename,
            "original_filename": original_filename,
            "file_size": file_size,
            "mime_type": mime_type,
            "document_type": document_type,
            "year": year
        }
        
    except HTTPException:
        if written_file_path and written_file_path.exists():
            try:
                written_file_path.unlink()
                logger.warning(f"Removed uploaded file after failed DB operation: {written_file_path}")
            except Exception as cleanup_error:
                logger.error(f"Failed to cleanup file after HTTP error: {cleanup_error}")
        raise
    except Exception as e:
        if written_file_path and written_file_path.exists():
            try:
                written_file_path.unlink()
                logger.warning(f"Removed uploaded file after failed DB operation: {written_file_path}")
            except Exception as cleanup_error:
                logger.error(f"Failed to cleanup file after upload failure: {cleanup_error}")
        logger.error(f"Error uploading file: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/api/files/{file_id}")
async def download_file(request: Request, file_id: int) -> FileResponse:
    """
    Download a file by its ID.
    
    Args:
        file_id: Unique file identifier
        
    Returns:
        File response with appropriate headers
    """
    try:
        # Get file metadata from database
        with engine.connect() as conn:
            result = conn.execute(
                text("call GetFileMeta(:file_id)"),
                {'file_id': file_id}
            ).fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail="File not found")
            
            file_path_rel = result.FilePath
            filename = result.FileName
            original_filename = result.OriginalFileName or filename
            mime_type = result.MimeType or 'application/octet-stream'

            # Fallback for older uploads where browser sent no reliable MIME type.
            if mime_type == 'application/octet-stream':
                guessed_mime, _ = mimetypes.guess_type(original_filename)
                if guessed_mime:
                    mime_type = guessed_mime
        
        # Build full path
        full_path = Path(STORAGE_BASE_PATH) / file_path_rel
        
        if not full_path.exists():
            logger.error(f"File not found on disk: {full_path}")
            raise HTTPException(status_code=404, detail="File not found on disk")
        
        # Force inline rendering in browser preview window instead of download behavior.
        safe_filename = str(original_filename).replace('"', '')
        return FileResponse(
            path=str(full_path),
            media_type=mime_type,
            headers={"Content-Disposition": f'inline; filename="{safe_filename}"'}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Download failed")


@app.get("/api/files/{file_id}/thumbnail")
async def get_file_thumbnail(request: Request, file_id: int) -> StreamingResponse:
    """
    Get a thumbnail for an image file.
    Generates 200x200px thumbnail on-the-fly.
    
    Args:
        file_id: Unique file identifier
        
    Returns:
        Thumbnail image as streaming response
    """
    try:
        # Get file metadata from database
        with engine.connect() as conn:
            result = conn.execute(
                text("call GetFileMeta(:file_id)"),
                {'file_id': file_id}
            ).fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail="File not found")
            
            file_path_rel = result.FilePath
            mime_type = result.MimeType or 'application/octet-stream'
        
        # Check if it's an image
        if not mime_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File is not an image")
        
        # Build full path
        full_path = Path(STORAGE_BASE_PATH) / file_path_rel
        
        if not full_path.exists():
            raise HTTPException(status_code=404, detail="File not found on disk")
        
        # Generate thumbnail
        try:
            with Image.open(full_path) as img:
                # Convert to RGB if necessary (for PNG with transparency, etc.)
                if img.mode in ('RGBA', 'LA', 'P'):
                    # Create white background
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Create thumbnail (maintains aspect ratio)
                img.thumbnail((200, 200), Image.Resampling.LANCZOS)
                
                # Save to bytes
                img_byte_arr = io.BytesIO()
                img.save(img_byte_arr, format='JPEG', quality=85)
                img_byte_arr.seek(0)
                
                return StreamingResponse(
                    img_byte_arr,
                    media_type="image/jpeg",
                    headers={"Cache-Control": "public, max-age=31536000"}  # Cache for 1 year
                )
        except Exception as img_error:
            logger.error(f"Error generating thumbnail for file {file_id}: {img_error}")
            raise HTTPException(status_code=500, detail="Failed to generate thumbnail")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting thumbnail for file {file_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Thumbnail generation failed")


@app.get("/api/person/{person_id}/files")
async def get_person_files(request: Request, person_id: int) -> List[Dict[str, Any]]:
    """
    Get all files associated with a person.
    
    Args:
        person_id: Unique person identifier
        
    Returns:
        List of file metadata dictionaries
    """
    try:
        with engine.connect() as conn:
            results = conn.execute(
                text("call GetPersonFiles(:person_id)"),
                {'person_id': person_id}
            ).fetchall()
            
            files = []
            for row in results:
                files.append({
                    'file_id': row.FileID,
                    'filename': row.FileName,
                    'original_filename': row.OriginalFileName,
                    'document_type': row.DocumentType,
                    'year': row.Year,
                    'file_size': row.FileSize,
                    'mime_type': row.MimeType,
                    'created_at': row.CreatedAt.isoformat() if row.CreatedAt else None,
                    'uploaded_by': row.UploadedBy
                })
            
            return files
            
    except Exception as e:
        logger.error(f"Error getting person files for person {person_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve files")


@app.get("/api/family/{father_id}/{mother_id}/files")
async def get_family_files(
    request: Request,
    father_id: int,
    mother_id: int
) -> List[Dict[str, Any]]:
    """
    Get all files associated with a family (parent couple).
    
    Args:
        father_id: Father's person ID
        mother_id: Mother's person ID
        
    Returns:
        List of file metadata dictionaries
    """
    try:
        with engine.connect() as conn:
            results = conn.execute(
                text("call GetFamilyFiles(:father_id, :mother_id)"),
                {'father_id': father_id, 'mother_id': mother_id}
            ).fetchall()
            
            files = []
            for row in results:
                files.append({
                    'file_id': row.FileID,
                    'filename': row.FileName,
                    'original_filename': row.OriginalFileName,
                    'document_type': row.DocumentType,
                    'year': row.Year,
                    'file_size': row.FileSize,
                    'mime_type': row.MimeType,
                    'created_at': row.CreatedAt.isoformat() if row.CreatedAt else None,
                    'uploaded_by': row.UploadedBy
                })
            
            return files
            
    except Exception as e:
        logger.error(f"Error getting family files for family {father_id}/{mother_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve files")

