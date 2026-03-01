import os
import logging
from datetime import datetime, date
from typing import List, Dict, Any

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

from auth import verify_sso_token, exchange_authorization_code, resolve_ldap_role_from_claims, require_admin_role

logger = logging.getLogger(__name__)

# Configuration from environment variables
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://HumansService:XHHxECL54EjvhhPSBLMU@localhost:3306/humans"
)
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://localhost:3310"
).split(",")

PUBLIC_PATHS = {
    "/",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/auth/callback",
    "/auth/discovery",
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

RELEASE_TABLES = {
    "fe": ("fe_releases", "fe_release_changes"),
    "mw": ("mw_releases", "mw_release_changes"),
    "be": ("be_releases", "be_release_changes"),
}

def fetch_releases(component: str) -> List[Dict[str, Any]]:
    if component not in RELEASE_TABLES:
        raise HTTPException(status_code=400, detail="Invalid component. Use fe, mw, or be.")

    releases_table, changes_table = RELEASE_TABLES[component]
    query = text(f"""
        SELECT
            r.ReleaseID,
            r.ReleaseNumber,
            DATE_FORMAT(r.ReleaseDate, '%Y-%m-%d %H:%i:%s') AS ReleaseDate,
            r.Description,
            c.ChangeID,
            c.ChangeDescription,
            c.ChangeType
        FROM {releases_table} r
        LEFT JOIN {changes_table} c ON c.ReleaseID = r.ReleaseID
        ORDER BY r.ReleaseDate DESC, r.ReleaseID DESC, c.ChangeID ASC
    """)

    with engine.connect() as connection:
        rows = connection.execute(query).fetchall()

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
    
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        logger.warning(f"[Auth] Missing or invalid auth header for {request.url.path}. Header: {auth_header[:50] if auth_header else 'NONE'}")
        return create_cors_json_response(401, {"detail": "Missing or invalid token"}, origin)

    token = auth_header.split(" ", 1)[1].strip()
    try:
        claims = verify_sso_token(token)
        request.state.user = claims
        request.state.user_access = resolve_ldap_role_from_claims(claims)
    except HTTPException as exc:
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
    """
    code = request_data.get("code", "").strip()
    
    if not code:
        logger.error("OAuth callback missing authorization code")
        raise HTTPException(status_code=400, detail="Missing code")
    
    try:
        access_token = exchange_authorization_code(code)
        return {"access_token": access_token}
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
            # Use simple SELECT instead of stored procedure to avoid missing columns
            results_proxy = connection.execute(
                text("""
                    SELECT 
                        PersonID,
                        PersonGivvenName,
                        PersonFamilyName,
                        PersonDateOfBirth,
                        PersonPlaceOfBirth,
                        PersonDateOfDeath,
                        PersonPlaceOfDeath,
                        PersonIsMale,
                        DATE_FORMAT(Timestamp,'%Y-%m-%d %T') as Timestamp
                    FROM persons
                    WHERE PersonID = :personId
                """),
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
            # Query partners from relations table
            results_proxy = connection.execute(
                text("""
                    SELECT DISTINCT
                        p.PersonID,
                        p.PersonGivvenName,
                        p.PersonFamilyName,
                        p.PersonDateOfBirth,
                        p.PersonDateOfDeath
                    FROM relations r1
                    JOIN relationnames rn ON r1.RelationName = rn.RelationnameID
                    JOIN persons p ON r1.RelationWithPerson = p.PersonID
                    WHERE r1.RelationPerson = :personId
                    AND rn.RelationnameName IN ('Partner', 'Echtgenoot', 'Echtgenote')
                """),
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
            results_proxy = connection.execute(
                text("""call ChangePerson(
                    :personId, 
                    :givvenName, 
                    :familyName, 
                    :dateOfBirth,
                    :placeOfBirth,
                    :dateOfDeath,
                    :placeOfDeath
                )"""),
                {
                    "personId": person_id,
                    "givvenName": person_data.get('PersonGivvenName', ''),
                    "familyName": person_data.get('PersonFamilyName', ''),
                    "dateOfBirth": person_data.get('PersonDateOfBirth'),
                    "placeOfBirth": person_data.get('PersonPlaceOfBirth'),
                    "dateOfDeath": person_data.get('PersonDateOfDeath'),
                    "placeOfDeath": person_data.get('PersonPlaceOfDeath')
                }
            )
            results = results_proxy.fetchall()
            if results and len(results) > 0:
                result_dict = results[0]._asdict() if hasattr(results[0], '_asdict') else dict(results[0])
                completed_ok = result_dict.get('CompletedOk')
                if completed_ok is not None and completed_ok != 0:
                    logger.warning(f"ChangePerson returned CompletedOk: {completed_ok}")
                    connection.rollback()
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
            full_name = f"{person_data.get('PersonGivvenName', '')} {person_data.get('PersonFamilyName', '')}".strip()

            is_male = person_data.get('PersonIsMale')
            
            # Call AddPerson procedure
            try:
                results_proxy = connection.execute(
                    text("""call AddPerson(
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
                        0,
                        0
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
            
            # Now fetch the inserted person by name and other details
            select_results = connection.execute(
                text("""
                    SELECT PersonID, PersonGivvenName, PersonFamilyName, PersonDateOfBirth, 
                           PersonPlaceOfBirth, PersonDateOfDeath, PersonPlaceOfDeath, PersonIsMale
                    FROM persons 
                    WHERE PersonGivvenName = :givvenName AND PersonFamilyName = :familyName
                    ORDER BY PersonID DESC
                    LIMIT 1
                """),
                {
                    "givvenName": person_data.get('PersonGivvenName', ''),
                    "familyName": person_data.get('PersonFamilyName', '')
                }
            ).fetchall()
            
            if select_results and len(select_results) > 0:
                person_dict = select_results[0]._asdict() if hasattr(select_results[0], '_asdict') else dict(select_results[0])
                return {
                    "success": True, 
                    "personId": person_dict.get('PersonID')
                }
            else:
                logger.error("Could not find inserted person after AddPerson procedure")
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

