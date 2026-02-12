import os
import logging
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text

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

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/")
def read_root() -> Dict[str, str]:
    return {"Hello visitor": "The Familiez Fastapi api lives!"}

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
                        PersonIsMale
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


@app.post("/UpdatePerson")
def update_person(
    person_data: Dict[str, Any]
) -> Dict[str, Any]:
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
            connection.commit()
            return {"success": True}
    except Exception as e:
        logger.error(f"Error in update_person: {e}")
        raise HTTPException(status_code=500, detail="Update failed")