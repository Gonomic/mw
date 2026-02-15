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


@app.post("/AddPerson")
def add_person(
    person_data: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        with engine.connect() as connection:
            full_name = f"{person_data.get('PersonGivvenName', '')} {person_data.get('PersonFamilyName', '')}".strip()
            logger.info(f"AddPerson called with: {full_name}")
            
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
                        NULL,
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
                        "isMale": person_data.get('PersonIsMale', 1),
                        "motherId": person_data.get('MotherId') or None,
                        "fatherId": person_data.get('FatherId') or None,
                    }
                )
                results = results_proxy.fetchall()
                logger.info(f"AddPerson procedure completed, results count: {len(results) if results else 0}")
                
                if results and len(results) > 0:
                    result_dict = results[0]._asdict() if hasattr(results[0], '_asdict') else dict(results[0])
                    logger.info(f"First result keys: {list(result_dict.keys())}, values: {result_dict}")
                    
                    # Check if procedure succeeded (CompletedOk == 0)
                    if result_dict.get('CompletedOk') != 0:
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
            logger.info(f"Fetching inserted person: {person_data.get('PersonGivvenName')} {person_data.get('PersonFamilyName')}")
            
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
                logger.info(f"Found inserted person with ID: {person_dict.get('PersonID')}")
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
    person_data: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        with engine.connect() as connection:
            person_id = person_data.get('personId')
            mother_id = person_data.get('MotherId') or None
            father_id = person_data.get('FatherId') or None
            partner_id = person_data.get('PartnerId') or None
            
            logger.info(f"DeletePerson called for personId: {person_id}")
            
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
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

