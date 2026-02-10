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
    "http://localhost:5174,http://localhost:3310"
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
def ping_api(timestampFE: datetime) -> List[Dict[str, datetime]]:
    return [{"FE request time": timestampFE, "MW request time": datetime.now()}]

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
                result[-1]['datetimeMWanswer'] = datetime.now()
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