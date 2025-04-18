from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
import uuid
import gc
app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173/"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ In-memory session store (use Redis/db in production)
session_store = {}

# ✅ Test DB Connection
@app.post("/test-db-connection")
async def test_db_connection(
    user: str = Form(...),
    password: str = Form(...),
    host: str = Form(...),
    port: int = Form(...),
    db: str = Form(...)
):
    try:
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(conn_str, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        session_key = str(uuid.uuid4())
        session_store[session_key] = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "db": db
        }

        return {
            "success": True,
            "message": "✅ Connection successful!",
            "session_key": session_key
        }

    except Exception as ex:
        return JSONResponse(status_code=500, content={
            "success": False,
            "message": f"❌ Connection failed: {str(ex)}"
        })


# ✅ Upload Excel and load to DB
@app.post("/upload-excel-to-db")
async def upload_excel_to_db(
    session_key: str = Form(...),
    file: UploadFile = File(...),  # ✅ Important fix here
    schema: str = Form(...),
    table_name: str = Form(...)
):
    try:
        if session_key not in session_store:
            raise HTTPException(status_code=403, detail="❌ Invalid or expired session key.")

        db_info = session_store[session_key]
        conn_str = f"postgresql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['db']}"
        engine = create_engine(conn_str, pool_pre_ping=True, pool_size=5, max_overflow=10)

        temp_path = f"temp_{uuid.uuid4().hex}_{file.filename}"
        with open(temp_path, "wb") as f:
            f.write(await file.read())

        df = pd.read_excel(temp_path, engine='openpyxl', dtype=str)
        print(f"📊 Loaded rows: {len(df)}")

        chunksize = 5000
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            chunk.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists='append' if i > 0 else 'replace',
                index=False
            )
            del chunk
            gc.collect()

        os.remove(temp_path)

        return {"success": True, "message": "✅ Excel uploaded and inserted into DB."}

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="❌ File not found.")
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"❌ Database error: {str(e)}")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"❌ Error: {str(ex)}")


# ✅ Preview Table Data
@app.get("/preview-table")
async def preview_table(
    session_key: str = Query(...),
    schema: str = Query(...),
    table_name: str = Query(...)
):
    try:
        if session_key not in session_store:
            raise HTTPException(status_code=403, detail="❌ Invalid or expired session.")

        db_info = session_store[session_key]
        conn_str = f"postgresql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['db']}"
        engine = create_engine(conn_str, pool_pre_ping=True)

        query = f'SELECT * FROM "{schema}"."{table_name}" LIMIT 10'
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result]

        return {
            "success": True,
            "data": rows,
            "message": "✅ Preview fetched successfully."
        }

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"❌ SQL error: {str(e)}")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"❌ Error fetching preview: {str(ex)}")


# ✅ Root Health Check
@app.get("/")
def root():
    return {"message": "✅ FastAPI is up and running"}

