# -*- coding: utf-8 -*-
"""
FastAPI Main Application
Jewelry Sales AI Chatbot with NLP and API endpoints
"""

from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # .../projectubs
load_dotenv(BASE_DIR / ".env")          # kalau .env kamu taruh di projectubs
load_dotenv(BASE_DIR.parent / ".env")   # fallback kalau .env kamu taruh di root repo

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Optional
from pathlib import Path
from pydantic import BaseModel

from app.sales_api import router as sales_router
from app.chatbot import create_bot

from fastapi.responses import StreamingResponse
import io
import csv
from datetime import datetime

# Initialize FastAPI app
app = FastAPI(
    title="Jewelry Sales AI Chatbot",
    description="AI-powered chatbot for querying jewelry sales data with NLP",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize chatbot
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "penjualan.db"
bot = None

def get_bot():
    """Get or create chatbot instance"""
    global bot
    if bot is None:
        try:
            bot = create_bot(str(DB_PATH))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to initialize chatbot: {str(e)}")
    return bot

# Include API routers - disabled for now
# try:
#     app.include_router(sales_router)
# except Exception as e:
#     print(f"Warning: Could not include sales_router: {e}")


# Root endpoint - API info
@app.get("/api/info")
def read_root():
    """Root endpoint with API info"""
    return {
        "service": "Jewelry Sales AI Chatbot API",
        "version": "1.0.0",
        "endpoints": {
            "chat": "/chat - Natural language query processing",
            "sales": "/api/sales - Query sales data with filters",
            "summary": "/api/sales/summary - Get aggregated statistics",
            "codes": "/api/sales/codes - Get unique code values",
            "stats": "/api/sales/stats - Get database statistics",
            "help": "/help - Show help information",
            "docs": "/docs - Interactive API documentation",
        }
    }


@app.get("/chat")
def chat(
    query: str = Query(..., description="Natural language query"),
):
    """
    Process natural language query and return overview (max 10 rows)
    """
    try:
        bot_instance = get_bot()

        # Always show only 10 rows for overview
        result = bot_instance.process_message(query, limit=10, offset=0)

        return {
            "status": "success",
            "query": query,
            **result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


# Add new endpoint for full CSV download
# Update endpoint /chat/download di main.py dengan logging

@app.get("/chat/download")
def download_chat_results(
    query: str = Query(..., description="Natural language query for full data export"),
):
    """
    Download complete query results as CSV (all rows, no limit)
    """
    try:
        bot_instance = get_bot()
        
        print(f"CSV Download Request:")
        print(f"   Query: {query}")
        print(f"   Requesting limit: 100000")
        
        # Get ALL results without limit (set high limit)
        result = bot_instance.process_message(query, limit=100000, offset=0)
        
        print(f"   Result keys: {list(result.keys())}")
        print(f"   Data length: {len(result.get('data', []))}")
        print(f"   Total count: {result.get('count', 'N/A')}")
        
        if not result.get("data"):
            raise HTTPException(
                status_code=404, 
                detail="Tidak ada data ditemukan untuk query ini"
            )
        
        data = result["data"]
        
        if len(data) == 0:
            raise HTTPException(
                status_code=404,
                detail="Query tidak menghasilkan data"
            )
        
        print(f"Creating CSV with {len(data)} rows")
        
        # Create CSV in memory
        output = io.StringIO()
        
        # Write CSV with proper encoding
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        
        # Prepare response
        output.seek(0)
        csv_content = output.getvalue()
        
        # Count actual lines in CSV
        csv_lines = len(csv_content.strip().split('\n'))
        print(f"CSV created with {csv_lines} lines (including header)")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"jewelry_sales_{timestamp}.csv"
        
        # Return as streaming response
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv;charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Type": "text/csv; charset=utf-8",
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"❌ Error in CSV download:")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, 
            detail=f"Error downloading CSV: {str(e)}"
        )


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    """
    try:
        # Verify database connection
        import sqlite3
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM penjualan")
        count = cursor.fetchone()[0]
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "records": count,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "detail": exc.detail,
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "detail": "Internal server error",
            "error": str(exc),
        },
    )


# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize bot on startup"""
    try:
        bot_instance = get_bot()
        print("✓ Chatbot initialized successfully")
    except Exception as e:
        print(f"❌ Error initializing chatbot: {e}")


# Mount static files
STATIC_PATH = Path(__file__).resolve().parent / "static"

@app.get("/")
def root():
    """Serve index.html"""
    return FileResponse(str(STATIC_PATH / "index.html"))

# Mount static files after root endpoint
app.mount("/static", StaticFiles(directory=str(STATIC_PATH)), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)