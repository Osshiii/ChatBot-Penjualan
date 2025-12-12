# -*- coding: utf-8 -*-
"""
FastAPI Main Application
Jewelry Sales AI Chatbot with NLP and API endpoints
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Optional
from pathlib import Path

from app.sales_api import router as sales_router
from app.chatbot import create_bot

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

# Include API routers
app.include_router(sales_router)


# Root endpoint
@app.get("/")
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
def chat(query: str = Query(..., description="Natural language query")):
    """
    Chat endpoint: Process natural language queries using NLP and return results.
    
    Examples:
    - "Tampilkan penjualan MP000197 bulan 4 tahun 2022"
    - "Ringkasan penjualan per lokasi"
    - "Berapa penjualan dengan berat 5 sampai 10?"
    
    Args:
        query: Natural language question/command in Indonesian
        
    Returns:
        JSON response with parsed query, results, and explanation
    """
    try:
        bot_instance = get_bot()
        response = bot_instance.process_message(query)
        
        return {
            "status": "success",
            "query": query,
            "response": response,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


@app.get("/help")
def help_endpoint():
    """
    Get help information and examples.
    """
    try:
        bot_instance = get_bot()
        help_response = bot_instance._handle_help_query()
        
        return {
            "status": "success",
            "help": help_response["message"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting help: {str(e)}")


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
app.mount("/ui", StaticFiles(directory=str(STATIC_PATH), html=True), name="ui")


# Root redirect to UI
@app.get("/")
def root():
    """Redirect to chat UI"""
    return RedirectResponse(url="/ui/")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)