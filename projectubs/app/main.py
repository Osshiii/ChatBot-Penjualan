from fastapi import FastAPI
from app.sales_api import router as sales_router
app = FastAPI()
app.include_router(sales_router)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}