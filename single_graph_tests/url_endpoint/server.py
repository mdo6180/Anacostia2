from fastapi import FastAPI
from fastapi.responses import JSONResponse


app = FastAPI()

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify that the server is running.
    Returns a JSON response with a status message.
    """
    return JSONResponse(content={"status": "ok"}, status_code=200)


id = 0

@app.get("/data")
async def data():
    """
    Data endpoint to return some sample data.
    Returns a JSON response with a list of items.
    """
    global id

    sample_data = {
        "items": [
            {"id": id, "name": f"Item {id}"},
            {"id": id + 1, "name": f"Item {id + 1}"},
            {"id": id + 2, "name": f"Item {id + 2}"}
        ]
    }

    id += 3  # Increment the id for the next request

    return JSONResponse(content=sample_data, status_code=200)

@app.get("/data/{item_id}")
async def get_item(item_id: int):
    """
    Endpoint to return a specific item by its ID.
    Returns a JSON response with the item details if found, otherwise returns a 404 error.
    """
    if 0 <= item_id < id:
        item = {"id": item_id, "name": f"Item {item_id}"}
        return JSONResponse(content=item, status_code=200)
    else:
        return JSONResponse(content={"error": "Item not found"}, status_code=404)