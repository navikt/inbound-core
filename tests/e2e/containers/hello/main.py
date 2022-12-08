import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/status")
def read_root():
    return True


@app.get("/hello")
def read_root():
    return "Hello"


if __name__ == "__main__":
    PORT = 3003
    uvicorn.run(app, host="0.0.0.0", port=PORT)
