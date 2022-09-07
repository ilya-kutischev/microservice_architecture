from pydantic import BaseModel, Field


class DataModel(BaseModel):
    datasearch: str = Field(...)
    count: int = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "datasearch": "Jane",
                "count": "1"
            }
        }