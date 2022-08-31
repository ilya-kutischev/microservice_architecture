from pydantic import BaseModel, Field


class PostSchema(BaseModel):
    email: str = Field(...)
    password: str = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "email": "admin@example.com",
                "password": "adminadmin",
                "info": "Information"
            }
        }


class UserSchema(BaseModel):
    fullname: str = Field(...)
    email: str = Field(...)
    password: str = Field(...)

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "fullname": "sus",
                "email": "admin@example.com",
                "password": "adminadmin"
            }
        }


class UserLoginSchema(BaseModel):
    email: str = Field(...)
    password: str = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "email": "admin@example.com",
                "password": "adminadmin"
            }
        }