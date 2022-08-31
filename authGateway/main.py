from fastapi import FastAPI, Body, Depends
import schema, db_models
from db import SessionLocal, engine, Base
from sqlalchemy.orm import Session
from auth.auth_bearer import JWTBearer
from auth.auth_handler import signJWT, decodeJWT
import passlib
from passlib.context import CryptContext

password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


users = []

app = FastAPI()
Base.metadata.create_all(bind=engine)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_user(email, password):
    for user in users:
        if user.email == email and user.password == password:
            return True
    return False


# route handlers

@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to your blog!."}


@app.post("/posts", tags=["posts"])
async def add_post(post: schema.PostSchema, info: str = Depends(decodeJWT)) -> dict:
    print(f"user info is {info} ========================================")
    # ТУТ ПОСЛЕ АВТОРИЗАЦИИ РЕДИРЕКТ ЧЕРЕЗ КАФКУ
    email = info["email"]
    print(email)
    # post.id = len(posts) + 1

    # posts.append(post.dict())
    return {
        "data": email
    }


@app.post("/user/signup", tags=["user"])
async def create_user(user: schema.UserSchema = Body(...), db: Session = Depends(get_db)):
    hashed_password = password_context.hash(user.password)
    db_user = db_models.User(fullname=user.fullname,email=user.email, hashed_password=hashed_password)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return signJWT(user.email)


@app.post("/user/login", tags=["user"])
async def user_login(user: schema.UserLoginSchema = Body(...),db: Session = Depends(get_db)):
    try:
        user = db.query(db_models.User).filter(db_models.User.email == user.email).first()
        print(user)
        return signJWT(user.email)

    except:
        print("Unable to find user")
        return {
            "error": "Wrong login details!"
        }

    # if check_user(user.email, user.password):
    #     return signJWT(user.email)
