from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from db import Base


class User(Base):
    __tablename__ = "users"
    fullname = Column(String, default="", nullable=True)
    email = Column(String, primary_key=True, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    # is_active = Column(Boolean, default=True)

