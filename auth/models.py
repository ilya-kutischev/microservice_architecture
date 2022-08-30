from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Users(Base):
    __tablename__ = 'users'
    id  = Column(Integer, primary_key=True, index=True)
    email = Column("email", String(40), unique=True)
    name = Column("name", String(100))
    hashed_password = Column("hashed_password", String())
    is_active = Column("is_active", Boolean(), default=True)
