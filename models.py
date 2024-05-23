import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy.dialects.postgresql import JSON

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5431")

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class SwapiPeople(Base):
    __tablename__ = 'swapi_people'

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column()
    eye_color: Mapped[str] = mapped_column()
    films: Mapped[dict] = mapped_column(JSON)
    gender: Mapped[str] = mapped_column()
    hair_color: Mapped[str] = mapped_column()
    height: Mapped[str] = mapped_column()
    homeworld: Mapped[str] = mapped_column()
    mass: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    skin_color: Mapped[str] = mapped_column()
    species: Mapped[dict] = mapped_column(JSON)
    starships: Mapped[dict] = mapped_column(JSON)
    vehicles: Mapped[dict] = mapped_column(JSON)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

