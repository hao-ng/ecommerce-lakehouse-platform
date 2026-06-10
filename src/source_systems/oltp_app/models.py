from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, MetaData, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Model(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Product(Model):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    gender: Mapped[str] = mapped_column(String(6))
    master_category: Mapped[str] = mapped_column(String(20))
    sub_category: Mapped[str] = mapped_column(String(30))
    article_type: Mapped[str] = mapped_column(String(30))
    base_colour: Mapped[Optional[str]] = mapped_column(String(20))
    season: Mapped[Optional[str]] = mapped_column(String(6))
    year: Mapped[Optional[int]] = mapped_column()
    usage: Mapped[Optional[str]] = mapped_column(String(15))
    product_display_name: Mapped[Optional[str]] = mapped_column(String(100))

    def __repr__(self) -> str:
        return f"Product(id={self.id}"


class Customer(Model):
    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column(primary_key=True)
    first_name: Mapped[str] = mapped_column(String(20))
    last_name: Mapped[str] = mapped_column(String(20))
    username: Mapped[str] = mapped_column(String(50))
    email: Mapped[str] = mapped_column(String(100))
    gender: Mapped[str] = mapped_column(String(1))
    birthdate: Mapped[str] = mapped_column(Date)
    device_type: Mapped[str] = mapped_column(String(7))
    device_id: Mapped[str] = mapped_column(String(50))
    device_version: Mapped[str] = mapped_column(String(50))
    home_location_lat: Mapped[float] = mapped_column()
    home_location_long: Mapped[float] = mapped_column()
    home_location: Mapped[str] = mapped_column(String(30))
    home_country: Mapped[str] = mapped_column(String(30))
    first_join_date: Mapped[date] = mapped_column(Date)

    def __repr__(self) -> str:
        return f"Customer(customer_id={self.customer_id}, username={self.username})"


class Transaction(Model):
    __tablename__ = "transactions"

    booking_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    customer_id: Mapped[int] = mapped_column()
    session_id: Mapped[str] = mapped_column(String(50))
    product_metadata: Mapped[str] = mapped_column(JSONB)
    payment_method: Mapped[str] = mapped_column(String(11))
    payment_status: Mapped[str] = mapped_column(String(7))
    promo_amount: Mapped[int] = mapped_column()
    promo_code: Mapped[Optional[str]] = mapped_column(String(20))
    shipment_fee: Mapped[int] = mapped_column()
    shipment_date_limit: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    shipment_location_lat: Mapped[float] = mapped_column()
    shipment_location_long: Mapped[float] = mapped_column()
    total_amount: Mapped[int] = mapped_column()

    def __repr__(self) -> str:
        return (
            f"Transaction(booking_id={self.booking_id}, session_id={self.session_id})"
        )
