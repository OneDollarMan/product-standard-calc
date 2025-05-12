from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Integer, Double, Text


class Base(DeclarativeBase):
    pass


# Входные данные
class Product(Base):
    __tablename__ = "products"

    storage_id: Mapped[str] = mapped_column(Text, primary_key=True)
    category_id: Mapped[str] = mapped_column(Text, primary_key=True)
    product_id: Mapped[str] = mapped_column(Text, primary_key=True)
    rating: Mapped[int] = mapped_column(Integer)
    sale_pcs: Mapped[float] = mapped_column(Double)
    sale_share: Mapped[float] = mapped_column(Double)
    cumulative_share: Mapped[float] = mapped_column(Double)


# Входные данные
class Equipment(Base):
    __tablename__ = "equipments"

    storage_id: Mapped[str] = mapped_column(Text, primary_key=True)
    equipment_type: Mapped[str] = mapped_column(Text, primary_key=True)
    capacity: Mapped[int] = mapped_column(Integer)


# Входные данные
class Category(Base):
    __tablename__ = "categories"

    category_id: Mapped[str] = mapped_column(Text, primary_key=True)
    equipment_type: Mapped[str] = mapped_column(Text, primary_key=True)


# Промежуточные данные
class AggData(Base):
    __tablename__ = "agg_data"

    storage_id: Mapped[str] = mapped_column(Text, primary_key=True)
    category_id: Mapped[str] = mapped_column(Text, primary_key=True)
    product_id: Mapped[str] = mapped_column(Text, primary_key=True)
    equipment_type: Mapped[str] = mapped_column(Text, primary_key=True)
    rating: Mapped[int] = mapped_column(Integer)
    sale_pcs: Mapped[float] = mapped_column(Double)
    sale_share: Mapped[float] = mapped_column(Double)
    cumulative_share: Mapped[float] = mapped_column(Double)
    capacity: Mapped[int] = mapped_column(Integer)


# Промежуточные данные
class SelectedProduct(Base):
    __tablename__ = 'selected_products'

    storage_id: Mapped[str] = mapped_column(Text, primary_key=True)
    category_id: Mapped[str] = mapped_column(Text, primary_key=True)
    product_id: Mapped[str] = mapped_column(Text, primary_key=True)
    rating: Mapped[int] = mapped_column(Integer)
    sale_pcs: Mapped[float] = mapped_column(Double)
    sale_share: Mapped[float] = mapped_column(Double)
    cumulative_share: Mapped[float] = mapped_column(Double)


# Выходные данные - эталон
class ProductStandard(Base):
    __tablename__ = 'product_standards'

    storage_id: Mapped[str] = mapped_column(Text, primary_key=True)
    category_id: Mapped[str] = mapped_column(Text, primary_key=True)
    amount: Mapped[int] = mapped_column(Integer)