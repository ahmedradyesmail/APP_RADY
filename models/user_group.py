from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class UserGroup(Base):
    """مجموعة فرز مشتركة — أعضاؤها يُقارَنون ضد اتحاد ملفاتهم الكبيرة."""

    __tablename__ = "user_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    max_stored_large_rows: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    users = relationship("User", back_populates="group")
