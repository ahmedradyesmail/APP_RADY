from pydantic import BaseModel, Field


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=128)
    is_admin: bool = False
    group_id: int | None = None
    max_stored_large_rows: int = Field(ge=1, le=200_000_000)


class UserOut(BaseModel):
    id: int
    username: str
    is_admin: bool
    is_active: bool
    device_id: str | None
    group_id: int | None = None
    group_name: str | None = None
    max_stored_large_rows: int | None = None
    used_stored_large_rows: int = 0

    class Config:
        from_attributes = True


class UserActiveUpdate(BaseModel):
    is_active: bool


class CreateGroupRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    max_stored_large_rows: int = Field(ge=1, le=200_000_000)


class GroupOut(BaseModel):
    id: int
    name: str
    max_stored_large_rows: int | None = None
    used_stored_large_rows: int = 0

    class Config:
        from_attributes = True


class UserGroupUpdate(BaseModel):
    group_id: int | None = None


class UserLargeRowsLimitUpdate(BaseModel):
    max_stored_large_rows: int = Field(ge=1, le=200_000_000)


class GroupLargeRowsLimitUpdate(BaseModel):
    max_stored_large_rows: int = Field(ge=1, le=200_000_000)
