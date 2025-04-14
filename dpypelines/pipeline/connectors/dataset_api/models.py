from typing import List, Optional

from pydantic import BaseModel


class Link(BaseModel):
    href: str
    id: Optional[str] = None


class Links(BaseModel):
    editions: Optional[Link] = None
    latest_version: Optional[Link] = None
    self: Optional[Link] = None


class Contact(BaseModel):
    name: str
    email: Optional[str] = None
    telephone: Optional[str] = None


class Publisher(BaseModel):
    href: str
    name: str
    type: str


class DatasetVersion(BaseModel):
    contacts: List[Contact]
    description: str
    keywords: List[str] = []
    id: str
    links: Links
    next_release: str
    publisher: Optional[Publisher] = None
    state: str
    title: str
    type: str
    topics: Optional[List[str]] = None


class Dataset(BaseModel):
    id: str
    current: Optional[DatasetVersion] = None
    next: DatasetVersion


class EditionVersion(BaseModel):
    edition: str
    edition_title: Optional[str] = None
    version: int


class Edition(BaseModel):
    current: Optional[EditionVersion] = None
    next: Optional[EditionVersion] = None


class DatasetsGETResponse(BaseModel):
    items: List
    count: int
    total_count: int
    offset: int
    limit: int


class GetDatasetsResponse(DatasetsGETResponse):
    items: List[Dataset]


class GetDatasetEditionsResponse(BaseModel):
    items: List[Edition]


class DatasetApiConfig:
    def __init__(self, url: str, auth_token: str):
        self.url = url
        self.auth_token = auth_token
