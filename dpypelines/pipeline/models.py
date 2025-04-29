from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field

from dpypelines.pipeline.messages.utils import get_mimetype


class DatasetVersion(BaseModel):
    edition_title: str
    distributions: List["Distribution"]
    release_date: datetime
    quality_designation: Optional[str] = None
    usage_notes: Optional[List["UsageNote"]] = Field(default_factory=list)
    alerts: Optional[List["Alert"]] = Field(default_factory=list)


class Metadata(DatasetVersion):
    dataset_id: str
    edition: str


class Manifest(BaseModel):
    metadata_file: str
    submission_contacts: List["SubmissionContact"]


class Distribution(BaseModel):
    title: str
    format: str
    file: str
    download_url: str = Field(default=None, init=False)
    media_type: str = Field(default=None, init=False)

    def model_post_init(self, __context):
        self.download_url = f"https://download.ons.gov.uk/{self.file}"
        self.media_type = get_mimetype(f".{self.format}")


class Alert(BaseModel):
    type: str
    description: str


class UsageNote(BaseModel):
    title: str
    note: str


class SubmissionContact(BaseModel):
    email: EmailStr
