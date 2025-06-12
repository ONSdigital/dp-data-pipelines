from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field

from dpypelines.pipeline.messages.utils import get_mimetype


class Distribution(BaseModel):
    title: str
    format: str
    file: str
    download_url: Optional[str] = Field(default=None, init=False)
    media_type: Optional[str] = Field(default=None, init=False)

    def model_post_init(self, __context):
        self.download_url = f"https://download.ons.gov.uk/{self.file}"
        self.media_type = get_mimetype(f".{self.format}")


class Alert(BaseModel):
    type: str
    description: str


class UsageNote(BaseModel):
    title: Optional[str] = None
    note: Optional[str] = None


class SubmissionContact(BaseModel):
    email: EmailStr


class DatasetVersion(BaseModel):
    edition_title: Optional[str] = None
    quality_designation: Optional[str] = None
    usage_notes: Optional[List[UsageNote]] = Field(default_factory=list)
    alerts: Optional[List[Alert]] = Field(default_factory=list)
    distributions: List[Distribution]
    release_date: str


class Metadata(DatasetVersion):
    dataset_id: str
    edition: str


class MinimalMetadata(BaseModel):
    dataset_id: str
    edition: str
    distributions: List[Distribution]
    release_date: str


class Manifest(BaseModel):
    metadata_file: str
    submission_contacts: List[SubmissionContact]
    use_previous_metadata: bool = False

    def get_submission_contact_email(self):
        if len(self.submission_contacts) == 0:
            return ""

        return self.submission_contacts[0].email
