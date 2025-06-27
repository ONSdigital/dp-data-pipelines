import datetime
from enum import Enum
from typing import Annotated, List, Optional, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    EmailStr,
    Field,
    model_validator,
)

from dpypelines.pipeline.messages.utils import get_mimetype


class Distribution(BaseModel):
    title: str
    format: str
    file: str
    download_url: str = ""
    media_type: str = ""
    identifier: str = Field(default="", exclude=True)
    upload_path: str = Field(default="", exclude=True)

    def model_post_init(self, __context):
        mimetype = get_mimetype(f".{self.format}")
        self.media_type = mimetype if mimetype is not None else ".csv"
        timestamp = datetime.datetime.now().strftime(format="%d%m%y%H%M%S")
        self.identifier = f"{timestamp}-{self.file.replace('.', '-').replace(' ', '_')}"
        self.upload_path = f"datasets/{self.identifier}"
        self.download_url = f"{self.upload_path}/{self.file}"


class Alert(BaseModel):
    type: str
    description: str


class UsageNote(BaseModel):
    title: Optional[str] = None
    note: Optional[str] = None


class SubmissionContact(BaseModel):
    email: EmailStr


class QualityDesignation(str, Enum):
    AccreditedOfficial = "accredited-official"
    Official = "official"
    OfficialInDevelopment = "official-in-development"


def map_quality_desigination(value: Optional[str]) -> QualityDesignation:
    if value is None:
        return QualityDesignation.AccreditedOfficial
    try:
        return QualityDesignation(value)
    except ValueError:
        return QualityDesignation.AccreditedOfficial


def quality_desigination_checker(value: Optional[str]) -> str:
    return map_quality_desigination(value).value


class DatasetVersion(BaseModel):
    edition_title: Optional[str] = Field(default="")
    quality_designation: Annotated[
        Optional[str], BeforeValidator(quality_desigination_checker)
    ] = Field(default=None)
    usage_notes: Optional[List[UsageNote]] = Field(default_factory=list)
    alerts: Optional[List[Alert]] = Field(default_factory=list)
    distributions: List[Distribution]
    release_date: str


class Metadata(DatasetVersion):
    dataset_id: str
    edition: str

    @model_validator(mode="after")
    def check_edition_title_exists(self) -> Self:
        if self.edition_title == "":
            self.edition_title = self.edition
        return self


class MinimalMetadata(BaseModel):
    dataset_id: str
    edition: str
    distributions: List[Distribution]
    release_date: str


class Manifest(BaseModel):
    metadata_file: str
    submission_contacts: List[SubmissionContact]
    use_previous_metadata: bool = False
