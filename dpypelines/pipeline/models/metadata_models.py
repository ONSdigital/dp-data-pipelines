from enum import Enum
from optparse import Option
from typing import Annotated, List, Optional

from pydantic import AfterValidator, BaseModel, EmailStr, Field

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
    edition_title: Optional[str] = Field(alias="edition")
    quality_designation: Annotated[Optional[str], AfterValidator(quality_desigination_checker)]
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
