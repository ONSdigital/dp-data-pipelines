from dataclasses import dataclass, field
import json
from typing import Dict, List
from dpypelines.pipeline.messages.utils import get_mimetype


@dataclass
class Metadata:
    dataset_id: str = field(default=None)
    edition: str = field(default=None)
    edition_title: str = field(default=None)
    quality_designation: str = field(default=None)
    usage_notes: List["UsageNote"] = field(default_factory=list)
    alerts: List["Alert"] = field(default_factory=list)
    distributions: List["Distribution"] = field(default_factory=list)


@dataclass
class Manifest:
    metadata_file: str = field(default=None)
    submission_contacts: List["SubmissionContact"] = field(default_factory=list)


@dataclass
class Distribution:
    title: str = field(default=None)
    format: str = field(default=None)
    file: str = field(default=None)
    download_url: str = field(default=None, init=False)
    media_type: str = field(default=None, init=False)

    def __post_init__(self):
        self.download_url = f"https://download.ons.gov.uk/{self.file}"
        self.media_type = get_mimetype(f".{self.format}")


@dataclass
class Alert:
    type: str = field(default=None)
    description: str = field(default=None)


@dataclass
class UsageNote:
    title: str = field(default=None)
    note: str = field(default=None)


@dataclass
class SubmissionContact:
    email: str = field(default=None)


def get_metadata_from_json(metadata_dict: dict) -> Metadata:
    return Metadata(
        dataset_id=metadata_dict["dataset_id"],
        edition=metadata_dict["edition"],
        edition_title=metadata_dict["edition_title"],
        quality_designation=metadata_dict["quality_designation"],
        usage_notes=get_usage_notes_from_metadata(metadata_dict["usage_notes"]),
        alerts=get_alerts_from_metadata(metadata_dict["alerts"]),
        distributions=get_distributions_from_metadata(metadata_dict["distributions"]),
    )


def get_manifest_from_json(manifest_dict: dict) -> Manifest:
    try:
        return Manifest(
            metadata_file=manifest_dict["metadata_file"],
            submission_contacts=get_submission_contacts_from_manifest(
                manifest_dict["submission_contacts"]
            ),
        )
    except Exception as e:
        raise e


def get_usage_notes_from_metadata(usage_notes: List[Dict]) -> List[UsageNote]:
    return [
        UsageNote(title=usage_note["title"], note=usage_note["note"])
        for usage_note in usage_notes
    ]


def get_alerts_from_metadata(alerts: List[Dict]) -> List[Alert]:
    return [
        Alert(type=alert["type"], description=alert["description"]) for alert in alerts
    ]


def get_distributions_from_metadata(distributions: List[Dict]) -> List[Distribution]:
    return [
        Distribution(
            title=distribution["title"],
            format=distribution["format"],
            file=distribution["file"],
        )
        for distribution in distributions
    ]


def get_submission_contacts_from_manifest(
    submission_contacts: List[Dict],
) -> List[SubmissionContact]:
    return [
        SubmissionContact(email=submission_contact["email"])
        for submission_contact in submission_contacts
    ]
