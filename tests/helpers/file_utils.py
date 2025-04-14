import io
import json
import random
import string
import zipfile
from random import choice


def generate_random_string(length: int = 10):
    return "".join(
        [choice(string.digits + string.ascii_letters) for i in range(length)]
    )


def create_manifest(dataset_id: str):
    return {
        "manifestVersion": 1,
        "source_id": "_move",
        "fileAuthorEmail": "test@email.com",
        "fileAuthorUsername": "Username",
        "isPublishable": True,
        "licence": "My licence",
        "licenceUrl": "http://www.example.org/licence",
        "title": f"Test dataset - {dataset_id}",
        "aliasName": "Alias name",
    }


def create_distribution_for_file_format(file_format: str):
    match file_format:
        case "csv":
            return {
                "dcterms:title": "CSV distribution title",
                "download_url": "https://download.ons.gov.uk/download.csv",
                "dcat:mediaType": "text/csv",
                "TBC:distributionFormat": "csv",
            }
        case "xls":
            return {
                "dcterms:title": "XLS distribution title",
                "download_url": "https://download.ons.gov.uk/download.xls",
                "dcat:mediaType": "application/vnd.ms-excel",
                "TBC:distributionFormat": "xls",
            }

    raise Exception(f"Unmatched fie format: {file_format}")


def create_metadata(dataset_id: str, edition_id: str, file_format: str = "csv"):
    return {
        "@context": {
            "adms": "http://www.w3.org/ns/adms#",
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/",
            "dqv": "http://www.w3.org/ns/dqv#",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "vcard": "http://www.w3.org/2006/vcard/ns#",
        },
        "@type": "dcat:DatasetSeries",
        "dcterms:identifier": f"{dataset_id}",
        "dcterms:title": f"Test dataset - {dataset_id}",
        "dcterms:description": "Test dataset",
        "TBC:nextRelease": "The next release date for a dataset",
        "dcat:theme": ["theme1", "theme2"],
        "dqv:hasQualityAnnotation": [{"dqv:QualityCertificate": "QMI URL"}],
        "dcat:contactPoint": [
            {
                "vcard:fn": "Contact name",
                "vcard:hasEmail": "Contact email",
                "vcard:hasTelephone": "01215012345",
            }
        ],
        "dcterms:publisher": [
            {
                "TBC:type": "Publisher type",
                "foaf:name": "Publisher name",
                "foaf:homepage": "Publisher link",
            }
        ],
        "TBC:edition": [
            {
                "@type": "dcat:Dataset",
                "dcterms:title": f"Testing title - {edition_id}",
                "dcterms:identifier": f"{edition_id}",
                "TBC:quality_designation": "accredited-official",
                "TBC:usage_notes": [
                    {
                        "TBC:title": "Usge note title",
                        "TBC:note": "Usage note description",
                    }
                ],
                "TBC:alerts": [
                    {
                        "TBC:type": "AlertTypeEnum",
                        "adms:versionNotes": "Alert type description",
                    }
                ],
                "dcat:distribution": [create_distribution_for_file_format(file_format)],
            }
        ],
    }


DEFAULT_CSV_HEADERS = ["ID", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "CONF_STATUS"]
DEFAULT_CSV_ROWS = [
    ["T1500_2024-01-24T12-58-48", "1997", "18415", "A", "F"],
    ["T1500_2024-01-24T12-58-48", "1998", "18032", "A", "F"],
]


def create_csv(
    headers: list[str] = DEFAULT_CSV_HEADERS, rows: list[str] = DEFAULT_CSV_ROWS
):
    header_row = ",".join(headers)
    rows = [",".join(row) for row in rows]

    return "\n".join([header_row, *rows])


def create_zip(
    metadata: str | None = None,
    manifest: str | None = None,
    data: str | None = None,
    data_file_name: str | None = None,
) -> io.BytesIO:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zip_file:
        if metadata:
            zip_file.writestr("metadata.json", metadata)
        if manifest:
            zip_file.writestr("manifest.json", manifest)

        if data and data_file_name:
            zip_file.writestr(data_file_name, data)

    return zip_buffer


class ZipFileInfo:
    def __init__(self, zip_file: io.BytesIO, file_name: str):
        self.zip_file = zip_file
        self.file_name = file_name


def create_successful_file(
    dataset_id: str, edition_id: str, save_to_disk: bool = False
):
    metadata = json.dumps(create_metadata(dataset_id=dataset_id, edition_id=edition_id))
    manifest = json.dumps(create_manifest(dataset_id=dataset_id))
    csv = create_csv()
    zip = create_zip(
        metadata=metadata, manifest=manifest, data=csv, data_file_name="data.csv"
    )

    file_name = f"{dataset_id}-{edition_id}-{random.randint(10000,100000)}.zip"

    if save_to_disk:
        with open(file_name, "wb") as f:
            f.write(zip.getbuffer())

    return ZipFileInfo(zip, file_name)


def create_file_with_missing_metadata():
    manifest = json.dumps(create_manifest())
    csv = create_csv()
    return create_zip(manifest=manifest, data=csv, data_file_name="data.csv")


def create_file_with_missing_manifest():
    metadata = json.dumps(create_metadata())
    csv = create_csv()
    return create_zip(metadata=metadata, data=csv, data_file_name="data.csv")
