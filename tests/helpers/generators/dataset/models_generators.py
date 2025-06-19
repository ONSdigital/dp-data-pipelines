from typing import Callable, List, Optional
from dpytools.http.api.models import Dataset, DatasetVersion
from dpytools.http.api.models.dataset import (
    Contact,
    Methodology,
    Publication,
    Publisher,
    DatasetType,
    DatasetState,
    Qmi,
)
from dpytools.http.api.models.version import Distribution

from faker import Faker

faker = Faker()


def generate_id() -> str:
    return f"{'_'.join(faker.words(nb=5))}"


def generate_contact() -> Contact:
    return Contact(
        email=faker.email(), name=faker.name(), telephone=faker.phone_number()
    )


def generate_methodology() -> Methodology:
    return Methodology(
        href=faker.url(),
        title=" ".join(faker.words(5)),
        description=faker.paragraph(),
    )


def generate_publication() -> Publication:
    return Publication(
        href=faker.url(),
        title=" ".join(faker.words(5)),
        description=faker.paragraph(),
    )


def generate_publisher() -> Publisher:
    return Publisher(
        href=faker.url(),
        name=faker.company(),
    )


def generate_qmi() -> Qmi:
    return Qmi(
        href=faker.url(),
    )


def generate_list[T](func: Callable[[], T], count: int) -> List[T]:
    list: List[T] = []
    for i in range(count):
        list.append(func())

    return list


def generate_dataset(type: Optional[DatasetType] = None) -> Dataset:
    return Dataset(
        # Required
        id=generate_id(),
        next_release=faker.future_datetime().strftime("%d/%m/%Y, %H:%M:%S"),
        topics=faker.words(nb=faker.pyint(min_value=1, max_value=5)),
        title=faker.sentence(),
        type=type if type is not None else faker.enum(enum_cls=DatasetType),
        keywords=faker.words(nb=faker.random_int(1, 3)),
        contacts=generate_list(generate_contact, faker.random_int(min=1, max=3)),
        # Optional
        collection_id=None if not faker.boolean(10) else generate_id(),
        canonical_topic=faker.word() if faker.boolean(10) else None,
        description=faker.paragraph(),
        license="ONS",
        methodologies=[] if not faker.boolean(10) else [generate_methodology()],
        publications=[] if not faker.boolean(10) else [generate_publication()],
        publishers=generate_list(generate_publisher, faker.random_int(min=1, max=3)),
        state=DatasetState.CREATED,
    )


def generate_dataset_version(
    distributions: list[Distribution], edition: Optional[str] = None
):
    return DatasetVersion(
        quality_designation=faker.random_element(
            ["accredited-official", "official", "official-in-development"]
        ),
        release_date=faker.future_datetime().strftime("%d/%m/%Y, %H:%M:%S"),
        edition=edition if edition else generate_id(),
        edition_title=faker.sentence(),
        distributions=distributions,
    )


dataset = generate_dataset()

dumped = dataset.model_dump_json()
print(dumped)
