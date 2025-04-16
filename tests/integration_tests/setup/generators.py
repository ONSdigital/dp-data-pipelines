from faker import Faker
from dpypelines.pipeline.connectors.dataset_api.models import Contact, Dataset, DatasetVersion

faker = Faker()


def generate_contact():
    return Contact(
        email=faker.email(),
        name=faker.name(),
        telephone=faker.random_element([f"+44{faker.basic_phone_number()}", None]),
    )


def generate_topics():
    topic_count = faker.random_int(1, 5)
    topics = [str(faker.random_int(1000, 10000)) for i in range(topic_count)]
    return topics


def generate_dataset():
    return DatasetVersion(
        id=faker.uuid4(),
        contacts=[generate_contact()],
        description="".join(faker.paragraphs(faker.random_int(1, 3))),
        license="Open Government Licence v3.0",
        title=faker.sentence(nb_words=10, variable_nb_words=True),
        state=faker.random_element(["created", "published"]),
        topics=generate_topics(),
        type=faker.random_element(
            [
                "filterable",
                "cantabular_flexible_table",
                "cantabular_multivariate_table",
                "static",
            ]
        ),
    )


dataset = generate_dataset()

print(dataset)
