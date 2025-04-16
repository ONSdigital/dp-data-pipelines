from dpypelines.pipeline.validation.xml_validator import XMLValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)
from pathlib import Path
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom


class TestXMLValidator(BaseTestFileValidator):
    file_format: str = "xml"
    validator_type = XMLValidator

    def generate_test_file(self, file_path: Path):
        # Create root element
        root = ET.Element("root")

        # Add items
        items = ET.SubElement(root, "items")

        # Add item
        item1 = ET.SubElement(items, "item")
        item1.set("id", "1")
        name1 = ET.SubElement(item1, "name")
        name1.text = "item1"
        value1 = ET.SubElement(item1, "value")
        value1.text = "."

        rough_string = ET.tostring(root, "utf-8")
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="  ")

        with open(file_path, "w") as xmlfile:
            xmlfile.write(pretty_xml)

    def test_validator_passes_valid_file(self):
        self._test_validator_passes_valid_file()

    def test_validator_errors_empty_file(self):
        self._test_validator_errors_empty_file()
