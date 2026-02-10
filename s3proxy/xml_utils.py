"""XML utilities for S3 API parsing."""

import xml.etree.ElementTree as ET

S3_XML_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def find_element(parent: ET.Element, tag_name: str) -> ET.Element | None:
    """Find single XML element with S3 namespace fallback."""
    elem = parent.find(f"{S3_XML_NAMESPACE}{tag_name}")
    if elem is None:
        elem = parent.find(tag_name)
    return elem


def find_elements(parent: ET.Element, tag_name: str) -> list[ET.Element]:
    """Find all XML elements with S3 namespace fallback."""
    elements = parent.findall(f".//{S3_XML_NAMESPACE}{tag_name}")
    if not elements:
        elements = parent.findall(f".//{tag_name}")
    return elements


def get_element_text(parent: ET.Element, tag_name: str, default: str = "") -> str:
    """Get text content of child element with namespace fallback."""
    elem = find_element(parent, tag_name)
    return elem.text if elem is not None and elem.text else default
