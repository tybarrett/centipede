"""What a package carries between stages."""

import dill
import pytest

from centipede.internal.package import Package


def test_a_new_package_carries_only_its_linked_resources():
    assert Package().fields() == ["linked_resources"]


def test_fields_lists_what_stages_have_attached():
    package = Package()
    package.raw_records = [1, 2]
    package.venue = "example"

    assert package.fields() == ["linked_resources", "raw_records", "venue"]


def test_get_returns_a_field_a_stage_attached():
    package = Package()
    package.raw_records = [1, 2]

    assert package.get("raw_records") == [1, 2]


def test_get_falls_back_when_no_stage_attached_it():
    assert Package().get("raw_records", []) == []
    assert Package().get("raw_records") is None


def test_reading_a_field_nobody_attached_says_what_is_there():
    package = Package()
    package.raw_records = []

    with pytest.raises(AttributeError) as raised:
        package.raw_record

    message = str(raised.value)
    assert "raw_record" in message
    assert "linked_resources" in message and "raw_records" in message


def test_linked_resources_are_still_read_the_old_way():
    package = Package()
    package.linked_resources.extend(["https://example.com/one"])

    assert package.get_linked_resources() == ["https://example.com/one"]


def test_a_package_survives_the_trip_between_processes():
    """Packages are dilled to cross a socket, and dill asks about dunders."""
    package = Package()
    package.raw_records = [{"artist": "Turnstile"}]
    package.linked_resources.append("https://example.com/one")

    arrived = dill.loads(dill.dumps(package))

    assert arrived.fields() == ["linked_resources", "raw_records"]
    assert arrived.raw_records == [{"artist": "Turnstile"}]
    assert arrived.get_linked_resources() == ["https://example.com/one"]


def test_a_package_describes_itself():
    package = Package()
    package.raw_records = []

    assert repr(package) == "<Package carrying linked_resources, raw_records>"
