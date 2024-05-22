import grosbeak.util as util


def test_strip_extension():
    assert util.strip_extension("test_file.txt") == "test_file"
    assert util.strip_extension("test_file.test.json") == "test_file"
