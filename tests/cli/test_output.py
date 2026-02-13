"""Tests for CLI output helpers."""

import json

from ontologia.cli._output import print_error, print_object, print_table


def test_print_table_json(capsys):
    print_table(["id", "name"], [["1", "Alice"], ["2", "Bob"]], json_mode=True)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 2
    assert data[0] == {"id": "1", "name": "Alice"}


def test_print_table_text(capsys):
    print_table(["id", "name"], [["1", "Alice"]], json_mode=False)
    out = capsys.readouterr().out
    assert "id" in out
    assert "Alice" in out


def test_print_table_empty(capsys):
    print_table(["id"], [], json_mode=False)
    out = capsys.readouterr().out
    assert out == ""


def test_print_object_json(capsys):
    print_object({"key": "val"}, json_mode=True)
    out = capsys.readouterr().out
    assert json.loads(out) == {"key": "val"}


def test_print_object_text(capsys):
    print_object({"key": "val"}, json_mode=False)
    out = capsys.readouterr().out
    assert "key: val" in out


def test_print_error(capsys):
    print_error("something broke")
    err = capsys.readouterr().err
    assert "Error: something broke" in err
