"""Tests for hashing functionality."""

import datetime as dt
from io import BytesIO
from pathlib import Path

import pytest
from little_pipelines._hashing import hash_file, hash_files, hash_script


hello_hash = '185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969'
from_hash = '75857a45899985be4c4d941e90b6b396d6c92a4c7437aaf0bf102089fe21379d'
lp_hash = 'd37442848d3dfc438434d12505fee8b3646b569ca8e87cfeaea0e05a48e51a58'
comb_hash = 'ac35409a76b171d41856220dc84010b43014f8bd9109c87995a8676f74ad062e'


@pytest.fixture
def files():
    return [
        BytesIO(b"Hello"),
        BytesIO(b"from"),
        BytesIO(b"little_pipelines"),
    ]


def test_hash_file(files):
    assert hash_file(files[0]) == hello_hash


def test_hash_files(files):
    assert hash_files(*files) == comb_hash


def test_hash_script():
    assert hash_script(__file__) != "HASHERROR"
