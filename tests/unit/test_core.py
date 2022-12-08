import os

from inbound import core


def test_clean_column_names_norwegian():
    assert core.clean_column_names(" æå.ø ") == "aeaa_o"


def test_clean_column_names_numbers():
    assert (
        core.clean_column_names(
            "03013: Konsumprisindeks, etter konsumgruppe, måned og statistikkvariabel"
        )
        == "_03013_konsumprisindeks_etter_konsumgruppe_maaned_og_statistikkvariabel"
    )


def test_get_env():
    os.environ["TEST_GET_ENV"] = "test"
    assert core.get_env("TEST_GET_ENV") == "test"
