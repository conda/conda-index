import pytest
from conda.base.context import context
from conda.models.channel import Channel
from conda.models.match_spec import MatchSpec
from conda.models.records import PackageRecord

from conda_index.index.current_repodata import _package_sort_key, _PackageIndex


def _record(
    name,
    version,
    build="0",
    build_number=0,
    subdir="linux-64",
    timestamp=0,
    track_features=None,
    fn=None,
):
    fields = {
        "name": name,
        "version": version,
        "build": build,
        "build_number": build_number,
        "channel": Channel("dummy"),
        "subdir": subdir,
        "fn": fn or f"{name}-{version}-{build}.tar.bz2",
        "timestamp": timestamp,
    }
    if track_features:
        fields["track_features"] = track_features
    return PackageRecord(**fields)


def _make_index(*records):
    return _PackageIndex({prec: prec for prec in records})


def test_groups_sorted_newest_first():
    old = _record("pkg", "1.0", build_number=1)
    newest = _record("pkg", "2.0", build_number=0)
    newer_build = _record("pkg", "2.0", build="1", build_number=1)
    index = _make_index(old, newest, newer_build)

    assert index.groups["pkg"] == [newer_build, newest, old]


def test_arch_sorts_before_noarch():
    noarch = _record("pkg", "1.0", subdir="noarch")
    arch = _record("pkg", "1.0", subdir="linux-64")
    index = _make_index(noarch, arch)

    assert index.groups["pkg"] == [arch, noarch]


def test_newer_timestamp_wins(monkeypatch):
    monkeypatch.setattr(context, "solver_ignore_timestamps", False)
    older = _record("pkg", "1.0", timestamp=1)
    newer = _record("pkg", "1.0", timestamp=2)

    ordered = sorted([older, newer], key=_package_sort_key, reverse=True)
    assert ordered == [newer, older]


def test_timestamps_ignored_when_configured(monkeypatch):
    monkeypatch.setattr(context, "solver_ignore_timestamps", True)
    older = _record("pkg", "1.0", timestamp=1)
    newer = _record("pkg", "1.0", timestamp=2)

    assert _package_sort_key(older) == _package_sort_key(newer)


def test_find_matches_by_name_and_version():
    old = _record("pkg", "1.0")
    newest = _record("pkg", "2.0")
    other = _record("other", "1.0")
    index = _make_index(old, newest, other)

    assert index.find_matches(MatchSpec("pkg")) == (newest, old)
    assert index.find_matches(MatchSpec("pkg=1.0")) == (old,)
    assert index.find_matches(MatchSpec("missing")) == ()


def test_find_matches_track_features():
    plain = _record("pkg", "1.0")
    featured = _record("pkg", "1.0", build="0feature", track_features="mkl")
    index = _make_index(plain, featured)

    assert index.find_matches(MatchSpec(track_features="mkl")) == (featured,)


@pytest.mark.parametrize("ignore_timestamps", [True, False])
def test_sort_key_uses_build_string(monkeypatch, ignore_timestamps):
    monkeypatch.setattr(context, "solver_ignore_timestamps", ignore_timestamps)
    low = _record("pkg", "1.0", build="0", build_number=0)
    high = _record("pkg", "1.0", build="1", build_number=0)
    index = _make_index(low, high)

    assert index.groups["pkg"] == [high, low]
