import contextlib
import mock
import pkg_resources
import re
import json

# flake8: noqa: D200, D107, D102, D105


def sort_dictionary_sortable_values(dictionary):
    """
    Sort lists which are values of a dictionary. May be used for better dict comparisons.

    Args:
        dictionary (dict):
            Dictionary whose values should be sorted.
    """
    for v in dictionary.values():
        if isinstance(v, list):
            v.sort()


def compare_logs(caplog, expected_logs):
    """
    Compare log messages between captured messages and expected ones.

    Expected messages can contain regexes.

    Args:
        caplog:
            Caplog generated by pytest.
        expected_logs ([str]):
            List of strings/regexes containing expected logs lines.
    """
    assert len(caplog.records) == len(expected_logs)

    for i in range(len(expected_logs)):
        log_line = caplog.records[i].getMessage()
        match = re.search(expected_logs[i], log_line)
        if match is None:
            raise AssertionError(
                "Captured log line '{0}' couldn't be matched with '{1}'".format(
                    log_line, expected_logs[i]
                )
            )


class FixedEntryPoint(object):
    """An EntryPoint which always points at the specified object rather than going
    through normal resolution."""

    def __init__(self, delegate, resolved_object):
        # These all work identically as in the real entry point
        self.name = delegate.name
        self.module_name = delegate.module_name
        self.attrs = delegate.attrs
        self.extras = delegate.extras
        self.dist = delegate.dist
        self.require = delegate.require
        self.__str__ = delegate.__str__

        # But any attempt to load or resolve will just return this rather than
        # the true underlying object
        self.load = lambda: resolved_object
        self.resolve = lambda: resolved_object


class EntryPointOverride(object):
    """Point a named entry point at a provided object temporarily."""

    def __init__(self, replacement, dist, group, name):
        self._name = name

        # Keep a reference to the original entry point
        dist = pkg_resources.get_distribution(dist)
        self._ep_map = dist.get_entry_map(group)
        self._old_ep = self._ep_map[self._name]

        # Prepare the replacement
        self._replace_ep = FixedEntryPoint(self._old_ep, replacement)

    def start(self):
        self._ep_map[self._name] = self._replace_ep

    def __enter__(self):
        self.start()
        return self

    def stop(self):
        self._ep_map[self._name] = self._old_ep

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


@contextlib.contextmanager
def mock_entry_point(dist, group, name):
    """Point a given entry point at a new mock object for the duration of the current test."""
    new_mock = mock.Mock()
    override = EntryPointOverride(new_mock, dist, group, name)
    try:
        override.start()
        yield new_mock
    finally:
        override.stop()


class IIBRes:
    def __init__(self, index_image, internal_index_image_copy_resolved, build_tags):
        self.index_image = index_image
        self.internal_index_image_copy_resolved = internal_index_image_copy_resolved
        self.build_tags = build_tags


def mock_manifest_list_requests(m, uri, manifest_list, manifests):
    m.get(
        uri,
        json=manifest_list,
        headers={"Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"},
    )
    base_uri = uri.rsplit("/", 1)[0]
    if isinstance(manifests, list):
        for man1, man2 in zip(manifest_list["manifests"], manifests):
            m.get(
                base_uri + "/" + man1["digest"],
                text=json.dumps(man2, sort_keys=True),
                headers={"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"},
            )
    else:
        for man in manifest_list["manifests"]:
            m.get(
                base_uri + "/" + man["digest"],
                text=json.dumps(manifests, sort_keys=True),
                headers={"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"},
            )


class GetManifestSideEffect:
    def __init__(self, v2s1_manifest, manifest_list):
        self.called_times = 0
        self.v2s1_manifest = v2s1_manifest
        self.manifest_list = manifest_list

    def __call__(self, image, raw=False, media_type=False):
        if self.called_times == 1:
            return None
        if media_type == "application/vnd.docker.distribution.manifest.list.v2+json":
            content = self.manifest_list
        else:
            content = self.v2s1_manifest
        self.called_times += 1
        return json.dumps(content) if raw else content
