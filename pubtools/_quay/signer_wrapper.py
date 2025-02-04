import abc
from contextlib import contextmanager, redirect_stdout
import logging
import pkg_resources
import tempfile
import json
import io


from typing import Optional, List, Dict, Any, Tuple, Generator

from marshmallow import Schema, fields, EXCLUDE

from .quay_api_client import QuayApiClient
from .utils.misc import (
    run_entrypoint,
    get_pyxis_ssl_paths,
    run_in_parallel,
    log_step,
    FData,
    grouper,
)
from .item_processor import SignEntry


LOG = logging.getLogger("pubtools.quay")


class SigningError(Exception):
    """Error raised when signing fails."""


class NoSchema(Schema):
    """Schema that does not validate anything."""


class MsgSignerSettingsSchema(Schema):
    """Validation schema for messaging signer settings."""

    pyxis_server = fields.String(required=True)
    pyxis_ssl_crtfile = fields.String(required=False)
    pyxis_ssl_keyfile = fields.String(required=False)
    num_thread_pyxis = fields.Integer(required=False, default=7)
    signing_chunk_size = fields.Integer(required=False, default=100)
    signing_parallelism = fields.Integer(required=False, default=10)


class SignerWrapper:
    """Wrapper providing functionality to sign containers with a generic signer."""

    label = "unused"
    pre_push = False

    SCHEMA: type[Schema] = NoSchema
    entry_point_conf = ["signer", "group", "signer"]

    def __init__(
        self, config_file: Optional[str] = None, settings: Dict[str, Any] | None = None
    ) -> None:
        """Initialize SignerWrapper.

        Args:
            config_file (str): Path to config file for the signer.
            settings (dict): Settings for the signer.
        """
        self.config_file = config_file
        self.settings = settings or {}
        self._ep = None
        self.validate_settings()

    @property
    def entry_point(self) -> Any:
        """Load and return entry point for pubtools-sign project."""
        if self._ep is None:
            self._ep = pkg_resources.load_entry_point(*self.entry_point_conf)
        return self._ep

    def remove_signatures(
        self,
        signatures: List[Tuple[str, str, str]],
        _exclude: Optional[List[Tuple[str, str, str]]] = None,
    ) -> None:
        """Remove signatures from a sigstore."""
        LOG.debug("Removing signatures %s", signatures)
        self._remove_signatures(signatures)

    @abc.abstractmethod
    def _run_remove_signatures(self, signatures_to_remove: List[Any]) -> None:
        pass  # pragma: no cover

    def _remove_signatures(self, signatures_to_remove: List[Any]) -> None:
        """Remove signatures from sigstore.

        This is helper to make testing easier.
        Args:
            signatures_to_remove (list): Signatures to remove.
        """
        self._run_remove_signatures(signatures_to_remove)

    @abc.abstractmethod
    def _run_store_signed(self, signatures: Dict[str, Any]) -> None:
        pass  # pragma: no cover

    def _store_signed(self, signatures: Dict[str, Any]) -> None:
        """Store signatures in sigstore.

        This is helper to make testing easier.
        Args:
            signatures (dict): Signatures to store.
        """
        LOG.debug("Storing signatures %s", signatures)
        self._run_store_signed(signatures)

    def sign_container_opt_args(
        self, sign_entry: SignEntry, task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return optional arguments for signing a container.

        Args:
            sign_entry (SignEntry): SignEntry to sign.
            task_id (str): Task ID to identify the signing task if needed.

        Returns:
            dict: Optional arguments for signing a container.
        """
        return {}

    def sign_container_chunk(
        self,
        sign_entries: List[SignEntry],
        task_id: Optional[str] = None,
    ) -> None:
        """Sign a specific chunk of references and digests with given signing key.

        Args:
            sign_entries (List[SignEntry]): Chunk of SignEntry to sign.
            task_id (str): Task ID to identify the signing task if needed.
        """
        for sign_entry in sign_entries:
            # in the case of last chunk, None is set to fill value to fit the chunk size
            # Therefore skip if that presents
            if not sign_entry:
                break
            LOG.debug(
                "Signing container %s %s %s",
                sign_entry.reference,
                sign_entry.digest,
                sign_entry.signing_key,
            )
        sign_entry = sign_entries[0]
        opt_args = self.sign_container_opt_args(sign_entry, task_id)
        signed = self.entry_point(
            config_file=self.config_file,
            signing_key=sign_entry.signing_key,
            reference=[x.reference for x in sign_entries if x],
            digest=[x.digest for x in sign_entries if x],
            **opt_args,
        )
        if signed["signer_result"]["status"] != "ok":
            raise SigningError(signed["signer_result"]["error_message"])
        for sign_entry in sign_entries:
            if not sign_entry:
                break
            LOG.info(
                "Signed %s(%s) with %s in %s",
                sign_entry.reference,
                sign_entry.digest,
                sign_entry.signing_key,
                self.label,
            )
        self._store_signed(signed)

    def _filter_to_sign(self, to_sign_entries: List[SignEntry]) -> List[SignEntry]:
        """Filter entries to sign.

        Args:
            to_sign_entries (List[SignEntry]): list of entries to sign.

        Returns:
            List[SignEntry]: list of entries to sign.
        """
        return to_sign_entries

    @log_step("Sign container images")
    def sign_containers(
        self,
        to_sign_entries: List[SignEntry],
        task_id: Optional[str] = None,
    ) -> None:
        """Sign signing entries.

        Entries are sent to signer in chunks of chunk_size size.

        Args:
            to_sign_entries (List[SignEntry]): list of entries to sign.
            task_id (str): optional identifier used in signing process.
            parallelism (int): determines how many entries should be signed in parallel.
        """
        to_sign_entries = self._filter_to_sign(to_sign_entries)
        to_sign_chunks = []
        # split entries to chunk of chunk_size, fill shorter chunks with None
        to_sign_chunks.extend(
            list(grouper(to_sign_entries, self.settings.get("signing_chunk_size", 100)))
        )

        with redirect_stdout(io.StringIO()):
            run_in_parallel(
                self.sign_container_chunk,
                [
                    FData(args=x)
                    for x in zip(
                        to_sign_chunks,
                        [
                            str(task_id)
                            + "-"
                            + str(z % self.settings.get("signing_parallelism", 7))
                            for z in range(len(to_sign_chunks))
                        ],
                    )
                ],
                self.settings.get("signing_parallelism", 7),
            )

    def validate_settings(self, settings: Dict[str, Any] | None = None) -> None:
        """Validate provided settings for the SignerWrapper."""
        settings = settings or self.settings
        schema = self.SCHEMA(unknown=EXCLUDE)
        schema.load(settings)


class MsgSignerWrapper(SignerWrapper):
    """Wrapper for messaging signer functionality."""

    label = "msg_signer"
    pre_push = True

    entry_point_conf = ["pubtools-sign", "modules", "pubtools-sign-msg-container-sign"]

    MAX_MANIFEST_DIGESTS_PER_SEARCH_REQUEST = 50
    SCHEMA = MsgSignerSettingsSchema

    def _filter_to_sign(self, to_sign_entries: List[SignEntry]) -> List[SignEntry]:
        to_sign_digests = [x.digest for x in to_sign_entries]
        existing_signatures = [esig for esig in self._fetch_signatures(to_sign_digests)]
        existing_signatures_drk = {
            (x["manifest_digest"], x["reference"], x["sig_key_id"]) for x in existing_signatures
        }
        ret = []
        for tse in to_sign_entries:
            if (tse.digest, tse.reference, tse.signing_key) not in existing_signatures_drk:
                ret.append(tse)
        return ret

    def sign_container_opt_args(
        self, sign_entry: SignEntry, task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return optional arguments for signing a container.

        Args:
            sign_entry (SignEntry): SignEntry to sign.
            task_id (str): Task ID to identify the signing task if needed.

        Returns:
            dict: Optional arguments for signing a container.
        """
        return {k: v for k, v in [("task_id", task_id), ("repo", sign_entry.repo)] if v is not None}

    @contextmanager
    def _save_signatures_file(self, signatures: List[Dict[str, Any]]) -> Generator[Any, None, None]:
        """Save signatures to a temporary file and yield the file."""
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="pubtools_quay_upload_signatures_"
        ) as signature_file:
            json.dump(signatures, signature_file)
            signature_file.flush()
            yield signature_file

    def _fetch_signatures(
        self, manifest_digests: List[str]
    ) -> Generator[dict[str, Any], None, None]:
        """Fetch signatures from sigstore.

        Args:
            manifest_digests (list): Manifest digests to fetch signatures for.
        Returns:
            List[Dict[str, Any]]: List of fetched signatures.
        """
        cert, key = get_pyxis_ssl_paths(self.settings)
        chunk_size = self.MAX_MANIFEST_DIGESTS_PER_SEARCH_REQUEST
        manifest_digests = sorted(list(set(manifest_digests)))

        args = ["--pyxis-server", self.settings["pyxis_server"]]
        args += ["--pyxis-ssl-crtfile", cert]
        args += ["--pyxis-ssl-keyfile", key]
        args += ["--request-threads", str(self.settings.get("num_thread_pyxis", 7))]

        for chunk_start in range(0, len(manifest_digests), chunk_size):
            chunk = manifest_digests[chunk_start : chunk_start + chunk_size]  # noqa: E203

            args = ["--pyxis-server", self.settings["pyxis_server"]]
            args += ["--pyxis-ssl-crtfile", cert]
            args += ["--pyxis-ssl-keyfile", key]

            with tempfile.NamedTemporaryFile(
                mode="w", prefix="pubtools_quay_get_signatures_"
            ) as signature_fetch_file:
                if manifest_digests:
                    json.dump(chunk, signature_fetch_file)
                    signature_fetch_file.flush()
                    args += ["--manifest-digest", "@{0}".format(signature_fetch_file.name)]

                env_vars: dict[Any, Any] = {}
                chunk_results = run_entrypoint(
                    ("pubtools-pyxis", "console_scripts", "pubtools-pyxis-get-signatures"),
                    "pubtools-pyxis-get-signatures",
                    args,
                    env_vars,
                )

            for result in chunk_results:
                yield result

    def _run_store_signed(self, signed_results: Dict[str, Any]) -> None:
        """
        Upload signatures to Pyxis by using a pubtools-pyxis entrypoint.

        Data required for a Pyxis POST request:
        - manifest_digest
        - reference
        - repository
        - sig_key_id
        - signature_data

        Signatures are uploaded in batches.

        Args:
            signed_results: (Dict[str, Any]):
                Dictionary of {"signer_result":..., "operation_results":..., "signing_key":...}"}
                holding signed manifest claims data
        """
        LOG.info("Sending new signatures to Pyxis")

        signatures: List[Dict[str, Any]] = []
        for reference, op_res in zip(
            signed_results["operation"]["references"], signed_results["operation_results"]
        ):
            signatures.append(
                {
                    "manifest_digest": op_res[0]["msg"]["manifest_digest"],
                    "reference": reference,
                    "repository": op_res[0]["msg"]["repo"],
                    "sig_key_id": signed_results["signing_key"],
                    "signature_data": op_res[0]["msg"]["signed_claim"],
                }
            )

        for sig in signatures:
            LOG.debug(
                f"Uploading new signature. Reference: {sig['reference']}, "
                f"Repository: {sig['repository']}, "
                f"Digest: {sig['manifest_digest']}, "
                f"Key: {sig['sig_key_id']}"
            )

        cert, key = get_pyxis_ssl_paths(self.settings)

        args = ["--pyxis-server", self.settings["pyxis_server"]]
        args += ["--pyxis-ssl-crtfile", cert]
        args += ["--pyxis-ssl-keyfile", key]
        args += ["--request-threads", str(self.settings.get("num_thread_pyxis", 7))]

        with self._save_signatures_file(signatures) as signature_file:
            args += ["--signatures", "@{0}".format(signature_file.name)]
            LOG.info("Uploading {0} new signatures".format(len(signatures)))
            env_vars: dict[Any, Any] = {}
            run_entrypoint(
                ("pubtools-pyxis", "console_scripts", "pubtools-pyxis-upload-signatures"),
                "pubtools-pyxis-upload-signature",
                args,
                env_vars,
                False,
            )

    def _run_remove_signatures(self, signatures_to_remove: List[str]) -> None:
        """Remove signatures from the sigstore.

        Args:
            signatures_to_remove (List[str]): List of signatures to remove.
        """
        cert, key = get_pyxis_ssl_paths(self.settings)
        args = []
        args = ["--pyxis-server", self.settings["pyxis_server"]]
        args += ["--pyxis-ssl-crtfile", cert]
        args += ["--pyxis-ssl-keyfile", key]
        args += ["--request-threads", str(self.settings.get("num_thread_pyxis", 7))]

        with tempfile.NamedTemporaryFile(mode="w") as temp:
            json.dump(signatures_to_remove, temp)
            temp.flush()

            args += ["--ids", "@%s" % temp.name]

            env_vars: dict[Any, Any] = {}
            run_entrypoint(
                ("pubtools-pyxis", "console_scripts", "pubtools-pyxis-delete-signatures"),
                "pubtools-pyxis-delete-signatures",
                args,
                env_vars,
            )

    def _filter_to_remove(
        self,
        signatures: List[Tuple[str, str, str]],
        _exclude: Optional[List[Tuple[str, str, str]]] = None,
    ) -> List[str]:
        """Filter signatures to remove.

        Args:
            signatures (List[Tuple[str, str, str]]): List of (digest, tag, repository)
            tuples of signautres to remove.
            _exclude (Optional[List[Tuple[str, str, str]]]): List of  (digest, tag, repository)
            tuples of signautres to keep.
        """
        exclude = _exclude or []
        signatures_to_remove = list(self._fetch_signatures([x[0] for x in signatures]))
        sig_ids_to_remove = []
        for existing_signature in signatures_to_remove:
            if (
                existing_signature["manifest_digest"],
                existing_signature["reference"].split(":")[-1],
                existing_signature["repository"],
            ) in signatures and (
                existing_signature["manifest_digest"],
                existing_signature["reference"],
                existing_signature["repository"],
            ) not in exclude:
                sig_ids_to_remove.append(existing_signature["_id"])
                LOG.debug(
                    f"Removing signature. Reference: {existing_signature['reference']}, "
                    f"Repository: {existing_signature['repository']}, "
                    f"Digest: {existing_signature['manifest_digest']}, "
                    f"Key: {existing_signature['sig_key_id']}"
                )
        return sig_ids_to_remove

    @log_step("Remove outdated signatures")
    def remove_signatures(
        self,
        signatures: List[Tuple[str, str, str]],
        _exclude: Optional[List[Tuple[str, str, str]]] = None,
    ) -> None:
        """Remove signatures from sigstore.

        Args:
            signatures (list): List of tuples containing (digest, reference, repository) of
            signatures to remove.
            exclude (Optional[List[Tuple[str, str, str]]]): List of  (digest, tag, repository)
            tuples of signautres to keep.
        """
        _signatures = list(signatures)
        to_remove = self._filter_to_remove(_signatures, _exclude=_exclude)
        self._remove_signatures(to_remove)


class CosignSignerSettingsSchema(Schema):
    """Validation schema for cosign signer settings."""

    quay_namespace = fields.String(required=True)
    quay_host = fields.String(required=True)
    dest_quay_api_token = fields.String(required=True)
    signing_chunk_size = fields.Integer(required=False, default=100)
    signing_parallelism = fields.Integer(required=False, default=10)


class CosignSignerWrapper(SignerWrapper):
    """Wrapper for cosign signer functionality."""

    label = "cosign_signer"
    pre_push = False

    entry_point_conf = ["pubtools-sign", "modules", "pubtools-sign-cosign-container-sign"]

    SCHEMA = CosignSignerSettingsSchema

    def _list_signatures(self, repository: str, tag: str) -> List[Tuple[str, str]]:
        """List cosign signatures for given repository.

        This methods runs pubtools-sign-cosign-container-list entrypoint which is expected to
        return list of full references to signature tags in format sha256-<digest>.sig

        Args:
            repository (str): Repository to list signatures for.
            tag (str): Tag to list signatures for.
        Returns:
            List[Tuple[str, str]]: List of (repository, signature tag) tuples
            for existing signatures.
        """
        full_reference = (
            f"{self.settings['quay_host']}/"
            + f"{self.settings['quay_namespace']}/{repository.replace('/','----')}"
            + f":{tag}"
        )
        existing_signatures = run_entrypoint(
            ("pubtools-sign", "modules", "pubtools-sign-cosign-container-list"),
            "pubtools-sign-cosign-container-list",
            [full_reference],
            {},
        )
        if existing_signatures[0]:
            return [
                (repository, e.split(":")[-1].replace(".sig", "").replace("-", ":"))
                for e in existing_signatures[1]
            ]
        else:
            LOG.warning("Fetch existing signatures error:" + existing_signatures[1])
            return []

    def _filter_to_remove(
        self,
        signatures: List[Tuple[str, str, str]],
        _exclude: Optional[List[Tuple[str, str, str]]] = None,
    ) -> List[str]:
        """Filter signatures to remove.

        Args:
            signatures (List[Tuple[str, str, str]]): List of (digest, tag, repository)
            tuples of signautres to remove.
            _exclude (Optional[List[Tuple[str, str, str]]]): List of  (digest, tag, repository)
            tuples of signautres to keep.
        """
        repo_tag_list = list(set([(x[2], x[1]) for x in signatures]))
        signatures_to_remove = [(x[2], x[0]) for x in signatures]
        signatures_to_exclude = [(x[2], x[0]) for x in _exclude or []]
        existing_signatures = set(
            sum(
                run_in_parallel(
                    self._list_signatures, [FData(args=repo_tag) for repo_tag in repo_tag_list]
                ).values(),
                [],
            )
        )

        to_remove = []
        for existing_signature in existing_signatures:
            if (
                existing_signature in signatures_to_remove
                and existing_signature not in signatures_to_exclude
            ):
                to_remove.append(existing_signature)
                LOG.debug(
                    f"Removing signature "
                    f"Repository: {existing_signature[0]}, "
                    f"Digest: {existing_signature[1]}, "
                )
        return to_remove

    def _run_remove_signatures(self, signatures_to_remove: List[Tuple[str, str]]) -> None:
        """Remove signatures from the sigstore.

        Args:
            signatures_to_remove (List[Tuple(str, str)]): List of signatures to remove.
        """
        qc = QuayApiClient(self.settings["dest_quay_api_token"], host=self.settings["quay_host"])
        for sig_to_remove in signatures_to_remove:
            ref = self.settings["quay_namespace"] + "/" + sig_to_remove[0].replace("/", "----")
            sig_tag = sig_to_remove[1].replace(":", "-") + ".sig"
            qc.delete_tag(ref, sig_tag)

    def remove_signatures(
        self,
        signatures: List[Tuple[str, str, str]],
        _exclude: Optional[List[Tuple[str, str, str]]] = None,
    ) -> None:
        """Remove signatures from sigstore.

        Args:
            signatures (list): List of tuples containing (digest, reference, repository) of
            signatures to remove.
            exclude (Optional[List[Tuple[str, str, str]]]): List of  (digest, tag, repository)
            tuples of signautres to keep.
        """
        to_remove = self._filter_to_remove(signatures, _exclude=_exclude)
        self._remove_signatures(to_remove)


SIGNER_BY_LABEL = {
    wrapper.label: wrapper
    for name, wrapper in locals().items()
    if type(wrapper) is type and issubclass(wrapper, SignerWrapper) and wrapper != SignerWrapper
}
