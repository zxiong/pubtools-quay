import logging

from .utils import setup_arg_parser, add_args_env_variables
from .command_executor import LocalExecutor, RemoteExecutor

LOG = logging.getLogger("pubtools-quay")
LOG.setLevel(logging.INFO)

TAG_IMAGES_ARGS = {
    ("--source-ref",): {
        "help": "Source image reference.",
        "required": True,
        "type": str,
    },
    ("--dest-ref",): {
        "help": "Destination image reference. Multiple can be specified.",
        "required": True,
        "type": str,
        "action": "append",
    },
    ("--quay-user",): {
        "help": "Username for Quay login.",
        "required": False,
        "type": str,
    },
    ("--quay-token",): {
        "help": "Authentication token for Quay. Can be specified by env variable AUTH_TOKEN.",
        "required": False,
        "type": str,
        "env_variable": "AUTH_TOKEN",
    },
    ("--remote-exec",): {
        "help": "Flag of whether the commands should be executed on a remote server.",
        "required": False,
        "type": bool,
    },
    ("--ssh-remote-host",): {
        "help": "Hostname for remote execution.",
        "required": False,
        "type": str,
    },
    ("--ssh-remote-host-port",): {
        "help": "Port of the remote host",
        "required": False,
        "type": int,
    },
    ("--ssh-reject-unknown-host",): {
        "help": "Flag of whether to reject an SSH host when it's not found among known hosts.",
        "required": False,
        "type": bool,
    },
    ("--ssh-username",): {
        "help": "Username for SSH connection. Defaults to local username.",
        "required": False,
        "type": str,
    },
    ("--ssh-password",): {
        "help": "Password for SSH. Will only be used if key-based validation is not available. "
        "Can be specified by env variable SSH_PASSWORD",
        "required": False,
        "type": str,
        "env_variable": "SSH_PASSWORD",
    },
    ("--ssh-key-filename",): {
        "help": "Path to the private key file for SSH authentication.",
        "required": False,
        "type": str,
    },
    ("--send-umb-msg",): {
        "help": "Flag of whether to send a UMB message",
        "required": False,
        "type": bool,
    },
    ("--umb-url",): {
        "help": "UMB URL. More than one can be specified.",
        "required": False,
        "type": str,
        "action": "append",
    },
    ("--umb-cert",): {
        "help": "Path to the UMB certificate for SSL authentication.",
        "required": False,
        "type": str,
    },
    ("--umb-client-key",): {
        "help": "Path to the UMB private key for accessing the certificate.",
        "required": False,
        "type": str,
    },
    ("--umb-ca-cert",): {
        "help": "Path to the UMB CA certificate.",
        "required": False,
        "type": str,
    },
    ("--umb-topic",): {
        "help": "UMB topic to send the message to.",
        "required": False,
        "type": str,
    },
}


def tag_images(args):
    """Tag images main function."""
    if args.remote_exec:
        accept_host = (
            not args.ssh_reject_unknown_host if args.ssh_reject_unknown_host else True
        )
        executor = RemoteExecutor(
            args.ssh_remote_host,
            args.ssh_username,
            args.ssh_key_filename,
            args.ssh_password,
            args.ssh_remote_host_port,
            accept_host,
        )
    else:
        executor = LocalExecutor()

    dest_refs = args.dest_ref if isinstance(args.dest_ref, list) else [args.dest_ref]
    executor.skopeo_login(args.quay_user, args.quay_token)
    executor.tag_images(args.source_ref, dest_refs)

    if args.send_umb_msg:
        from rhmsg.activemq.producer import AMQProducer

        topic = args.umb_topic or "VirtualTopic.eng.pub.quay_tag_image"
        producer = AMQProducer(
            urls=args.umb_url,
            certificate=args.umb_cert,
            private_key=args.umb_client_key,
            topic=topic,
            trusted_certificates=args.umb_ca_cert,
        )
        msg = {"source_ref": args.source_ref, "dest_refs": dest_refs}
        producer.send_msg({}, msg)


def verify_tag_images_args(args):
    """Verify the presence of input parameters."""
    if args.remote_exec:
        if not args.ssh_remote_host:
            raise ValueError(
                "Remote host is missing when remote execution was specified."
            )

    if (args.quay_user and not args.quay_token) or (
        args.quay_token and not args.quay_user
    ):
        raise ValueError(
            "Both user and token must be present when attempting to log in."
        )

    if args.send_umb_msg:
        if not args.umb_url:
            raise ValueError(
                "UMB URL must be specified if sending a UMB message was requested."
            )
        if not args.umb_cert:
            raise ValueError(
                "A path to a client certificate must be provided "
                "when sending a UMB message."
            )


def tag_images_main(sysargs=None):
    """Entrypoint for image tagging."""
    parser = setup_arg_parser(TAG_IMAGES_ARGS)
    if sysargs:
        args = parser.parse_args(sysargs[1:])
    else:
        args = parser.parse_args()  # pragma: no cover"
    args = add_args_env_variables(args, TAG_IMAGES_ARGS)

    verify_tag_images_args(args)
    tag_images(args)
