"""
Starbelly Command Line Client
"""
import argparse
from base64 import b64decode
from google.protobuf.json_format import MessageToJson
import inspect
import logging
import os
import trio
from trio_websocket import ConnectionClosed, ConnectionRejected
from starbelly_proto import starbelly_pb2

from .client import connect_starbelly


logger = logging.getLogger("StarbellyCli")
_JOB_RUN_STATES = {
    "CANCELLED": starbelly_pb2.JobRunState.CANCELLED,
    "COMPLETED": starbelly_pb2.JobRunState.COMPLETED,
    "PAUSED": starbelly_pb2.JobRunState.PAUSED,
    "PENDING": starbelly_pb2.JobRunState.PENDING,
    "RUNNING": starbelly_pb2.JobRunState.RUNNING,
}


async def cli(args):
    """Run the command line client
    """
    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.WARN

    logging.basicConfig(level=log_level)
    try:
        async with connect_starbelly(args.url, args.username, args.password) as conn:
            func = getattr(conn, args.func)
            sig = inspect.signature(func)
            func_args = {k: getattr(args, k) for k in sig.parameters}
            if inspect.isasyncgenfunction(func):
                async for event in func(**func_args):
                    print(MessageToJson(event))
            else:
                response = await func(**func_args)
                print(MessageToJson(response))
                await conn.done()
    except ConnectionClosed as cc:
        logger.debug("Connection closed")
    except ConnectionRejected as cc:
        print(f"{cc.status_code} Connection rejected: {cc.body}")
    except OSError as ose:
        logger.error("Connection attempt failed: %s", ose)


def _cmd_environ_or_required(key):
    return (
        {"default": os.environ.get(key)} if os.environ.get(key) else {"required": True}
    )


def _build_cmd_parser():
    parser = argparse.ArgumentParser(
        description="Starbelly Command Line Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    required_options = parser.add_argument_group("required named arguments")
    required_options.add_argument(
        "--username",
        "-u",
        help="Starbelly username",
        **_cmd_environ_or_required("STARBELLY_USERNAME"),
    )
    required_options.add_argument(
        "--password",
        "-p",
        help="Starbelly password",
        **_cmd_environ_or_required("STARBELLY_PASSWORD"),
    )
    required_options.add_argument(
        "--url",
        "-U",
        help="Starbelly url",
        **_cmd_environ_or_required("STARBELLY_URL"),
    )
    _build_cmd_subparsers(parser)
    return parser


def _build_delete_captcha_solver_parser(subparsers: argparse.ArgumentParser):
    delete_captcha_solver_parser = subparsers.add_parser(
        "delete-captcha-solver", help="delete captcha solver"
    )
    delete_captcha_solver_parser.set_defaults(func="delete_captcha_solver")
    delete_captcha_solver_parser.add_argument(
        "captcha_solver_id",
        metavar="captcha_solver_id",
        help="captcha solver ID [base64 encoded]",
        type=b64decode,
    )
    delete_captcha_solver_parser.set_defaults(func="delete_captcha_solver")


def _build_delete_domain_login_parser(subparsers: argparse.ArgumentParser):
    delete_domain_login_parser = subparsers.add_parser(
        "delete-domain-login", help="delete login for domain"
    )
    delete_domain_login_parser.set_defaults(func="delete_domain_login")
    delete_domain_login_parser.add_argument(
        "domain_login_id",
        metavar="domain_login_id",
        help="domain login ID [base64 encoded]",
        type=b64decode,
    )
    delete_domain_login_parser.set_defaults(func="delete_domain_login")


def _build_delete_job_parser(subparsers: argparse.ArgumentParser):
    delete_job_parser = subparsers.add_parser("delete-job", help="delete job")
    delete_job_parser.set_defaults(func="delete_job")
    delete_job_parser.add_argument(
        "job_id", metavar="job-id", help="job ID [base64 encoded]", type=b64decode
    )
    delete_job_parser.set_defaults(func="delete_job")


def _build_delete_policy_parser(subparsers: argparse.ArgumentParser):
    delete_policy_parser = subparsers.add_parser("delete-policy", help="delete policy")
    delete_policy_parser.set_defaults(func="delete_policy")
    delete_policy_parser.add_argument(
        "policy_id",
        metavar="policy-id",
        help="policy ID [base64 encoded]",
        type=b64decode,
    )
    delete_policy_parser.set_defaults(func="delete_policy")


def _build_get_captcha_solver_parser(subparsers: argparse.ArgumentParser):
    get_captcha_solver_parser = subparsers.add_parser(
        "get-captcha-solver", help="get captcha solver"
    )
    get_captcha_solver_parser.set_defaults(func="get_captcha_solver")
    get_captcha_solver_parser.add_argument(
        "captcha_solver_id",
        metavar="captcha-solver-id",
        help="captcha solver ID [base64 encoded]",
        type=b64decode,
    )
    get_captcha_solver_parser.set_defaults(func="get_captcha_solver")


def _build_get_job_parser(subparsers: argparse.ArgumentParser):
    get_job_parser = subparsers.add_parser("get-job", help="get job")
    get_job_parser.set_defaults(func="get_job")
    get_job_parser.add_argument(
        "job_id", metavar="job-id", help="job ID [base64 encoded]", type=b64decode
    )
    get_job_parser.set_defaults(func="get_job")


def _build_get_job_items_parser(subparsers: argparse.ArgumentParser):
    get_job_items_parser = subparsers.add_parser("get-job-items", help="get job items")
    get_job_items_parser.set_defaults(func="get_job_items")
    get_job_items_parser.add_argument(
        "job_id", metavar="job-id", help="job ID [base64 encoded]", type=b64decode
    )
    get_job_items_parser.add_argument(
        "--include-success",
        help="include successful crawl items",
        type=int,
        choices=[0, 1],
        default=1,
    )
    get_job_items_parser.add_argument(
        "--include-error",
        help="include error crawl items",
        type=int,
        choices=[0, 1],
        default=1,
    )
    get_job_items_parser.add_argument(
        "--include-exception",
        help="include crawl items that raised exceptions",
        type=int,
        choices=[0, 1],
        default=1,
    )
    get_job_items_parser.add_argument(
        "--compression_ok", help="compress data", type=bool, choices=[0, 1], default=1,
    )
    get_job_items_parser.add_argument("--offset", help="results offset", type=int)
    get_job_items_parser.add_argument("--limit", help="results to return", type=int)
    get_job_items_parser.set_defaults(func="get_job_items")


def _build_get_policy_parser(subparsers: argparse.ArgumentParser):
    get_policy_parser = subparsers.add_parser("get-policy", help="get policy")
    get_policy_parser.set_defaults(func="get_policy")
    get_policy_parser.add_argument(
        "policy_id",
        metavar="policy-id",
        help="policy ID [base64 encoded]",
        type=b64decode,
    )
    get_policy_parser.set_defaults(func="get_policy")


def _build_list_policies_parser(subparsers: argparse.ArgumentParser):
    list_policies_parser = subparsers.add_parser("list-policies", help="list policies")
    list_policies_parser.add_argument("--offset", help="results offset", type=int)
    list_policies_parser.add_argument("--limit", help="results to return", type=int)
    list_policies_parser.set_defaults(func="list_policies")


def _build_list_captcha_solvers_parser(subparsers: argparse.ArgumentParser):
    list_captcha_solvers_parser = subparsers.add_parser(
        "list-captcha-solvers", help="list captcha solvers"
    )
    list_captcha_solvers_parser.add_argument(
        "--offset", help="results offset", type=int
    )
    list_captcha_solvers_parser.add_argument(
        "--limit", help="results to return", type=int
    )
    list_captcha_solvers_parser.set_defaults(func="list_captcha_solvers")


def _build_list_jobs_parser(subparsers: argparse.ArgumentParser):
    list_jobs_parser = subparsers.add_parser("list-jobs", help="list jobs")
    list_jobs_parser.add_argument("--offset", help="results offset", type=int)
    list_jobs_parser.add_argument("--limit", help="results to return", type=int)
    list_jobs_parser.add_argument("--started-after", type=str)
    list_jobs_parser.add_argument("--tag", type=str)
    list_jobs_parser.add_argument("--schedule-id", type=bytes)
    list_jobs_parser.set_defaults(func="list_jobs")


def _build_list_schedules_parser(subparsers: argparse.ArgumentParser):
    list_schedules_parser = subparsers.add_parser(
        "list-schedules", help="list schedules"
    )
    list_schedules_parser.add_argument(
        "--page", "-p", type=int, help="results page number"
    )
    list_schedules_parser.add_argument("--offset", help="results offset", type=int)
    list_schedules_parser.add_argument("--limit", help="results to return", type=int)
    list_schedules_parser.set_defaults(func="list_schedules")


def _build_list_domain_logins_parser(subparsers: argparse.ArgumentParser):
    list_domain_logins_parser = subparsers.add_parser(
        "list-domain-logins", help="list domain logins"
    )
    list_domain_logins_parser.add_argument("--offset", help="results offset", type=int)
    list_domain_logins_parser.add_argument(
        "--limit", help="results to return", type=int
    )
    list_domain_logins_parser.set_defaults(func="list_domain_logins")


def _build_list_rate_limit_parser(subparsers: argparse.ArgumentParser):
    list_rate_limits_parser = subparsers.add_parser(
        "list-rate-limits", help="list rate limits"
    )
    list_rate_limits_parser.add_argument("--offset", help="results offset", type=int)
    list_rate_limits_parser.add_argument("--limit", help="results to return", type=int)
    list_rate_limits_parser.set_defaults(func="list_rate_limits")


def _build_performance_profile_parser(subparsers: argparse.ArgumentParser):
    performance_profile_parser = subparsers.add_parser(
        "performance-profile", help="get usage stats"
    )
    performance_profile_parser.add_argument("--duration", type=float)
    performance_profile_parser.add_argument("--sort-by", type=str)
    performance_profile_parser.add_argument("--top-n", type=int)
    performance_profile_parser.set_defaults(func="performance_profile")


def _build_set_job_parser(subparsers: argparse.ArgumentParser):
    set_job_parser = subparsers.add_parser("set-job", help="set job")
    set_job_parser.set_defaults(func="set_job")
    set_job_parser.add_argument(
        "run_state",
        metavar="run-state",
        help=f"run state [{','.join(_JOB_RUN_STATES.keys())}]",
        type=lambda run_state: _JOB_RUN_STATES[run_state],
        choices=starbelly_pb2.JobRunState.keys(),
    )
    set_job_parser.add_argument(
        "policy_id",
        metavar="policy-id",
        help="policy ID [base64 encoded]",
        type=b64decode,
    )
    set_job_parser.add_argument(
        "name", help="job name", type=str,
    )
    set_job_parser.add_argument(
        "--tags",
        help="job tags ('tag1,tag2,tag3')",
        type=lambda s: [i.strip() for i in s.split(",") if i.strip()],
    )
    set_job_parser.add_argument("seeds", help="job seed url(s)", nargs="+")
    set_job_parser.set_defaults(func="set_job")


def _build_subscribe_job_status_parser(subparsers: argparse.ArgumentParser):
    subscribe_job_status_parser = subparsers.add_parser(
        "subscribe-job-status", help="subscribe to job status"
    )
    subscribe_job_status_parser.add_argument("--min-interval", type=int)
    subscribe_job_status_parser.set_defaults(func="subscribe_job_status")


def _build_subscribe_job_sync_parser(subparsers: argparse.ArgumentParser):
    subscribe_job_sync_parser = subparsers.add_parser(
        "subscribe-job-sync", help="subscribe to job sync"
    )
    subscribe_job_sync_parser.add_argument(
        "job_id", metavar="job-id", type=b64decode, help="job ID [base64 encoded]"
    )
    subscribe_job_sync_parser.add_argument(
        "--sync-token", type=str, help="the job job sync token"
    )
    subscribe_job_sync_parser.add_argument(
        "--compression_ok", help="compress data", type=bool, choices=[0, 1], default=1,
    )
    subscribe_job_sync_parser.set_defaults(func="subscribe_job_sync")


def _build_subscribe_resource_monitor_parser(subparsers: argparse.ArgumentParser):
    subscribe_resource_monitor_parser = subparsers.add_parser(
        "subscribe-resource-monitor", help="subscribe to resource monitor"
    )
    subscribe_resource_monitor_parser.add_argument("--history", type=int)
    subscribe_resource_monitor_parser.set_defaults(func="subscribe_resource_monitor")


def _build_subscribe_task_monitor_parser(subparsers: argparse.ArgumentParser):
    subscribe_task_monitor_parser = subparsers.add_parser(
        "subscribe-task-monitor", help="subscribe to task monitor"
    )
    subscribe_task_monitor_parser.add_argument("--period", type=int)
    subscribe_task_monitor_parser.set_defaults(func="subscribe_task_monitor")


def _build_unsubscribe_parser(subparsers: argparse.ArgumentParser):
    unsubscribe_parser = subparsers.add_parser(
        "unsubscribe", help="unsubscribe from subscription"
    )
    unsubscribe_parser.add_argument(
        "subscription_id", metavar="subscription-id", type=int
    )
    unsubscribe_parser.set_defaults(func="unsubscribe")


def _build_cmd_subparsers(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        title="available commands",
        metavar="command [options]",
    )
    # Delete captcha solver
    _build_delete_captcha_solver_parser(subparsers)
    # Delete domain login
    _build_delete_domain_login_parser(subparsers)
    # Delete job
    _build_delete_job_parser(subparsers)
    # Delete policy
    _build_delete_policy_parser(subparsers)
    # Get captcha solver
    _build_get_captcha_solver_parser(subparsers)
    # Get job
    _build_get_job_parser(subparsers)
    # Get job items
    _build_get_job_items_parser(subparsers)
    # Get policy
    _build_get_policy_parser(subparsers)
    # List policies
    _build_list_policies_parser(subparsers)
    # List captcha solvers
    _build_list_policies_parser(subparsers)
    # List jobs
    _build_list_jobs_parser(subparsers)
    # List schedules
    _build_list_schedules_parser(subparsers)
    # List domain logins
    _build_list_domain_logins_parser(subparsers)
    # List rate limits
    _build_list_rate_limit_parser(subparsers)
    # Performance profile
    _build_performance_profile_parser(subparsers)
    # Set job
    _build_set_job_parser(subparsers)
    # Subscribe job status
    _build_subscribe_job_status_parser(subparsers)
    # Subscribe job sync
    _build_subscribe_job_sync_parser(subparsers)
    # Subscribe resource monitor
    _build_subscribe_resource_monitor_parser(subparsers)
    # Subscribe task monitor
    _build_subscribe_task_monitor_parser(subparsers)
    # Unsubscribe
    _build_unsubscribe_parser(subparsers)


def _parse_cmd_args():
    parser = _build_cmd_parser()
    args = parser.parse_args()
    return args


def main():
    args = _parse_cmd_args()
    trio.run(cli, args)
