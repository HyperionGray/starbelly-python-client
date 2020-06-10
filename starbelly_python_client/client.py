"""
Python Starbelly Client.
"""
import argparse
from base64 import b64encode
from contextlib import asynccontextmanager
import fire
from google.protobuf.json_format import MessageToJson
import itertools
import inspect
import logging
import os
import ssl
import trio
import trio_websocket
from starbelly_proto import starbelly_pb2

logger = logging.getLogger(__name__)


@asynccontextmanager
async def connect_starbelly(
    url: str, username: str, password: str, json_format: bool = False
):
    """
    Return Starbelly client connected over websocket. 
    """
    credential = "{}:{}".format(username, password)
    basic_auth_hash = b64encode(credential.encode("ascii")).decode("ascii")
    # Basic Auth Header
    headers = [("Authorization", f"Basic {basic_auth_hash}")]
    # Disable SSL verification
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Open websocket connection
    async with trio_websocket.open_websocket_url(
        url=url, extra_headers=headers, ssl_context=ssl_context
    ) as ws:
        # create client and run background tasks
        async with trio.open_nursery() as nursery:
            conn = StarbellyConnection(ws, json_format=json_format)
            nursery.start_soon(conn.background_task)
            yield conn


class StarbellyConnection:
    def __init__(self, ws: trio_websocket.WebSocketConnection, json_format=False):
        self.ws = ws
        self.requests = dict()
        self.request_id = itertools.count()
        self.json_format = json_format

    async def delete_captcha_solver(self, solver_id=None):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            delete_captcha_solver=starbelly_pb2.RequestDeleteCaptchaSolver(
                solver_id=solver_id
            ),
        )
        return await self.send_request(request)

    async def get_captcha_solver(self, solver_id: bytes):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            get_captcha_solver=starbelly_pb2.RequestGetCaptchaSolver(
                solver_id=solver_id
            ),
        )
        return await self.send_request(request)

    async def list_captcha_solvers(self, page=None):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_captcha_solvers=starbelly_pb2.RequestListCaptchaSolvers(page=page),
        )
        return await self.send_request(request)

    async def list_policies(self, page):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_policies=starbelly_pb2.RequestListPolicies(page=page),
        )
        return await self.send_request(request)

    async def send_request(self, request):
        when_completed = trio.Event()
        self.requests[request.request_id] = when_completed
        logger.debug(f"send request: {request}")
        await self.ws.send_message(request.SerializeToString())
        await when_completed.wait()
        return self.requests.pop(request.request_id)

    async def background_task(self):
        while True:
            msg = await self.ws.get_message()
            server_message = starbelly_pb2.ServerMessage()
            server_message.ParseFromString(msg)
            logger.debug(f"Response: {server_message}")
            if server_message.HasField("response"):
                request_id = server_message.response.request_id
                when_completed = self.requests[request_id]
                self.requests[request_id] = server_message.response
                if self.json_format:
                    print(MessageToJson(server_message.response))
                else:
                    print(server_message.response)
                when_completed.set()

    async def done(self):
        # await self.ws.aclose(code=1000, reason="Done")
        await self.ws.aclose(code=1000, reason="Done")


#######################
# Command Line Client #
#######################
async def cli(args):
    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.WARN

    logging.basicConfig(level=log_level)
    try:
        async with connect_starbelly(
            args.url, args.username, args.password, json_format=True
        ) as conn:
            func = getattr(conn, args.func)
            sig = inspect.signature(func)
            func_args = {k: getattr(args, k) for k in sig.parameters}
            await func(**func_args)
            await conn.done()
    except trio_websocket.ConnectionClosed as cc:
        logger.debug("Connection closed")
    except trio_websocket.ConnectionRejected as cc:
        print(f"{cc.status_code} Connection rejected: {cc.body}")
    except OSError as ose:
        logger.error("Connection attempt failed: %s", ose)


def _cmd_environ_or_required(key):
    return (
        {"default": os.environ.get(key)} if os.environ.get(key) else {"required": True}
    )


def _build_cmd_parser():
    parser = argparse.ArgumentParser()
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


def _build_cmd_subparsers(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(dest="command", required=True)
    list_policies_parser = subparsers.add_parser("list-policies")
    list_policies_parser.set_defaults(func="list_policies")
    list_policies_parser.add_argument("--page", "-p", help="results page number")
    list_captcha_solvers_parser = subparsers.add_parser("list-captcha-solvers")
    list_captcha_solvers_parser.set_defaults(func="list_captcha_solvers")
    list_captcha_solvers_parser.add_argument("--page", "-p", help="results page number")


def _parse_cmd_args():
    parser = _build_cmd_parser()
    args = parser.parse_args()
    return args


def main():
    args = _parse_cmd_args()
    trio.run(cli, args)


if __name__ == "__main__":
    main()
