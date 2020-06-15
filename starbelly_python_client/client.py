"""
Python Starbelly Client.
"""
import argparse
from base64 import b64encode
from contextlib import asynccontextmanager
from google.protobuf.json_format import MessageToJson
import itertools
import logging
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

    async def delete_captcha_solver(self, solver_id: bytes):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            delete_captcha_solver=starbelly_pb2.RequestDeleteCaptchaSolver(
                solver_id=solver_id
            ),
        )
        return await self.send_request(request)

    async def delete_domain_login(self, domain_login_id: bytes):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            delete_domain_login=starbelly_pb2.RequestDeleteDomainLogin(
                domain_login_id=domain_login_id
            ),
        )
        return await self.send_request(request)

    async def delete_job(self, job_id: bytes):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            delete_job=starbelly_pb2.RequestDeleteJob(job_id=job_id),
        )
        return await self.send_request(request)

    async def delete_policy(self, policy_id: bytes):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            delete_policy=starbelly_pb2.RequestDeletePolicy(policy_id=policy_id),
        )
        return await self.send_request(request)

    async def delete_schedule(self, schedule_id: bytes):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            delete_schedule=starbelly_pb2.RequestDeleteSchedule(
                schedule_id=schedule_id
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

    async def get_domain_login(
        self, domain_login_id: bytes
    ) -> starbelly_pb2.DomainLogin:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            get_domain_login=starbelly_pb2.RequestGetDomainLogin(
                domain_login_id=domain_login_id
            ),
        )
        return await self.send_request(request)

    async def get_job(self, job_id: bytes) -> starbelly_pb2.Job:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            get_job=starbelly_pb2.RequestGetJob(job_id=job_id),
        )
        return await self.send_request(request)

    async def get_job_items(
        self,
        job_id: bytes,
        include_success: bool = True,
        include_error: bool = True,
        include_exception: bool = True,
        compression_ok: bool = True,
        offset: int = None,
        limit: int = None,
    ) -> starbelly_pb2.ResponseListItems:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            get_job_items=starbelly_pb2.RequestGetJobItems(
                job_id=job_id,
                include_success=include_success,
                include_error=include_error,
                include_exception=include_exception,
                page=page,
            ),
        )
        return await self.send_request(request)

    async def get_schedule(self, schedule_id: bytes) -> starbelly_pb2.Schedule:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            get_schedule=starbelly_pb2.RequestGetSchedule(schedule_id=schedule_id),
        )
        return await self.send_request(request)

    async def get_policy(self, policy_id: bytes) -> starbelly_pb2.Policy:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            get_policy=starbelly_pb2.RequestGetPolicy(policy_id=policy_id),
        )
        return await self.send_request(request)

    async def list_captcha_solvers(
        self, page: int = None
    ) -> starbelly_pb2.ResponseListCaptchaSolvers:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_captcha_solvers=starbelly_pb2.RequestListCaptchaSolvers(page=page),
        )
        return await self.send_request(request)

    async def list_policies(
        self, offset: int = None, limit: int = None
    ) -> starbelly_pb2.ResponseListPolicies:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_policies=starbelly_pb2.RequestListPolicies(page=page),
        )
        return await self.send_request(request)

    async def list_jobs(
        self,
        offset: int = None,
        limit: int = None,
        started_after: str = None,
        tag: str = None,
        schedule_id: bytes = None,
    ) -> starbelly_pb2.ResponseListJobs:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_jobs=starbelly_pb2.RequestListJobs(
                page=page,
                started_after=started_after,
                tag=tag,
                schedule_id=schedule_id,
            ),
        )
        return await self.send_request(request)

    async def list_schedules(
        self, offset: int = None, limit: int = None
    ) -> starbelly_pb2.ResponseListSchedules:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_schedules=starbelly_pb2.RequestListSchedules(page=page),
        )
        return await self.send_request(request)

    async def list_domain_logins(
        self, offset: int = None, limit: int = None
    ) -> starbelly_pb2.ResponseListDomainLogins:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_domain_logins=starbelly_pb2.RequestListDomainLogins(page=page),
        )
        return await self.send_request(request)

    async def list_rate_limits(
        self, offset: int = None, limit: int = None
    ) -> starbelly_pb2.ResponseListRateLimits:
        page = starbelly_pb2.Page(limit=limit, offset=offset)
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            list_rate_limits=starbelly_pb2.RequestListRateLimits(page=page),
        )
        return await self.send_request(request)

    async def performance_profile(
        self, duration: float = None, sort_by: str = None, top_n: int = None
    ) -> starbelly_pb2.ResponsePerformanceProfile:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            performance_profile=starbelly_pb2.RequestPerformanceProfile(
                duration=duration, sort_by=sort_by, top_n=top_n
            ),
        )
        return await self.send_request(request)

    async def set_job(
        self,
        run_state: starbelly_pb2.JobRunState,
        policy_id: bytes,
        seeds: list,
        name: str,
        tags: list = [],
    ) -> starbelly_pb2.ResponseNewJob:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            set_job=starbelly_pb2.RequestSetJob(
                run_state=run_state,
                policy_id=policy_id,
                seeds=seeds,
                name=name,
                tags=tags,
            ),
        )
        return await self.send_request(request)

    async def subscribe_job_status(
        self, min_interval: float = None
    ) -> starbelly_pb2.ResponseNewSubscription:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            subscribe_job_status=starbelly_pb2.RequestSubscribeJobStatus(
                min_interval=min_interval
            ),
        )
        # TODO: implemement subscription
        response = await self.send_request(request)

    async def subscribe_job_status(
        self, min_interval: float = None
    ) -> starbelly_pb2.ResponseNewSubscription:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            subscribe_job_status=starbelly_pb2.RequestSubscribeJobStatus(
                min_interval=min_interval
            ),
        )
        # TODO: implemement subscription
        response = await self.send_request(request)

    async def subscribe_job_sync(
        self, job_id: bytes, sync_token: bytes = None, compression_ok: bool = True
    ) -> starbelly_pb2.ResponseNewSubscription:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            subscribe_job_sync=starbelly_pb2.RequestSubscribeJobSync(
                job_id=job_id, sync_token=sync_token, compression_ok=compression_ok
            ),
        )
        # TODO: implemement subscription
        response = await self.send_request(request)

    async def subscribe_resource_monitor(
        self, history: int = None
    ) -> starbelly_pb2.ResponseNewSubscription:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            subscribe_resource_monitor=starbelly_pb2.RequestSubscribeResourceMonitor(
                history=history
            ),
        )
        # TODO: implemement subscription
        response = await self.send_request(request)

    async def subscribe_task_monitor(
        self, period: int = None
    ) -> starbelly_pb2.ResponseNewSubscription:
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            subscribe_task_monitor=starbelly_pb2.RequestSubscribeTaskMonitor(
                period=period
            ),
        )
        # TODO: implemement subscription
        response = await self.send_request(request)

    async def unsubscribe(self, subscripion_id: int):
        request = starbelly_pb2.Request(
            request_id=next(self.request_id),
            unsubscribe=starbelly_pb2.RequestUnsubscribe(subscripion_id=subscripion_id),
        )
        # TODO: implemement unsubscription
        response = await self.send_request(request)

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
