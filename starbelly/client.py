import asyncio
from base64 import b64encode
import itertools
import logging
import ssl
import websockets

from aiokit import cancel_futures, daemon_task, wait_first
import starbelly_proto

logger = logging.getLogger(__name__)


class StarbellyError(Exception):
    ''' A server-side error from Starbelly. '''


class StarbellyServer:
    ''' Manages a connection to a Starbelly server. '''

    def __init__(self):
        ''' Constructor. '''
        self._headers = None
        self._pending_requests = dict()
        self._reader_task = None
        self._reconnect_future = asyncio.Future()
        self._request_seq = itertools.count()
        self._subscriptions = dict()
        self._subscription_closing = set()
        self._url = None

    async def connect(self, url, username, password):
        ''' Connect to Starbelly server. '''
        credential = '{}:{}'.format(username, password)
        hash_ = b64encode(credential.encode('ascii')).decode('ascii')
        headers = {'Authorization': 'Basic {}'.format(hash_)}
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        logger.info('Connecting to %s', url)
        self._socket = await websockets.connect(url, extra_headers=headers,
            ssl=ssl_context)
        self._headers = headers
        self._url = url
        self._reader_task = daemon_task(self._reader())

    async def close(self):
        ''' Disconnect. '''
        await cancel_futures(self._reader_task)
        await asyncio.gather(*self._pending_requests.values())
        await self._socket.close()

    async def send(self, request):
        '''
        Send a request and return a response.

        The request is expected to *not* have a request ID. This class takes
        care of assigning request IDs and returns the associated response.
        '''
        logger.debug('Sending request: %r', request)
        response_future = asyncio.Future()
        request.request_id = next(self._request_seq)
        self._pending_requests[request.request_id] = response_future
        request_data = request.SerializeToString()

        while True:
            try:
                await self._socket.send(request_data)
                break
            except websockets.exceptions.ConnectionClosed:
                await self._reconnect()
                continue

        response = await response_future
        return response

    async def unsubscribe(self, subscription):
        ''' Unsubscribe. '''
        subscription.cancel()
        sub_id = subscription.id
        del self._subscriptions[sub_id]
        request = starbelly_proto.Request()
        request.unsubscribe.subscription_id = sub_id
        response = await self.send(request)

    async def _process_event(self, event):
        ''' Process one event from the server. '''
        event_type = event.WhichOneof('Body')
        sub_id = event.subscription_id

        try:
            subscription = self._subscriptions[sub_id]
        except KeyError:
            return

        if sub_id in self._subscriptions:
            await subscription.put(event)

    async def _process_response(self, response):
        ''' Process one response from the server. '''
        request_id = response.request_id
        response_future = self._pending_requests.pop(request_id)

        if response_future.cancelled():
            # The coroutine waiting for this response was cancelled, so
            # we can discard the response.
            return

        if response.is_success:
            response_type = response.WhichOneof('Body')
            if response_type == 'new_subscription':
                sub_id = response.new_subscription.subscription_id
                subscription = StarbellySubscription(sub_id)
                self._subscriptions[sub_id] = subscription
                response_future.set_result(subscription)
            else:
                response_future.set_result(response)
        else:
            exc = StarbellyError(response.error_message)
            response_future.set_exception(exc)

    async def _reader(self):
        ''' Reads from socket and dispatches server messages. '''
        while True:
            try:
                message_data = await self._socket.recv()
            except websockets.exceptions.ConnectionClosed:
                logger.debug('Connection closed!')
                await self._reconnect()
                continue

            message = starbelly_proto.ServerMessage.FromString(message_data)
            logger.debug('Received message: %.500s', message)
            message_type = message.WhichOneof('MessageType')

            if message_type == 'event':
                await self._process_event(message.event)
            elif message_type == 'response':
                await self._process_response(message.response)
            else:
                logger.error('Unknown message type "%s" in :\n%r',
                    message_type, message)

    async def _reconnect(self):
        ''' Reconnect to Starbelly. '''
        if self._reconnect_future is None:
            self._reconnect_future = asyncio.Future()
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            logger.info('Re-connecting to %s', url)
            self._socket = await websockets.connect(url,
                extra_headers=self._headers, ssl=ssl_context, max_queue=1)
            self._reconnect_future.set_result(None)
            self._reconnect_future = None
        else:
            await self._reconnect_future


class StarbellySubscription:
    ''' Encapsulates a single subscription into an iterator. '''

    def __init__(self, id_):
        ''' Constructor. '''
        self.id = id_
        logger = logging.getLogger('cli.subscription')
        self._cancelled = asyncio.Event()
        self._queue = asyncio.Queue(maxsize=1)

    def __aiter__(self):
        ''' This class is an async iterator. '''
        # TODO After python 3.5.2, this can be `return self`
        future = asyncio.Future()
        future.set_result(self)
        return future

    async def __anext__(self):
        ''' Get next event from subscription. '''
        queue_get = asyncio.ensure_future(self._queue.get())
        cancel_wait = asyncio.ensure_future(self._cancelled.wait())
        done = await wait_first(queue_get, cancel_wait)
        if queue_get is done:
            event = queue_get.result()
            return event
        else:
            raise StopAsyncIteration()

    def cancel(self):
        '''
        Cancel this subscription.

        This will cause the iterator to exit when the next event is requested.
        '''
        self._cancelled.set()

    async def put(self, event):
        '''
        Add an event to the subscription.

        Suspends if the subscription's queue is full.
        '''
        done = await wait_first(self._queue.put(event), self._cancelled.wait())
        done.result()
