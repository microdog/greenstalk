import asyncio
from typing import Any, Coroutine, Dict, Iterable, List, Optional, Union

__version__ = '1.0.1'

Body = Union[bytes, str]
Stats = Dict[str, Union[str, int]]

DEFAULT_TUBE = 'default'
DEFAULT_PRIORITY = 2 ** 16
DEFAULT_DELAY = 0
DEFAULT_TTR = 60


class Job:
    """A job returned from the server."""

    __slots__ = ('id', 'body', '__dict__')

    def __init__(self, id: int, body: Body) -> None:
        self.id = id
        self.body = body


JobOrID = Union[Job, int]


class Error(Exception):
    """Base class for non-connection related exceptions. Connection related
    issues use the built-in ``ConnectionError``.
    """


class UnknownResponseError(Error):
    """The server sent a response that this client does not understand."""

    def __init__(self, status: bytes, values: List[bytes]) -> None:
        self.status = status
        self.values = values


class BeanstalkdError(Error):
    """Base class for error messages returned from the server."""


class BadFormatError(BeanstalkdError):
    """The client sent a malformed command."""


class BuriedError(BeanstalkdError):
    """The server ran out of memory trying to grow the priority queue and had to
    bury the job.
    """

    def __init__(self, values: List[bytes] = None) -> None:
        if values:
            self.id = int(values[0])  # type: Optional[int]
        else:
            self.id = None


class DeadlineSoonError(BeanstalkdError):
    """The client has a reserved job timing out within the next second."""


class DrainingError(BeanstalkdError):
    """The client tried to insert a job while the server was in drain mode."""


class ExpectedCrlfError(BeanstalkdError):
    """The client sent a job body without a trailing CRLF."""


class InternalError(BeanstalkdError):
    """The server detected an internal error."""


class JobTooBigError(BeanstalkdError):
    """The client attempted to insert a job larger than ``max-job-size``."""


class NotFoundError(BeanstalkdError):
    """For the delete, release, bury, and kick commands, it means that the job
    does not exist or is not reserved by the client.

    For the peek commands, it means the requested job does not exist or that
    there are no jobs in the requested state.
    """


class NotIgnoredError(BeanstalkdError):
    """The client attempted to ignore the only tube on its watch list."""


class OutOfMemoryError(BeanstalkdError):
    """The server could not allocate enough memory for a job."""


class TimedOutError(BeanstalkdError):
    """A job could not be reserved within the specified timeout."""


class UnknownCommandError(BeanstalkdError):
    """The client sent a command that the server does not understand."""


ERROR_RESPONSES = {
    b'BAD_FORMAT':      BadFormatError,
    b'BURIED':          BuriedError,
    b'DEADLINE_SOON':   DeadlineSoonError,
    b'DRAINING':        DrainingError,
    b'EXPECTED_CRLF':   ExpectedCrlfError,
    b'INTERNAL_ERROR':  InternalError,
    b'JOB_TOO_BIG':     JobTooBigError,
    b'NOT_FOUND':       NotFoundError,
    b'NOT_IGNORED':     NotIgnoredError,
    b'OUT_OF_MEMORY':   OutOfMemoryError,
    b'TIMED_OUT':       TimedOutError,
    b'UNKNOWN_COMMAND': UnknownCommandError,
}


class Client:
    """A client implementing the beanstalk protocol. Upon creation a TCP
    connection with beanstalkd is established and tubes are initialized.

    :param host: The IP or hostname of the server.
    :param port: The port the server is running on.
    :param encoding: The encoding used to encode and decode job bodies.
    :param use: The tube to use after connecting.
    :param watch: The tubes to watch after connecting. The ``default`` tube will
                  be ignored if it's not included.
    """

    def __init__(
        self,
        host: str = '127.0.0.1',
        port: int = 11300,
        encoding: Optional[str] = 'utf-8',
        use: str = DEFAULT_TUBE,
        watch: Union[str, Iterable[str]] = DEFAULT_TUBE,
    ) -> None:
        self._host = host
        self._port = port
        self._use = use
        self._watch = watch

        self._reader = None  # type: Optional[asyncio.StreamReader]
        self._writer = None  # type: Optional[asyncio.StreamWriter]

        self.encoding = encoding

    async def __aenter__(self) -> 'Client':
        await self.open()
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.close()

    async def open(self) -> None:
        self._reader, self._writer = await asyncio.streams.open_connection(
            self._host, self._port
        )

        if self._use != DEFAULT_TUBE:
            await self.use(self._use)

        if isinstance(self._watch, str):
            if self._watch != DEFAULT_TUBE:
                await self.watch(self._watch)
                await self.ignore(DEFAULT_TUBE)
        else:
            for tube in self._watch:
                await self.watch(tube)
            if DEFAULT_TUBE not in self._watch:
                await self.ignore(DEFAULT_TUBE)

    def close(self) -> None:
        """Closes the TCP connection to beanstalkd. The client instance should
        not be used after calling this method."""
        self._writer.close()
        self._writer = None
        self._reader = None

    async def _send_cmd(self, cmd: bytes, expected: bytes) -> List[bytes]:
        self._writer.write(cmd + b'\r\n')
        line = await self._reader.readline()
        return _parse_response(line, expected)

    async def _read_chunk(self, size: int) -> bytes:
        data = await self._reader.read(size + 2)
        return _parse_chunk(data, size)

    async def _int_cmd(self, cmd: bytes, expected: bytes) -> int:
        n, = await self._send_cmd(cmd, expected)
        return int(n)

    async def _job_cmd(self, cmd: bytes, expected: bytes) -> Job:
        id, size = (int(n) for n in await self._send_cmd(cmd, expected))
        chunk = await self._read_chunk(size)
        if self.encoding is None:
            body = chunk  # type: Body
        else:
            body = chunk.decode(self.encoding)
        return Job(id, body)

    def _peek_cmd(self, cmd: bytes) -> Coroutine[Any, Any, Job]:
        return self._job_cmd(cmd, b'FOUND')

    async def _stats_cmd(self, cmd: bytes) -> Stats:
        size = await self._int_cmd(cmd, b'OK')
        chunk = await self._read_chunk(size)
        return _parse_simple_yaml(chunk)

    async def _list_cmd(self, cmd: bytes) -> List[str]:
        size = await self._int_cmd(cmd, b'OK')
        chunk = await self._read_chunk(size)
        return _parse_simple_yaml_list(chunk)

    def put(
        self,
        body: Body,
        priority: int = DEFAULT_PRIORITY,
        delay: int = DEFAULT_DELAY,
        ttr: int = DEFAULT_TTR,
    ) -> Coroutine[Any, Any, int]:
        """Inserts a job into the currently used tube and returns the job ID.

        :param body: The data representing the job.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: The number of seconds to delay the job for.
        :param ttr: The maximum number of seconds the job can be reserved for
                    before timing out.
        """
        if isinstance(body, str):
            if self.encoding is None:
                raise TypeError("Unable to encode string with no encoding set")
            body = body.encode(self.encoding)
        cmd = b'put %d %d %d %d\r\n%b' % (priority, delay, ttr, len(body), body)
        return self._int_cmd(cmd, b'INSERTED')

    async def use(self, tube: str) -> None:
        """Changes the currently used tube.

        :param tube: The tube to use.
        """
        await self._send_cmd(b'use %b' % tube.encode('ascii'), b'USING')

    def reserve(self, timeout: Optional[int] = None) -> Coroutine[Any, Any, Job]:
        """Reserves a job from a tube on the watch list, giving this client
        exclusive access to it for the TTR. Returns the reserved job.

        This blocks until a job is reserved unless a ``timeout`` is given,
        which will raise a :class:`TimedOutError <greenstalk.TimedOutError>` if
        a job cannot be reserved within that time.

        :param timeout: The maximum number of seconds to wait.
        """
        if timeout is None:
            cmd = b'reserve'
        else:
            cmd = b'reserve-with-timeout %d' % timeout
        return self._job_cmd(cmd, b'RESERVED')

    async def delete(self, job: JobOrID) -> None:
        """Deletes a job.

        :param job: The job or job ID to delete.
        """
        await self._send_cmd(b'delete %d' % _to_id(job), b'DELETED')

    async def release(
        self, job: Job, priority: int = DEFAULT_PRIORITY, delay: int = DEFAULT_DELAY
    ) -> None:
        """Releases a reserved job.

        :param job: The job to release.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        :param delay: The number of seconds to delay the job for.
        """
        await self._send_cmd(
            b'release %d %d %d' % (job.id, priority, delay), b'RELEASED'
        )

    async def bury(self, job: Job, priority: int = DEFAULT_PRIORITY) -> None:
        """Buries a reserved job.

        :param job: The job to bury.
        :param priority: An integer between 0 and 4,294,967,295 where 0 is the
                         most urgent.
        """
        await self._send_cmd(b'bury %d %d' % (job.id, priority), b'BURIED')

    async def touch(self, job: Job) -> None:
        """Refreshes the TTR of a reserved job.

        :param job: The job to touch.
        """
        await self._send_cmd(b'touch %d' % job.id, b'TOUCHED')

    def watch(self, tube: str) -> Coroutine[Any, Any, int]:
        """Adds a tube to the watch list. Returns the number of tubes this
        client is watching.

        :param tube: The tube to watch.
        """
        return self._int_cmd(b'watch %b' % tube.encode('ascii'), b'WATCHING')

    def ignore(self, tube: str) -> Coroutine[Any, Any, int]:
        """Removes a tube from the watch list. Returns the number of tubes this
        client is watching.

        :param tube: The tube to ignore.
        """
        return self._int_cmd(b'ignore %b' % tube.encode('ascii'), b'WATCHING')

    def peek(self, id: int) -> Coroutine[Any, Any, Job]:
        """Returns a job by ID.

        :param id: The ID of the job to peek.
        """
        return self._peek_cmd(b'peek %d' % id)

    def peek_ready(self) -> Coroutine[Any, Any, Job]:
        """Returns the next ready job in the currently used tube."""
        return self._peek_cmd(b'peek-ready')

    def peek_delayed(self) -> Coroutine[Any, Any, Job]:
        """Returns the next available delayed job in the currently used tube."""
        return self._peek_cmd(b'peek-delayed')

    def peek_buried(self) -> Coroutine[Any, Any, Job]:
        """Returns the oldest buried job in the currently used tube."""
        return self._peek_cmd(b'peek-buried')

    def kick(self, bound: int) -> Coroutine[Any, Any, int]:
        """Moves delayed and buried jobs into the ready queue and returns the
        number of jobs effected.

        Only jobs from the currently used tube are moved.

        A kick will only move jobs in a single state. If there are any buried
        jobs, only those will be moved. Otherwise delayed jobs will be moved.

        :param bound: The maximum number of jobs to kick.
        """
        return self._int_cmd(b'kick %d' % bound, b'KICKED')

    async def kick_job(self, job: JobOrID) -> None:
        """Moves a delayed or buried job into the ready queue.

        :param job: The job or job ID to kick.
        """
        await self._send_cmd(b'kick-job %d' % _to_id(job), b'KICKED')

    def stats_job(self, job: JobOrID) -> Coroutine[Any, Any, Stats]:
        """Returns job statistics.

        :param job: The job or job ID to return statistics for.
        """
        return self._stats_cmd(b'stats-job %d' % _to_id(job))

    def stats_tube(self, tube: str) -> Coroutine[Any, Any, Stats]:
        """Returns tube statistics.

        :param tube: The tube to return statistics for.
        """
        return self._stats_cmd(b'stats-tube %b' % tube.encode('ascii'))

    def stats(self) -> Coroutine[Any, Any, Stats]:
        """Returns system statistics."""
        return self._stats_cmd(b'stats')

    def tubes(self) -> Coroutine[Any, Any, List[str]]:
        """Returns a list of all existing tubes."""
        return self._list_cmd(b'list-tubes')

    async def using(self) -> str:
        """Returns the tube currently being used by the client."""
        tube, = await self._send_cmd(b'list-tube-used', b'USING')
        return tube.decode('ascii')

    def watching(self) -> Coroutine[Any, Any, List[str]]:
        """Returns a list of tubes currently being watched by the client."""
        return self._list_cmd(b'list-tubes-watched')

    async def pause_tube(self, tube: str, delay: int) -> None:
        """Prevents jobs from being reserved from a tube for a period of time.

        :param tube: The tube to pause.
        :param delay: The number of seconds to pause the tube for.
        """
        await self._send_cmd(
            b'pause-tube %b %d' % (tube.encode('ascii'), delay), b'PAUSED'
        )


def _to_id(j: JobOrID) -> int:
    return j.id if isinstance(j, Job) else j


def _parse_response(line: bytes, expected: bytes) -> List[bytes]:
    if not line:
        raise ConnectionError("Unexpected EOF")

    assert line[-2:] == b'\r\n'
    line = line[:-2]

    status, *values = line.split()

    if status == expected:
        return values

    if status in ERROR_RESPONSES:
        raise ERROR_RESPONSES[status](values)

    raise UnknownResponseError(status, values)


def _parse_chunk(data: bytes, size: int) -> bytes:
    assert data[-2:] == b'\r\n'
    data = data[:-2]

    if len(data) != size:
        raise ConnectionError("Unexpected EOF reading chunk")

    return data


def _parse_simple_yaml(buf: bytes) -> Stats:
    data = buf.decode('ascii')

    assert data[:4] == '---\n'
    data = data[4:]  # strip YAML head

    stats = {}
    for line in data.splitlines():
        key, value = line.split(': ', 1)
        try:
            v = int(value)  # type: Union[int, str]
        except ValueError:
            v = value
        stats[key] = v

    return stats


def _parse_simple_yaml_list(buf: bytes) -> List[str]:
    data = buf.decode('ascii')

    assert data[:4] == '---\n'
    data = data[4:]  # strip YAML head

    list = []
    for line in data.splitlines():
        assert line.startswith('- ')
        list.append(line[2:])

    return list
