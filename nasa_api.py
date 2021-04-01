import asyncio
import aiohttp
import urllib.parse
from aiolimiter import AsyncLimiter


class ResultIterator:
    """
    An asynchonous iterator that allows a consumer to iterate over search results, blocking
    when no more results are ready
    """

    def __init__(self, queue, total):
        self.queue = queue
        self.total = total

    def __len__(self):
        return self.total

    def __aiter__(self):
        return self

    async def __anext__(self):
        r = await self.queue.get()
        if r is None:
            raise StopAsyncIteration
        return r


class NASAApi:
    """
    Class for interacting with the NASA images API. Works as an async context manager via the __aenter__
    and __aexit__ methods; it will keep an HTTP session open as long as it is inside an `async with:` block.
    """

    BASE_URL = "https://images-api.nasa.gov"

    def __init__(self, concurrency_limit, requests_per_second):
        """
        :param concurrency_limit: number of connections allowed to be open at once
        :param requests_per_second: maximum average requests per second
        """
        self.concurrency_limit = concurrency_limit
        self.requests_per_second = requests_per_second
        self.rate_limiter = AsyncLimiter(
            self.concurrency_limit, self.concurrency_limit / self.requests_per_second
        )
        self.session = None
        self._entered_count = 0

    def open(self):
        """Open an HTTP session"""
        self.session = aiohttp.ClientSession()

    async def __aenter__(self):
        self.open()
        self._entered_count += 1
        return self

    async def __aexit__(self, *_, **__):
        self._entered_count -= 1
        if self._entered_count == 0:
            return await self.close()

    async def close(self):
        """Closes the HTTP session"""
        return await self.session.close()

    async def search(self, **kwargs):
        """
        Performs a NASA Image API search. **kwargs are passed directly as part of
        the URL query string. See the NASA Image API documentation for details.
        """
        queue = asyncio.Queue()
        asyncio.create_task(self._get_pages(queue, **kwargs))
        # block until the first item in the queue is ready, which will be the total number of results
        total = await queue.get()
        return ResultIterator(queue, total)

    async def _get_pages(self, queue, **kwargs):
        next_page = f"{self.BASE_URL}/search?{urllib.parse.urlencode(kwargs)}"
        tasks = set()
        first = True
        while next_page:
            async with self.rate_limiter:
                async with self.session.get(next_page) as resp:
                    resp.raise_for_status()  # raise an error if result is not 200 OK

                    data = (await resp.json())["collection"]

                    # get URL for next page, or None if this is the last page
                    next_page = next(
                        (url["href"] for url in data["links"] if url["rel"] == "next"),
                        None,
                    )

                    # if this is the first page, put the total number of results as the first item in the queue
                    if first:
                        await queue.put(data["metadata"]["total_hits"])
                        first = False

                    # enqueue a task to fetch the full metadata for each item
                    for item in data["items"]:
                        tasks.add(asyncio.create_task(self._get_item(queue, item)))

        # wait for all item fetching tasks to finish
        await asyncio.gather(*tasks)
        # indicate that there are no more results
        await queue.put(None)

    async def _get_item(self, queue, item):
        result = dict(item["data"][0])

        # fetch full metadata
        async with self.session.get(item["href"]) as resp:
            resp.raise_for_status()  # raise an error if result is not 200 OK
            data = await resp.json()

        # the metadata is already in a reasonable dictionary format, just need to specially parse the image URLs
        # and key them by size
        result["image_urls"] = {}
        for key in ["orig", "large", "medium", "small", "thumb"]:
            url = next((u for u in data if key in u), None)
            if url:
                result["image_urls"][key] = url
        await queue.put(result)


CONCURRENCY_LIMIT = 5
REQUESTS_PER_SECOND = 5
# use one global instance so that the rate limits are shared
NASA_API = NASAApi(
    concurrency_limit=CONCURRENCY_LIMIT, requests_per_second=REQUESTS_PER_SECOND
)
