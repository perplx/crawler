#!/usr/bin/env python

"""Basic Web-Crawler using Python async.
see: https://youtu.be/ftmdDlwMwwQ
"""


# standard imports
import asyncio
import html.parser
import pathlib
import time
import urllib.parse
from typing import Callable, Iterable

# library imports
import httpx


class UrlFilterer:
    """Filter URLs for crawling."""

    def __init__(
            self,
            allowed_domains: set[str] | None = None,
            allowed_schemes: set[str] | None = None,
            allowed_filetypes: set[str] | None = None,
    ):
        self.allowed_domains = allowed_domains
        self.allowed_schemes = allowed_schemes
        self.allowed_filetypes = allowed_filetypes

    def filter_url(self, base: str, url: str) -> str | None:
        # parse the URL
        url = urllib.parse.urljoin(base, url)
        url, _frag = urllib.parse.urldefrag(url)
        parsed = urllib.parse.urlparse(url)

        # filter URL based on scheme
        if self.allowed_schemes is not None and parsed.scheme not in self.allowed_schemes:
            return None
        
        # filter URL based on domain
        if self.allowed_domains is not None and parsed.netloc not in self.allowed_domains:
            return None
        
        # filter URL based on file-extension
        ext = pathlib.Path(parsed.path).suffix
        if self.allowed_filetypes is not None and ext not in self.allowed_filetypes:
            return None
        
        return url  # keep this one


class UrlParser(html.parser.HTMLParser):
    """Parse HTML page to extract the URLKs in `<a>` tags."""

    def __init__(
            self,
            base: str,
            filter_url: Callable[[str, str], str | None]
    ):
        super().__init__()
        self.base = base
        self.filter_url = filter_url
        self.found_links = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            for attr, value in attrs:
                if attr == "href":
                    if (url := self.filter_url(self.base, value)) is not None:
                        self.found_links.add(url)


class Crawler:
    def __init__(
            self,
            client: httpx.AsyncClient,
            urls: Iterable[str],
            filter_url: Callable[[str, str], str | None],
            workers: int = 10,
            limit: int = 25,
    ):
        self.client = client

        self.start_urls = set(urls)
        self.todo = asyncio.Queue()
        self.seen = set()
        self.done = set()
    
        self.filter_url = filter_url
        self.num_workers = workers
        self.limit = limit
        self.total = 0

    async def run(self):
        # create all the workers and wait for them to be done
        await self.on_found_links(self.start_urls)  # prime the queue
        workers = [asyncio.create_task(self.worker()) for _ in range(self.num_workers)]
        await self.todo.join()

        # once all the work is done, cancel all the workers
        for worker in workers:
            worker.cancel()

    async def worker(self):
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                return

    async def process_one(self):
        """Process a single URL from the queue."""

        # get a task to do
        url = await self.todo.get()

        try:
            # do the work
            await self.crawl(url)
        except Exception as e:
            # TODO retry handling:
            pass
        finally:
            # mark task as complete
            self.todo.task_done()

    async def crawl(self, url: str):
        # rate limiting
        await asyncio.sleep(1.1)

        # retrieve the page at the given URL
        response = await self.client.get(url, follow_redirects=True)

        # extract all the URLs from the links in the given page
        found_links = await self.parse_links(
            base=str(response.url),
            text=response.text,
        )

        # add found URLs to the queue
        await self.on_found_links(found_links)

        self.done.add(url)

    async def parse_links(self, base: str, text: str) -> set[str]:
        """Extract the URLs from the links in the given page text."""

        parser = UrlParser(base, self.filter_url)
        parser.feed(text)
        return parser.found_links

    async def on_found_links(self, urls: set[str]):
        """When new URLs are found on a crawled page, add them to hte queue."""

        # get the new URLs, which haven't been seen yet
        new = urls - self.seen
        self.seen.update(new)

        # await save to db or file here

        for url in new:
            await self.put_todo(url)

    async def put_todo(self, url: str):
        """enqueue a new URL."""
        if self.total >= self.limit:
            return
        self.total += 1
        await self.todo.put(url)


async def main():
    # select which URLs will be crawled
    filterer = UrlFilterer(
        allowed_domains={"mcoding.io"},
        allowed_schemes={"http", "https"},
        allowed_filetypes={".html", ".php", ""},
    )

    # crawl from the given URLs
    start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        crawler = Crawler(
            client=client,
            urls=["https://mcoding.io"],
            filter_url=filterer.filter_url,
            workers=5,
            limit=30,
        )
        await crawler.run()
    end = time.perf_counter()

    # show statistics about the crawl
    seen = sorted(crawler.seen)
    print("results:")
    for url in seen:
        print(f"url: {url}")
    print(f"crawled: {len(crawler.done)} URLs")
    print(f"found: {len(seen)} URLs")
    print(f"Done in {end - start:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main(), debug=True)