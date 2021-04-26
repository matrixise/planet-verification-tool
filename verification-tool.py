import configparser
import asyncio

import prettyprinter
import httpx
import feedparser

class UnableToParseFeedError(Exception):
    pass

class Verificator:
    def __init__(self, urls, maxtasks=20):
        self.urls = urls
        self.todo = set()
        self.busy = set()
        self.done = {}
        self.failed = {}
        self.tasks = set()
        self.sem = asyncio.Semaphore(maxtasks)

    async def run(self):
        task = asyncio.ensure_future(self.fetch_rss_feeds(self.urls))
        await asyncio.sleep(1)
        while self.busy:
            await asyncio.sleep(1)
        await task

    async def fetch_rss_feeds(self, urls):
        loop = asyncio.get_running_loop()
        for url in urls:
            if (url not in self.todo
                and url not in self.busy
                and url not in self.done):
                self.todo.add(url)
                await self.sem.acquire()
                task = asyncio.ensure_future(self.process(loop, url))
                task.add_done_callback(lambda t: self.sem.release())
                task.add_done_callback(self.tasks.remove)
                self.tasks.add(task)

    async def process(self, loop, url):
        print(f"processing: {url}")
        self.todo.remove(url)
        self.busy.add(url)

        transport = httpx.AsyncHTTPTransport(retries=3)
        client = httpx.AsyncClient(
            verify=False,
            transport=transport,
            timeout=httpx.Timeout(10.0, connect=20.0)
        )
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                self.done[url] = False
                self.failed[url] = resp.status_code
            else:
                parsed_feed = await loop.run_in_executor(None, self._parse_feed, resp.text)
                self.done[url] = True
        except Exception as exc:
            self.done[url] = False
            self.failed[url] = { 'type': 'exception', 'message': str(exc), 'other': type(exc) }
        finally:
            await client.aclose()
        self.busy.remove(url)

    def _parse_feed(self, xml):
        parsed_feed = feedparser.parse(xml)
        if parsed_feed.bozo:
            raise UnableToParseFeedError('Unable to parse feed')
        return parsed_feed


async def amain(urls):
    verificator = Verificator(urls=urls)
    result = await verificator.run()
    return verificator.failed

def main():
    parser = configparser.ConfigParser()
    parser.read(['config.ini'])
    urls = set(parser.sections())
    urls.remove('Planet')

    result = asyncio.run(
        amain(
            urls=list(urls)
        )
    )
    prettyprinter.cpprint(result)
    for section in result:
        parser.remove_section(section)

    with open('config.modified.ini', 'w') as fp:
        parser.write(fp)

main()