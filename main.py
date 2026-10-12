
import sys
import requests
from urllib.parse import urlsplit, urljoin, urlparse
from crawl import normalize_url, extract_page_data
import asyncio
import aiohttp
import json
from json_report import write_json_report

class AsyncCrawler():
    def __init__(self, base_url, max_concurrency, max_pages):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        self.page_data = {}
        self.visited = set()
        self.lock = asyncio.Lock()
        self.max_concurrency = max_concurrency
        self.session = None
        self.should_stop = False
        self.all_tasks = set()
        self.max_pages = max_pages

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def add_page_visit(self, normalized_url):
        async with self.lock:
            if self.should_stop == True:
                return False
            elif normalized_url in self.visited:
                return False
            elif len(self.visited) >= self.max_pages:
                print("Reached maximum number of pages to crawl")
                self.should_stop = True
                return False
            else:
                self.visited.add(normalized_url)
                return True

    async def get_html(self, url):
        async with self.session.get(url, headers={"User-Agent": "BootCrawler/1.0"}) as response:
            if response.status >= 400:
                raise Exception("There was an error with the request")
            if "text/html" not in response.headers.get("Content-Type", ""):
                print(response.headers.get("Content-Type"))
                raise Exception("Content type should be html")
            return await response.text()

    async def crawl_page(self, current_url=None):
        if self.should_stop == True:
            return []

        if current_url == None:
            current_url = self.base_url

        if self.page_data == None:
            self.page_data = {}

        base_url_hostname = urlsplit(self.base_url).hostname
        current_url_hostname = urlsplit(current_url).hostname
        if base_url_hostname != current_url_hostname:
            return []

        current_url_normalized = normalize_url(current_url)
        response_new = await self.add_page_visit(current_url_normalized)
        if response_new == False:
            return []

        if self.page_data.get(current_url_normalized, "") != "":
            return []

        print(f"Crawling data from {current_url_normalized}...")
        try:
            print(f"FETCHING {current_url}")
            html = await self.get_html(current_url)
            print(f"FETCHED {current_url}")
        except Exception as e:
            print("Nothing returned. Exiting")
            return []

        page_info = extract_page_data(html, current_url)
        async with self.lock:
            self.page_data[current_url_normalized] = page_info

        outgoing_links: list = self.page_data[current_url_normalized]["outgoing_links"]
        return outgoing_links

    async def worker(self, queue):
        """Worker en segundo plano que procesa elementos de la cola."""
        while not self.should_stop:
            try:
                current_url = await asyncio.wait_for(queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                break

            outgoing_links = await self.crawl_page(current_url)
            
            if outgoing_links:
                for link in outgoing_links:
                    if self.should_stop or len(self.visited) >= self.max_pages:
                        break
                    try:
                        normalized_link = normalize_url(link)
                    except (TypeError, ValueError):
                        continue

                    if normalized_link not in self.visited:
                        print(f"Starting crawl of site {link}")
                        await queue.put(link)

            queue.task_done()

    async def crawl(self):
        queue = asyncio.Queue()
        await queue.put(self.base_url)

        # Se crean exactamente 'max_concurrency' workers y se almacenan en self.all_tasks
        for _ in range(self.max_concurrency):
            task = asyncio.create_task(self.worker(queue))
            self.all_tasks.add(task)

        # Esperar a que terminen todos los trabajadores
        await asyncio.gather(*self.all_tasks, return_exceptions=True)
        return self.page_data


async def crawl_site_async(base_url, max_concurrency, max_pages):
    async with AsyncCrawler(base_url, max_concurrency, max_pages) as crawler_max:
        final_page = await crawler_max.crawl()
    return final_page


async def main():
    if len(sys.argv) < 4:
        print("not enough data provided")
        sys.exit(1)
    elif len(sys.argv) > 4:
        print("too many arguments provided")
        sys.exit(1)
    else:
        print(f"starting crawl of: {sys.argv[1]}")
        page_dict = await crawl_site_async(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
        print(page_dict.values())
        write_json_report(page_dict)


if __name__ == "__main__":
    asyncio.run(main())