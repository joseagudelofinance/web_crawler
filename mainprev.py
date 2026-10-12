import sys
import requests
from urllib.parse import urlsplit, urljoin, urlparse
from crawl import normalize_url, extract_page_data
import asyncio
import aiohttp

class AsyncCrawler():
    def __init__(self, base_url, max_concurrency, max_pages):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        self.page_data = {}
        self.visited = set()
        self.lock = asyncio.Lock()
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
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
        async with self.session.get(url, headers={"User-Agent":"BootCrawler/1.0"}) as response:
            if response.status >= 400:
                raise Exception("There was an error with the request")
                return

            if "text/html" not in response.headers["Content-Type"]:
                print(response.headers["Content-Type"])
                raise Exception("Content type should be html")
                return

            return await response.text()
        
    async def crawl_page(self, current_url=None):
        print(f"ENTER crawl_page: {current_url}, all_tasks={len(self.all_tasks)}")

        my_own_ticket = asyncio.current_task()
        if self.should_stop == True:
            return
        try:

            if current_url == None:
                current_url = self.base_url
            if self.page_data == None:
                self.page_data = {}

            base_url_hostname = urlsplit(self.base_url).hostname
            current_url_hostname = urlsplit(current_url).hostname

            if base_url_hostname != current_url_hostname:
                return
            current_url_normalized = normalize_url(current_url)

            response_new = await self.add_page_visit(current_url_normalized)
            if response_new == False:
                return
            
            async with self.semaphore:
                if self.page_data.get(current_url_normalized,"") != "":
                    return
                print(f"Crawling data from {current_url_normalized}...")

                try:
                    print(f"FETCHING {current_url}")
                    html = await self.get_html(current_url)
                    print(f"FETCHED {current_url}")
                except Exception as e:
                    print("Nothing returned. Exiting")
                    return
                
                page_info = extract_page_data(html, current_url)

                async with self.lock:

                    self.page_data[current_url_normalized] = page_info
                
            outgoing_links:list = self.page_data[current_url_normalized]["outgoing_links"]

            for link in outgoing_links:
                if self.should_stop or len(self.visited) >= self.max_pages:
                    break
                try:
                    normalized_link = normalize_url(link)
                except (TypeError, ValueError):
                    continue
                if normalized_link in self.visited:
                    continue
                print(f"Starting crawl of site {link}")
                self.all_tasks.add(asyncio.create_task(self.crawl_page(link)))

             
        finally:
            self.all_tasks.remove(my_own_ticket)
    
                


    async def crawl(self):
        self.all_tasks.add(asyncio.create_task(self.crawl_page(self.base_url)))
        while len(self.all_tasks) > 0:
            all_tasks_subset = self.all_tasks.copy()
            await asyncio.gather(*all_tasks_subset)
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
        page_dict = await crawl_site_async(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))
        print(page_dict.values())
        

if __name__ == "__main__":
    asyncio.run(main())
