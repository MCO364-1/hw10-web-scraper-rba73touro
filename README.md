# hw10-web-scraper-rba73touro
hw10-web-scraper-rba73touro created by GitHub Classroom

WebScraperCode
pseudo-code

instantiate WebScrape object containing:
- The global Set which will contain all of the unique hyperlinks
- The global Set which will contain all of the unique emails
- The global Queue of hyperlinks to visit that will be shared amongst threads
- a lock email upload
- another lock object
add a url to the queue
create a thread that seeds the queue with 200 url's
create an executor that will asynchronously launch all the threads
a while loop to delay the main thread while the other threads are crawling/scraping

each thread will:
-have a reference to the global object
-have a local set containing scraped emails
-have a local set containing scraped links

-pop a url from the queue
get the document (otherwise catch the error)
get all emails from page
--if the emails are a sizable amount, it will then transfer them to the global email set
get all hyperlinks from page
--ensure that only unchecked hyperlinks are added to the queue by referencing the indexed hyperlink set
check the global email set to see if the target email amount was reached; 
if it was, that thread will update the table in the database and then exit


