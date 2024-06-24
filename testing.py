import json
import requests
from requests import Response # check im correct about this import path
from dpytools.logging.logger import DpLogger

logger = DpLogger("data-ingress-pipeline")

def get_scheme(url: str) -> str:

    index = url.find('/')
    scheme = url[0:index-1]
    return scheme

def get_host(response: Response) ->str:
    json_form = response.json()

    host = response.headers["Host"]
    return host


r: Response = requests.get("https://www.google.com")

r_dict = {"method": r.request.method, "scheme": get_scheme(r.url), "host": get_host(r), "port": 443, "path": r.request.path_url, "status_code" : r.status_code,"started_at": r.headers["Date"]}
# so given theres a requests.Response object, you could potentially have a pattern of say

logger.info("Some log message involving https", response=r)


