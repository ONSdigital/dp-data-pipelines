
from cmath import exp
from logging import exception
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse
from requests import Response # check im correct about this import path
from dpytools.logging.logger import DpLogger

logger = DpLogger("data-ingress-pipeline")

def get_scheme(url: str) -> str:

    index = url.find('/')
    scheme = url[0:index-1]
    return scheme

def get_domain(url: str) -> str:
    """This funciton will return the domain name from the provided url."""
    #Parsing url to extract the domain name
    parsed_url = urlparse(url)

    #Getting the domain name
    domain = parsed_url.netloc.split(':')[0]
    return domain

def get_port(url: str) ->int:
    """This funciton will return the port number form the provided url."""
    #Parsing url
    parsed_url = urlparse(url)

    #checking if the port was give if not scheking scheme for port number
    if parsed_url.port is None:
        if parsed_url.scheme == "http":
            return 80
        else:
            return 443
    else:
        return parsed_url.port

def start_date(date: str) ->str:
    
    strp_time = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S GMT')
    
    return strp_time.isoformat() + "Z"

def get_end_date(time_delta: timedelta, date: str) ->str:

    strp_time = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S GMT')
    td = timedelta(microseconds=time_delta.microseconds)
    
    end_date = strp_time + td

    return end_date.isoformat() + "Z"

def calculate_duration_in_nanoseconds(time_delta: timedelta, date: str)->int:

    strp_time = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S GMT')
    td = timedelta(microseconds=time_delta.microseconds)
    
    end_date = strp_time + td

    duration = end_date - strp_time
    
    return duration.microseconds * 1000 

def get_content_length(res: Response)->int:
    try:
        content_length = res.headers["Content-Length"]
        return int(content_length)
    except KeyError:
        return 0
    
    


r: Response = requests.get("https://www.google.com")

r_dict = {"method": r.request.method, "scheme": get_scheme(r.url), "host": get_domain(r.url), "port": get_port(r.url), "path": r.request.path_url, "status_code" : r.status_code,"started_at":start_date(r.headers["Date"]), "ended_at":get_end_date(r.elapsed, r.headers["Date"]), "duration": calculate_duration_in_nanoseconds(r.elapsed, r.headers["Date"]), "response_content_length":get_content_length(r)}
# so given theres a requests.Response object, you could potentially have a pattern of say

logger.info("Some log message involving https", response=r)


