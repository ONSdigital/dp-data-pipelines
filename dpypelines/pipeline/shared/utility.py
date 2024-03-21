import os

def enrich_online(message_creating_function):
    def wrapper(*args, **kwargs):
        msg: str = message_creating_function(*args, **kwargs)
        
        #Get the enviormentl variable
        enrich_message = os.environ.get('ENRICH_OUTGOING_MESSAGES', None)
        #If there is a value stored add (enrich) message otherwise return original message
        if enrich_message is not None:
            return msg + "/n" + enrich_message
        else:
             return msg
        
    return wrapper