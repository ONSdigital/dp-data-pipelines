import os

def enrich_online(message_creating_function):
    def wrapper(*args):
        msg: str = message_creating_function(*args)
        
        #Get the enviormentl variable
        enrich_message = os.environ.get('ENRICH_OUTGOING_MESSAGES')
        #If there is a value stored add (enrich) message otherwise return original message
        if enrich_message is not None:
            return msg + enrich_message + "\n notification-source: ENRICH_OUTGOING_MESSAGES"
        else:
             return msg
        
    return wrapper