import json
from sseclient import SSEClient as EventSource
from datetime import datetime, timedelta

past_datetime = datetime.now() - timedelta(weeks=2)
since_timestamp = int(past_datetime.timestamp()*1000)
url = f'https://stream.wikimedia.org/v2/stream/recentchange?since={since_timestamp}'

# url = 'https://stream.wikimedia.org/v2/stream/recentchange'
wiki = 'enwiki' #Client side filter
counter = 0
maxEvents = 30 # print n events and stop
users = {}
for event in EventSource(url):
    if event.event == 'message':
        try:
            change = json.loads(event.data)
        except ValueError:
            continue

        if change.get('type') == None or change.get('type') != 'edit':
            continue
    
        if change['wiki'] == wiki and change['bot'] == False:
            print(change)
            counter += 1
            if counter > maxEvents:
                break