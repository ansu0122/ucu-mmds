import json
import csv
from sseclient import SSEClient as EventSource
from tqdm import tqdm


url = 'https://stream.wikimedia.org/v2/stream/recentchange'

def is_relevant_event(event, wiki='enwiki'):
    """
    Checks if the event is relevant for sampling, i.e., it is an edit made by a bot on the specified wiki.

    Args:
    - event (dict): The event data.
    - wiki (str): The wiki to filter for (default is 'enwiki').

    Returns:
    - bool: True if the event is an edit by a bot in the specified wiki, False otherwise.
    """
    return event.get('type') == 'edit' and event.get('bot') == True and event.get('wiki') == wiki

def should_sample_event(event, threshold=0.2):
    """
    Determines if an event should be sampled based on a simple hash function.

    Args:
    - event (dict): The event data containing fields for sampling.
    - threshold (float): Probability threshold for sampling (0.2 for 20%).

    Returns:
    - bool: True if the event should be sampled, False otherwise.
    """
    unique_string = f"{event['namespace']}_{event['timestamp']}_{event['revision']['new']}"
    event_hash_val = abs(hash(unique_string)) % 100 / 100
    return event_hash_val < threshold

def write_event_to_csv(writer, event):
    """
    Writes a single event to the CSV file.

    Args:
    - writer (csv.DictWriter): The CSV writer object.
    - event (dict): The event data to be written.
    """
    row = {
        'user': event['user'],
        'timestamp': event['timestamp'],
        'namespace': event['namespace'],
        'title': event['title'],
        'comment': event.get('comment', ''),
        'length_old': event['length']['old'],
        'length_new': event['length']['new'],
        'revision_old': event['revision']['old'],
        'revision_new': event['revision']['new'],
    }
    writer.writerow(row)

def subscribe_to_stream(url, wiki='enwiki'):
    """
    Subscribes to the Wikimedia recent changes stream and yields relevant events.

    Args:
    - url (str): The URL of the Wikimedia recent changes stream.
    - wiki (str): The wiki to filter for (default is 'enwiki').

    Yields:
    - dict: A dictionary representing a relevant bot edit event.
    """
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                continue

            if is_relevant_event(change, wiki=wiki):
                yield change

def sample_to_csv(events, output_file='sampled.csv', threshold=0.2, sample_limit=40000):
    """
    Samples events and writes them to a CSV file up to a specified limit.

    Args:
    - events (iterable): An iterable of events to be sampled.
    - output_file (str): The filename for the output CSV file.
    - threshold (float): Probability threshold for sampling (0.2 for 20%).
    - sample_limit (int): The maximum number of samples to collect.
    """
    csv_fields = ['user', 'timestamp', 'namespace', 'title', 'comment', 'length_old', 'length_new', 'revision_old', 'revision_new']
    
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fields)
        writer.writeheader()

        sample_count = 0
        with tqdm(total=sample_limit, desc="Sampling bot edits", unit="sample") as prog_bar:
            for event in events:
                if sample_count >= sample_limit:
                    print(f"Sample limit reached: {sample_limit} entries written to {output_file}")
                    break

                # Check if the event should be sampled
                if should_sample_event(event, threshold):
                    write_event_to_csv(writer, event)
                    sample_count += 1
                    prog_bar.update(1)

    print(f"Sampling complete. {sample_count} bot edits written to {output_file}")

if __name__ == '__main__':
    events = subscribe_to_stream(url)
    sample_to_csv(events, output_file='src/homework2/sampled_bots.csv', threshold=0.2, sample_limit=40000)
