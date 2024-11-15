import argparse
import json
from csv import DictWriter
from sseclient import SSEClient as EventSource
from tqdm import tqdm
from datetime import datetime, timedelta
from typing import Generator, Dict

past_datetime = datetime.now() - timedelta(weeks=2)
since_timestamp = int(past_datetime.timestamp() * 1000)
URL = f'https://stream.wikimedia.org/v2/stream/recentchange?since={since_timestamp}'


def is_relevant_event(event: Dict, wiki='enwiki') -> bool:
    """
    Checks if the event is relevant for sampling, i.e., it is an edit made by a bot on the specified wiki topic.

    Args:
    - event (dict): The event data.
    - wiki (str): The wiki to filter for (default is 'enwiki').

    Returns:
    - bool: True if the event is an edit by a bot in the specified wiki, False otherwise.
    """
    return  event.get('type') != None and event.get('type') == 'edit' and event.get('wiki') == wiki


def should_sample_event(event: Dict, sample_by, threshold=0.2) -> bool:
    """
    Determines if an event should be sampled based on threshold.

    Args:
    - event (dict): The event data containing fields for sampling.
    - sample_by (str): The event field we sample by.
    - threshold (float): Probability threshold for sampling (default: 0.2 for 20%).

    Returns:
    - bool: True if the event should be sampled, False otherwise.
    """
    sample_by_value = event.get(sample_by, "")
    if sample_by_value == "":
        return False

    time_hash_val = abs(hash(sample_by_value)) % 100 / 100
    return time_hash_val < threshold


def write_event_to_csv(writer: DictWriter, event):
    """
    Writes a single event to the CSV file.

    Args:
    - writer (csv.DictWriter): The CSV writer object.
    - event (dict): The event data to be written.
    """
    row = {
        'bot': event['bot'],
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


def subscribe_to_stream(url: str, wiki: str='enwiki') -> Generator[Dict, None, None]:
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


def sample_to_csv(events: Generator, output_file: str, sample_by: str, threshold: float=0.2, sample_limit: int=40000):
    """
    Samples events and writes them to a CSV file up to a specified limit.

    Args:
    - events (iterable): An iterable of events to be sampled.
    - output_file (str): The filename for the output CSV file.
    - interval_size (int): The size of the time interval in seconds (60 seconds by default).
    - threshold (float): Probability threshold for sampling (0.2 for 20%).
    - sample_limit (int): The maximum number of samples to collect.
    """
    csv_fields = ['bot', 'user', 'timestamp', 'namespace', 'title', 'comment', 'length_old', 'length_new', 'revision_old', 'revision_new']
    
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, fieldnames=csv_fields)
        writer.writeheader()

        sample_count = 0
        with tqdm(total=sample_limit, desc=f"Sampling {threshold*100:.0f}%", unit="sample", miniters=1) as prog_bar:
            for event in events:
                if sample_count >= sample_limit:
                    break

                if should_sample_event(event, sample_by, threshold):
                    write_event_to_csv(writer, event)
                    sample_count += 1
                    prog_bar.update(1)

    print(f"Sampling complete. {sample_count} edits written to {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Sample Wikimedia events and write to a CSV file.")
    parser.add_argument(
        '--url',
        type=str,
        default=URL,
        help="URL of the Wikimedia recent changes stream."
    )
    parser.add_argument(
        '--wiki',
        type=str,
        default='enwiki',
        help="Wiki topic to subscribe to (default: 'enwiki')."
    )
    parser.add_argument(
        '--output_file',
        type=str,
        default='sampled_events.csv',
        help="Path to the output CSV file."
    )
    parser.add_argument(
        '--sample_by',
        type=str,
        default='id',
        help="Field to use for sampling (default: 'timestamp')."
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.2,
        help="Sampling probability threshold (default: 0.2)."
    )
    parser.add_argument(
        '--sample_limit',
        type=int,
        default=40000,
        help="Maximum number of samples to collect (default: 40,000)."
    )
    
    args = parser.parse_args()
    events = subscribe_to_stream(args.url, wiki=args.wiki)
    sample_to_csv(
        events=events,
        output_file=args.output_file,
        sample_by=args.sample_by,
        threshold=args.threshold,
        sample_limit=args.sample_limit
    )

