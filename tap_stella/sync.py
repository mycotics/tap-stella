import singer
import datetime

from .client import Client
from tap_stella.util import get_logger

LOGGER = get_logger()


def deep_get(d: dict, key_path: tuple):
    key_path = key_path.split('.')
    value = d
    for key in key_path:
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return None
    return value


def sync_qa(client, stream, state, config) -> dict:
    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )
    new_bookmark = state.get('qa')
    # after indicates the 'sequence_id' and not a timestamp

    kwargs = {key: config[key] for key in ('created_at_gte', 'created_at_lte') if config.get(key)}

    for new_bookmark, rows in client.paging_get('v2/qa', after=state.get('qa'), **kwargs):
        for row in rows:
            if row.get('score'):
                row['score'] = float(row['score'].strip('%'))/100

            if isinstance(deep_get(row, 'scorecard.archived_at'), dict):
                # The schema expects a string here for this field, but we've
                # been getting json serializations of the ruby on rails type
                # ActiveSupport::TimeWithZone. This converts to string.
                archived_at = deep_get(row, 'scorecard.archived_at.utc.^t')
                if archived_at:
                    archived_at = datetime.datetime.fromtimestamp(
                        archived_at, datetime.timezone.utc
                    ).isoformat()
                row['scorecard']['archived_at'] = archived_at

        # write one or more rows to the stream:
        singer.write_records(stream.tap_stream_id, rows)
    return {stream.tap_stream_id: new_bookmark}

def sync_feedback(client, stream, state, config) -> dict:
    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )
    # after indicates the 'sequence_id' and not a timestamp
    new_bookmark = state.get('feedback')

    kwargs = {key: config[key] for key in ('created_at_gte', 'created_at_lte') if config.get(key)}

    for new_bookmark, rows in client.paging_get('v2/data', after=state.get('feedback'), **kwargs):
        # write one or more rows to the stream:
        singer.write_records(stream.tap_stream_id, rows)
    return {stream.tap_stream_id: new_bookmark}

def sync(config, state, catalog):
    """ Sync data from tap source """
    client = Client(config)
    # Loop over selected streams in catalog
    new_state = {}
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        if stream.tap_stream_id == 'qa':
            qa_state = sync_qa(client, stream, state, config)
            new_state.update(qa_state)
        if stream.tap_stream_id == 'feedback':
            feedback_state = sync_feedback(client, stream, state, config)
            new_state.update(feedback_state)

    if new_state:
        singer.write_state(new_state)
    else:
        LOGGER.warning("State is empty after syncing, not writing state")
