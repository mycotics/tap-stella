import singer

from .client import Client

LOGGER = singer.get_logger()


def sync_qa(client, stream, state):
    bookmark_column = stream.replication_key
    is_sorted = False  # TODO: indicate whether data is sorted ascending on bookmark value

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )

    max_bookmark = None
    for new_bookmark, rows in client.paging_get('v2/qa', after=state.get('qa')):
        for row in rows:
            if row['score']:
                row['score'] = float(row['score'].strip('%'))/100
        # write one or more rows to the stream:
        singer.write_records(stream.tap_stream_id, rows)
        singer.write_state({stream.tap_stream_id: new_bookmark})


def sync(config, state, catalog):
    """ Sync data from tap source """
    client = Client(config)
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        if stream.tap_stream_id == 'qa':
            sync_qa(client, stream, state)
