#!/usr/bin/env python3

import os

import backoff
import json
import requests
import pendulum
import sys
import time

import singer
from singer import metadata
from singer import Transformer, utils

LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "refresh_token",
    "client_id",
    "client_secret",
    "user_agent",
]

ENDPOINTS = [
    "clients",
    "contacts",
    "expenses",
    "expense_categories",
    "invoices",
    # "invoice_messages",
    "invoice_item_categories",
    "estimate_item_categories",
    # "estimate_messages",
    "estimates",
    "projects",
    "user_assignments",
    "task_assignments",
    "roles",
    "tasks",
    "time_entries",
    "users"
]

PRIMARY_KEY = "id"
REPLICATION_KEY = 'updated_at'

BASE_API_URL = "https://api.harvestapp.com/v2/"
BASE_ID_URL = "https://id.getharvest.com/api/v2/"
CONFIG = {}
STATE = {}
AUTH = {}


class Auth:
    def __init__(self, client_id, client_secret, refresh_token, personal_token, account_id):
        self._client_id = client_id
        self._client_secret = client_secret
        self._personal_token = personal_token
        self._refresh_token = refresh_token
        self._account_id = account_id
        if self._personal_token is None:
            self._refresh_access_token()

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2)
    def _make_refresh_token_request(self):
        return requests.request('POST',
                                url=BASE_ID_URL + 'oauth2/token',
                                data={
                                    'client_id': self._client_id,
                                    'client_secret': self._client_secret,
                                    'refresh_token': self._refresh_token,
                                    'grant_type': 'refresh_token',
                                },
                                headers={"User-Agent": CONFIG.get("user_agent")})

    def _refresh_access_token(self):
        LOGGER.info("Refreshing access token")
        resp = self._make_refresh_token_request()
        expires_in_seconds = resp.json().get('expires_in', 17 * 60 * 60)
        self._expires_at = pendulum.now().add(seconds=expires_in_seconds)
        resp_json = {}
        try:
            resp_json = resp.json()
            self._access_token = resp_json['access_token']
        except KeyError as key_err:
            if resp_json.get('error'):
                LOGGER.critical(resp_json.get('error'))
            if resp_json.get('error_description'):
                LOGGER.critical(resp_json.get('error_description'))
            raise key_err
        LOGGER.info("Got refreshed access token")

    def get_access_token(self):
        if self._personal_token is not None:
            return self._personal_token

        if self._access_token is not None and self._expires_at > pendulum.now():
            return self._access_token

        self._refresh_access_token()
        return self._access_token

    def get_account_id(self):
        if self._account_id is not None:
            return self._account_id

        response = requests.request('GET',
                                    url=BASE_ID_URL + 'accounts',
                                    data={
                                        'access_token': self._access_token,
                                    },
                                    headers={"User-Agent": CONFIG.get("user_agent")})

        self._account_id = str(response.json()['accounts'][0]['id'])

        return self._account_id


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def load_and_write_schema(name, key_properties='id', bookmark_property='updated_at'):
    schema = load_schema(name)
    singer.write_schema(name, schema, key_properties, bookmark_properties=[bookmark_property])
    return schema


def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint):
    return BASE_API_URL + endpoint


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=5,
    giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
    factor=2)
@utils.ratelimit(100, 15)
def request(url, params=None):
    params = params or {}
    access_token = AUTH.get_access_token()
    headers = {"Accept": "application/json",
               "Harvest-Account-Id": AUTH.get_account_id(),
               "Authorization": "Bearer " + access_token,
               "User-Agent": CONFIG.get("user_agent")}
    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()
    return resp.json()


# Any date-times values can either be a string or a null.
# If null, parsing the date results in an error.
# Instead, removing the attribute before parsing ignores this error.
def remove_empty_date_times(item, schema):
    fields = []

    for key in schema['properties']:
        subschema = schema['properties'][key]
        if subschema.get('format') == 'date-time':
            fields.append(key)

    for field in fields:
        if item.get(field) is None:
            del item[field]

def get_stream_version(tap_stream_id):
    return int(time.time() * 1000)

def append_times_to_dates(item, date_fields):
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] = utils.strftime(utils.strptime_with_tz(item[date_field]))


def get_company():
    url = get_url('company')
    return request(url)


def sync_endpoint(stream, schema, mdata, date_fields=None):

    singer.write_schema(stream.tap_stream_id,
                        schema,
                        [PRIMARY_KEY],
                        bookmark_properties=[REPLICATION_KEY])

    start = get_start(stream.tap_stream_id)
    # Check if the stream includes the replication key
    if metadata.get(mdata, ('properties', REPLICATION_KEY), 'selected'):
        with_updated_since = True
        start_dt = pendulum.parse(start)
        updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        with_updated_since = False

    url = get_url(stream.tap_stream_id)

    stream_version = get_stream_version(stream.tap_stream_id)
    activate_version_message = singer.ActivateVersionMessage(
        stream=stream.stream,
        version=stream_version
    )

    with Transformer() as transformer:
        page = 1
        while page is not None:
        
            params = {"updated_since": updated_since} if with_updated_since else {}
            params['page'] = page
            response = request(url, params)
            data = response[stream.tap_stream_id]
            time_extracted = utils.now()

            for row in data:
                remove_empty_date_times(row, schema)

                item = transformer.transform(row, schema, mdata)

                # @TODO Not currently passing in date_fields values. Check mdata instead?
                append_times_to_dates(item, date_fields)

                if with_updated_since:
                    updated_at = item[REPLICATION_KEY]
                else:
                    updated_at = start

                if updated_at >= start:
                    new_record = singer.RecordMessage(
                        stream=stream.stream,
                        record=item,
                        version=stream_version,
                        time_extracted=time_extracted)
                    singer.write_message(new_record)

                    utils.update_state(STATE, stream.tap_stream_id, updated_at)

            page = response['next_page']

    singer.write_state(STATE)
    singer.write_message(activate_version_message)

def do_sync(catalog):
    LOGGER.info("Starting sync")

    company = get_company()
    
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        
        is_selected = metadata.get(mdata, (), 'selected')
        if is_selected:
            sync_endpoint(stream, stream.schema.to_dict(), mdata)

    LOGGER.info("Sync complete")

def do_discover():
    streams = []
    catalog = []

    for endpoint in ENDPOINTS:
        schema = load_schema(endpoint)

        mdata = metadata.get_standard_metadata(schema,
            endpoint,
            [PRIMARY_KEY],
            [REPLICATION_KEY])

        mdata = metadata.to_list(metadata.write(metadata.to_map(mdata), (), 'selected', True))

        catalog_entry = {
            'stream' : endpoint,
            'tap_stream_id' : endpoint,
            'schema' : schema,
            'metadata' : mdata
        }

        streams.append(catalog_entry)

    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    # @TODO Check for either OAuth config OR Personal Token
    CONFIG.update(args.config)
    global AUTH  # pylint: disable=global-statement
    AUTH = Auth(CONFIG['client_id'], CONFIG['client_secret'], CONFIG['refresh_token'], CONFIG['personal_token'], CONFIG['account_id'])
    STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.catalog:
        do_sync(args.catalog)

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
