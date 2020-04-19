import models

import dateutil.parser
import dateutil.tz
from requests_oauthlib import OAuth2Session

from datetime import datetime
import json
import logging
import os
import os.path
import time

# The file where we will save the token
TOKEN_FILE = 'token.json'
# The URL to use to fetch and to renew the token
TOKEN_URL = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
# The scopes we need for our app
SCOPES = ['User.Read', 'offline_access', 'Files.Read', 'Files.Read.All',
          'Files.ReadWrite', 'Files.ReadWrite.All']
# The base URL for OneDrive requests
DRIVE_URL = 'https://graph.microsoft.com/v1.0/me/drive/'

logger = logging.getLogger(__name__)


def date_from_onedrive(datestring):
    return dateutil.parser.parse(datestring)


def date_to_onedrive(dt):
    return dt.astimezone(dateutil.tz.UTC).strftime('%Y-%m-%dT%H:%m:%S.%fZ')


def json_to_item(obj, parent_id=None):
    kwargs = {
        'onedrive_id': obj['id'],
        'name': obj['name'],
        'parent_id': parent_id,
        'original_path': None,
    }
    if parent_id == 'root':
        kwargs['parent_id'] = None
    if 'folder' in obj:
        kwargs['is_folder'] = True
    elif 'file' in obj:
        kwargs['is_folder'] = False
        kwargs['size'] = obj['size']
        try:
            kwargs['hash'] = obj['file']['hashes']['quickXorHash']
        except KeyError:
            # File size == 0 usually does not have hash
            kwargs['hash'] = None
        kwargs['mdate'] = date_from_onedrive(
            obj['fileSystemInfo']['lastModifiedDateTime'])
    else:
        return None

    return models.Item(**kwargs)


class ThrottleError(Exception):
    def __init__(self, retry_after):
        self.retry_after = float(retry_after)

    def sleep(self):
        time.sleep(self.retry_after)


class Client:
    def __init__(self):
        self.config = {}
        try:
            with open('config.json') as f:
                self.config = json.load(f)
        except FileNotFoundError as e:
            logger.exception('Configuration not found', exc_info=e)
            raise e

        # First, try to use an existing token
        try:
            with open(TOKEN_FILE) as f:
                token = json.load(f)
                token['expires_in'] = time.time() - token['expires_at']
        except FileNotFoundError as e:
            logger.debug('Token not found', exc_info=e)
            token = {}

        # Microsoft might change token scopes, so we have to tell
        # Requests-OAuthlib to ignore them, like they do in their example.
        # Sadly, using environment variables is the only way.
        os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'
        os.environ['OAUTHLIB_IGNORE_SCOPE_CHANGE'] = '1'
        refresh_extra = {
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret']
        }
        self.oauth = OAuth2Session(
            client_id=self.config['client_id'], scope=SCOPES, token=token,
            auto_refresh_url=TOKEN_URL, auto_refresh_kwargs=refresh_extra,
            token_updater=self.token_saver)
        logger.info('Oauth client ready')

    def token_saver(self, token):
        with open(TOKEN_FILE, 'w') as f:
            logger.debug('Saving refreshed token')
            json.dump(token, f)

    def get_children(self, parent_id):
        select = '?select=id,name,file,folder,size,fileSystemInfo'
        url = '{}items/{}/children{}'.format(DRIVE_URL, parent_id, select)
        children = []
        while url:
            r = self.oauth.get(url)
            if r.status_code == 429:
                raise ThrottleError(r.headers['Retry-After'])
            if r.status_code != 200:
                logger.error('Could not get the children of item %s. '
                             'URL=%s Status=%d, response=%s', url, parent_id,
                             r.status_code, r.text)
                break
            data = r.json()
            children += [json_to_item(obj) for obj in data['value']]
            url = data.get('@odata.nextLink', '')
        return children

    def get_item_by_path(self, path):
        url = '{}root:/{}'.format(DRIVE_URL, path)
        r = self.oauth.get(url)
        data = r.json()
        if r.status_code != 200:
            logger.error(
                'Could not get the item %s. Status=%d, response=%s',
                path, data)
            return None
        return json_to_item(data)

    def create_folder(self, parent_id, name):
        create_url = '{}items/{}/children'.format(DRIVE_URL, parent_id)
        create_obj = {
            'name': name,
            'folder': {},
            '@microsoft.graph.conflictBehavior': 'rename'
        }
        r = self.oauth.post(create_url, json=create_obj)

        if r.status_code == 429:
            logger.debug('Sleeping during folder creation')
            time.sleep(float(r.headers['Retry-After']))
            return self.create_folder(parent_id, name)

        if r.status_code != 201:
            logger.error(
                'Could not create folder %s. Status=%d, response=%s',
                name, r.status_code, r.text)
            return None

        item = r.json()
        if item['name'] != name:
            logger.info('Renamed to avoid conflict', name, item['name'])
        return models.Item(
            item['id'], item['name'], None, True, True, parent_id=parent_id)

    def delete_item(self, item_id):
        logger.debug('Deleting item', item_id)
        del_url = '{}items/{}'.format(DRIVE_URL, item_id)
        r = self.oauth.delete(del_url)

        if r.status_code == 429:
            logger.debug('Sleeping during item deletion')
            time.sleep(float(r.headers['Retry-After']))
            return self.delete_item(item_id)

        if r.status_code == 404:
            logger.info('Treating a 404 during elimination as a success.')
            return True

        if r.status_code != 204:
            logger.error(
                'Could not delete item %s. Status=%d, response=%s', item_id,
                r.status_code, r.text)
            return False
        return True

    def upload(self, source_filename, target, parent_id, target_is_id=True):
        if target_is_id:
            create_url = '{}items/{}/createUploadSession'.format(
                DRIVE_URL, target)
        else:
            create_url = '{}root:/{}:/createUploadSession'.format(
                DRIVE_URL, target)

        stat = os.stat(source_filename)
        ctime = datetime.fromtimestamp(stat.st_ctime)
        mtime = datetime.fromtimestamp(stat.st_mtime)

        if stat.st_size == 0:
            logger.warning('Ignoring empty file %s', source_filename)
            return None

        obj = {
            'item': {
                'fileSystemInfo': {
                    'createdDateTime': date_to_onedrive(ctime),
                    'lastModifiedDateTime': date_to_onedrive(mtime),
                }
            }
        }
        if not target_is_id:
            obj['name']: os.path.basename(target)
            obj['@microsoft.graph.conflictBehavior'] = 'rename'

        r = self.oauth.post(create_url, json=obj)
        if r.status_code != 200:
            logger.error(
                'Cannot create the upload session. Status=%d, response=%s',
                r.status_code, r.text)
            return None
        data = r.json()
        upload_url = data['uploadUrl']

        chunk_size = 10485760  # 10 MiB, multiple of 320 KiB
        sent = 0
        with open(source_filename, 'rb') as f:
            while sent < stat.st_size:
                upper = min(stat.st_size, sent + chunk_size)
                crange = 'bytes {}-{}/{}'.format(sent, upper - 1, stat.st_size)
                buffer = f.read(upper - sent)
                r = self.oauth.put(upload_url, buffer,
                                   headers={'Content-Range': crange})
                if r.status_code not in (200, 201, 202):
                    logger.error(
                        'Cannot upload chunk %d. Status=%d, response=%s',
                        sent, r.status_code, r.text)
                    return None
                sent = upper

        item = json_to_item(r.json(), parent_id)
        item.original_path = source_filename
        return item
