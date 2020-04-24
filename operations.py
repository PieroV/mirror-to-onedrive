import client
import database

from quickxorhash import quickxorhash

import base64
import logging
import pathlib

logger = logging.getLogger(__name__)


def quickxor_file(filename):
    h = quickxorhash()
    with open(filename, 'rb') as f:
        buf_size = 4096
        while True:
            buf = f.read(buf_size)
            if buf:
                h.update(buf)
            else:
                break
    return base64.b64encode(h.digest()).decode()


class Node:

    def __init__(self, path, item, db, client, parent_node=None):
        self.db = db
        self.client = client
        self.queries = 0
        self.parent_node = parent_node

        if path is not None and not isinstance(path, pathlib.Path):
            path = pathlib.Path(path)
        self.path = path
        self.item = item

        if self.item is not None and parent_node is not None:
            self.onedrive_path = '{}/{}'.format(
                parent_node.onedrive_path, self.item.name)
        elif self.item is not None:
            self.onedrive_path = self.item.name
        else:
            self.onedrive_path = ''

    def act(self, check_hash=False):
        if self.path is not None and self.item is not None:
            logger.debug('Act: update %s, %s', self.path,
                         self.item.onedrive_id)
            if not self.check_folder():
                return

            if self.item.original_path != str(self.path):
                # Usually this happens only the first time, so an
                # additional query here is not that bad
                self.item.original_path = str(self.path)
                self.db.update_items([self.item])
                self.queries += 1

            # Directories are always up to date
            if self.path.is_file():
                self.update(check_hash)
        elif self.path is not None:
            logger.debug('Act: create %s', self.path)
            self.create()
        elif self.item is not None:
            logger.debug('Act: delete %s %s',
                         self.item.onedrive_id, self.item.name)
            self.delete()
        else:
            logger.error('Node with both path and item none')

    def check_folder(self):
        if (self.item is None or self.path is None
                or self.item.is_folder == self.path.is_dir()):
            return True

        logging.warning('Inconsistency in path and item is_folder. '
                        'Deleting old item and creating a new one.')
        self.delete()
        self.create()
        return False

    def update(self, check_hash=False):
        if self.path is None or self.item is None:
            logger.error('Called update with None path or item')
            return False
        if self.path.is_dir():
            logger.debug('Ignoring update on directory %s', self.path)
            return True

        mtime_window = 2
        stat = self.path.stat()
        updated = (stat.st_size == self.item.size
                   and (abs(stat.st_mtime - self.item.mdate.timestamp())
                        < mtime_window))

        if check_hash and updated:
            hash_ = quickxor_file(str(self.path))
            if hash_ != self.item.hash:
                updated = False
                logger.info('%s passed size and mtime check but not hash',
                            self.path)

        if updated:
            logger.debug('Item %s already up to date', self.path)
            return True

        if stat.st_size == 0:
            # XXX Maybe still possible to do something
            logger.warning('Skipping update of an empty file')
            return True

        logger.debug('Uploading new version of %s', self.path)
        parent_id = (self.parent_node.item.onedrive_id
                     if self.parent_node is not None else None)
        new_item = self.client.upload(
            str(self.path), self.item.onedrive_id, parent_id)
        if new_item is None:
            logger.error('Could not update %s', self.path)
            return False

        self.item = new_item
        self.db.update_items([new_item])
        self.queries += 1
        return True

    def create(self):
        parent_id = (self.parent_node.item.onedrive_id
                     if self.parent_node is not None else None)
        if self.item is not None:
            logger.error('Tried to call create on a node that already has an '
                         'item (%s, %s)', self.path, self.item.onedrive_id)
            return False

        if self.path.is_dir():
            item = self.client.create_folder(parent_id, self.path.name)
        elif self.path.is_file():
            target = self.parent_node.onedrive_path + '/' + self.path.name
            item = self.client.upload(
                str(self.path), target, self.parent_node.item.onedrive_id,
                False)
        else:
            logger.error('Tried to call create on something that is neither a '
                         'file nor a directory (%s)', self.path)
            return False

        if item is None:
            logger.error('Creation of %s failed', self.path)
            return False
        self.db.add_item(item)
        self.queries += 1
        self.item = item

        if self.parent_node is not None:
            self.onedrive_path = self.parent_node.onedrive_path + '/'
        else:
            self.onedrive_path = ''
        self.onedrive_path += item.name

        return True

    def delete(self):
        okay = False
        if self.client.delete_item(self.item.onedrive_id):
            self.db.delete_items([self.item])
            self.queries += 1
            okay = True
        if not okay:
            logger.error('Could not delete %s (%s).', self.item.onedrive_id,
                         self.item.name)
        self.item = None
        return okay

    def get_children(self):
        if self.path is None or not self.path.is_dir():
            return []

        # Avoid saving children, because they contain the reference to
        # us, and this prevents garbage collection.
        return ChildrenLister(self).get_children()


class ChildrenLister:

    def __init__(self, node):
        if node.path is None or not node.path.is_dir():
            raise ValueError('Need a directory to list children')

        self.node = node
        self.path = node.path
        self.item = node.item
        self.db = node.db
        self.client = node.client

        self.children = {}
        self.orphaned_items = {}
        self.new_children = {}
        self.conflicts = {}

    def add_child(self, path, item):
        self.children[path.name] = Node(
            path, item, self.db, self.client, self.node)

    def list_items(self):
        if self.item is not None:
            items = self.db.get_children(self.item.onedrive_id)
        else:
            return

        for item in items:
            if item.original_path is not None:
                path = pathlib.Path(item.original_path)
                if path.exists():
                    self.add_child(path, item)
                else:
                    item.original_path = None
            # Not elif: might change in the previous if
            if item.original_path is None:
                self.orphaned_items[item.name.lower()] = item

    def list_fs(self):
        for i in self.path.iterdir():
            if i.name in self.children:
                continue
            key = i.name.lower()
            if i not in self.new_children:
                self.new_children[key] = []
            self.new_children[key].append(i)

    def resolve_simple(self):
        to_add = []
        for lower, files in self.new_children.items():
            if len(files) == 1 and lower in self.orphaned_items:
                self.add_child(files[0], self.orphaned_items.pop(lower))
            elif lower not in self.orphaned_items:
                # This is a conclict, but OneDrive will handle it
                to_add += files
            else:
                self.conflicts[lower] = files
        self.new_children = to_add

    def resolve_conflicts(self):
        for lower, files in self.conflicts.items():
            item = self.orphan_items[lower]
            associate_to = -1
            for i in range(len(files)):
                hash_ = quickxor_file(files[i])
                if hash_ == item.hash:
                    associate_to = i
                    break
            if associate_to != -1:
                self.add_child(files.pop(associate_to), item)
                del self.orphaned_items[lower]
            # Leave the rest to OneDrive
            self.new_children += files

    def get_children(self):
        logger.debug('Started children lister for %s', self.path)

        # List from DB and associate whatever possible
        self.list_items()
        # List from filesystem, to look for new files/directories
        self.list_fs()
        # Resolve what does not create conclifcts (for us)
        self.resolve_simple()
        # Try to resolve any conflict
        self.resolve_conflicts()

        oprhaned_nodes = [Node(None, item, self.db, self.client, self.node)
                          for item in self.orphaned_items.values()]
        new_nodes = [Node(path, None, self.db, self.client, self.node)
                     for path in self.new_children]

        return list(self.children.values()) + oprhaned_nodes + new_nodes


class Operations:

    def __init__(self):
        self.db = database.Database()
        self.client = client.Client()

        # Get the drives, only to test the connection, raise any error,
        # if needed, or refresh the token with an easy request
        # a GET, without e.g. POST data
        self.client.get_drives()

    def populate_db(self):
        logger.info('Starting populating the database')

        commit_every_n = 1000
        to_get = []

        self.db.mark_not_existing()

        for one_path, local_path in self.client.config['synchronize'].items():
            item = self.client.get_item_by_path(one_path)
            item.original_path = local_path
            self.db.add_update_item(item)
            if item.is_folder:
                # Should always be the case for this kind of query
                to_get.append(item.onedrive_id)
        self.db.commit()

        counter = 0
        while to_get:
            parent_id = to_get[0]
            logger.debug('Populating children of %s', parent_id)

            try:
                children = self.client.get_children(parent_id)
            except client.ThrottleError as e:
                logger.debug('Throttle request: sleeping for %i',
                             e.retry_after)
                e.sleep()
                continue

            to_get.pop(0)
            for item in children:
                item.parent_id = parent_id
                self.db.add_update_item(item)
                if item.is_folder:
                    to_get.append(item.onedrive_id)

                counter += 1
                if counter % commit_every_n == 0:
                    self.db.commit()

        self.db.delete_not_existing()
        self.db.commit()
        self.db.vacuum()

    def compare_trees(self, check_hash=False):
        save_every_n = 1000

        to_work = []
        for name, dir_ in self.client.config['synchronize'].items():
            to_work.append(Node(
                pathlib.Path(dir_),
                self.db.get_from_root(name),
                self.db,
                self.client))

        unsaved = 0
        while to_work:
            node = to_work.pop()
            node.act(check_hash)
            to_work = node.get_children() + to_work

            unsaved += node.queries
            if unsaved > save_every_n:
                self.db.commit()
                logger.debug('Committing (%d unsaved)', unsaved)
                unsaved = 0
