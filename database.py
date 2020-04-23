import models

from datetime import datetime
import logging
import sqlite3

DB_FILE = 'items.db'

logger = logging.getLogger(__name__)


def record_to_item(row):
    data = {
        'onedrive_id': row[0],
        'name': row[1],
        'original_path': row[2],
        'existing': row[3],
        'parent_id': row[8],
    }
    if bool(row[4]):
        data['is_folder'] = True
    else:
        data['is_folder'] = False
        data['size'] = row[5]
        data['mdate'] = datetime.fromtimestamp(float(row[6]))
        data['hash'] = row[7]

    return models.Item(**data)


def item_to_tuple(item, id_as_last=False):
    lst = list(item)
    if item.is_folder:
        lst[4] = 1
        lst[5] = 0
        lst[6] = 0
        lst[7] = None
    else:
        lst[4] = 0
        lst[6] = lst[6].timestamp()
    if id_as_last:
        lst.append(lst.pop(0))
    return tuple(lst)


class Database:

    def __init__(self):
        self.db = sqlite3.connect(DB_FILE)

    def add_item(self, item):
        query = 'INSERT INTO item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        cur = self.db.cursor()
        try:
            cur.execute(query, item_to_tuple(item))
        except sqlite3.IntegrityError as e:
            logger.error('Could not add item %s', item.onedrive_id, exc_info=e)
            return False
        return True

    def update_items(self, items):
        items = [item_to_tuple(i, True) for i in items]
        query = ('UPDATE item SET onedrive_name = ?, original_path = ?, '
                 'existing = ?, is_folder = ?, size = ?, mdate = ?, hash = ?, '
                 'parent_id = ? WHERE onedrive_id = ?')
        cur = self.db.cursor()
        cur.executemany(query, items)

    def add_update_item(self, item):
        cur = self.db.cursor()
        cur.execute('SELECT onedrive_id FROM item WHERE onedrive_id = ?',
                    (item.onedrive_id,))
        if cur.fetchone() is not None:
            self.update_items([item])
        else:
            self.add_item(item)

    def delete_items(self, items):
        to_delete = []
        for i in items:
            if type(i) == models.Item:
                to_delete.append((i.onedrive_id,))
            else:
                to_delete.append((i,))

        cur = self.db.cursor()
        cur.executemany(
            'DELETE FROM item WHERE onedrive_id = ?', to_delete)

    def get_children(self, parent, where='', where_fields=tuple()):
        cur = self.db.cursor()

        if parent is not None:
            query = 'SELECT * FROM item WHERE parent_id = ?'
            where_fields = (parent,) + where_fields
        else:
            query = 'SELECT * FROM item WHERE parent_id IS NULL'

        if where:
            query += ' AND ' + where
        cur.execute(query, where_fields)
        return [record_to_item(row) for row in cur.fetchall()]

    def get_from_root(self, name):
        cur = self.db.cursor()
        cur.execute('SELECT * FROM item WHERE onedrive_name = ? '
                    'AND parent_id is NULL', (name,))
        row = cur.fetchone()
        if row:
            return record_to_item(row)

    def commit(self):
        self.db.commit()

    def vacuum(self):
        self.db.cursor().execute('VACUUM;')

    def close(self):
        self.db.close()
