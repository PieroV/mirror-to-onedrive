from recordclass import recordclass


Item = recordclass(
    'Item',
    'onedrive_id name original_path existing is_folder size mdate hash '
    'parent_id',
    defaults=(True, False, 0, 0, '', None))
