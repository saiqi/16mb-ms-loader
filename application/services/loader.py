from nameko.rpc import rpc, RpcProxy
import bson.json_util


class LoaderServiceError(Exception):
    pass


class LoaderService(object):
    name = 'loader'

    metadata = RpcProxy('metadata')
    datastore = RpcProxy('datastore')
    referential = RpcProxy('referential')

    @rpc
    def write(self, write_policy, meta, target_table, records, upsert_key=None, delete_keys=None, chunk_size=None):
        if write_policy not in ('insert', 'upsert', 'bulk_insert', 'delete_insert', 'delete_bulk_insert',
                                'truncate_insert', 'truncate_bulk_insert'):
            raise LoaderServiceError('Wrong value for parameter write_policy')

        if write_policy in ('bulk_insert', 'delete_bulk_insert', 'truncate_bulk_insert') and not chunk_size:
            raise LoaderServiceError('Bulk loading strategy requires a chunk size')

        try:
            meta = list(map(tuple, meta))
        except:
            raise LoaderServiceError('Bad formated meta')

        if write_policy == 'insert':
            self.datastore.insert(target_table, records, meta)
        elif write_policy == 'upsert':
            self.datastore.upsert(target_table, upsert_key, records, meta)
        elif write_policy == 'bulk_insert':
            self.datastore.bulk_insert(target_table, records, meta, chunk_size=chunk_size)
        elif write_policy == 'delete_insert':
            self.datastore.delete(target_table, delete_keys)
            self.datastore.insert(target_table, records, meta)
        elif write_policy == 'delete_bulk_insert':
            self.datastore.delete(target_table, delete_keys)
            self.datastore.bulk_insert(target_table, records, meta, chunk_size=chunk_size)
        elif write_policy == 'truncate_insert':
            self.datastore.truncate(target_table)
            self.datastore.insert(target_table, records, meta)
        else:
            self.datastore.truncate(target_table)
            self.datastore.bulk_insert(target_table, records, meta, chunk_size=chunk_size)

        return {'target_table': target_table, 'count': len(records)}

    def compute_transformation(self, t, param_value=None):
        try:
            self.datastore.create_or_replace_python_function(t['function_name'], t['function'])
        except:
            raise LoaderServiceError('An error occured while creating python function in transformation {}'.format(t['id']))

        if t['type'] == 'fit' and t['process_date'] is None:
            try:
                last_entry = bson.json_util.loads(self.datareader.select(t['output']))
                if last_entry and len(last_entry) > 0:
                    self.datastore.delete(t['target_table'], {'id': last_entry[0]['id']})
                self.datastore.insert_from_select(t['target_table'], t['output'], None)
            except:
                raise LoaderServiceError('An error occured while fitting transformation {}'.format(t['id']))
            self.metadata.update_process_date(t['id'])
        elif t['type'] in ('transform', 'predict',) and t['materialized'] is True:
            try:
                if t['parameters'] is None:
                    self.datastore.truncate(t['target_table'])
                    self.datastore.insert_from_select(t['target_table'], t['output'], None)
                else:
                    if len(t['parameters']) > 1:
                        raise BadRequest('Does not support transformation with multiple parameters')
                    param_name = t['parameters'][0]
                    if param_value is None:
                        raise BadRequest('Transformation requires a parameter')
                    self.datastore.delete(t['target_table'], {param_name: param_value})
                    self.datastore.insert_from_select(t['target_table'], t['output'], [param_value])
            except:
                raise LoaderServiceError('An error occured while computing transformation {}'.format(t['id']))
            self.metadata.update_process_date(t['id'])

    @rpc
    def update_transformations(self, trigger_table, param_value=None):
        meta = self.metadata.get_update_pipeline(trigger_table)
        if not meta:
            return {'trigger_table': trigger_table}
        pipeline = bson.json_util.loads(meta)
        for job in pipeline:
            for t in job['transformations']:
                self._compute_transformation(t, param_value)
        return {'trigger_table': trigger_table}

    @rpc
    def apply_transformation(self, transformation_id, param_value=None):
        result = self.metadata.get_transformation(transformation_id)

        transformation = bson.json_util.loads(result)
        self._compute_transformation(transformation, param_value)

        return {'id': transformation_id}

    @rpc
    def update_entry_ngrams(self, entry_id):
        return self.referential.update_entry_ngrams(entry_id)

    @rpc
    def add_entity(self, data):
        self.referential.add_entity(**data)
        return {'id': data['id']}

    @rpc
    def add_event(self, data):
        data = self.referential.add_event(**data)
        return {'id': data['id']}
