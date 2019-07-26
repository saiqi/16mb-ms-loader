import logging

from nameko.rpc import rpc, RpcProxy
from nameko.messaging import consume
from nameko.events import EventDispatcher
from kombu.messaging import Queue, Exchange
from nameko.extensions import DependencyProvider
import bson.json_util

_log = logging.getLogger(__name__)


class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return

        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class LoaderServiceError(Exception):
    pass


class LoaderService(object):
    name = 'loader'

    metadata = RpcProxy('metadata')
    datastore = RpcProxy('datastore')
    referential = RpcProxy('referential')
    dispatch = EventDispatcher()
    error = ErrorHandler()

    def write(self, write_policy, meta, target_table, records, upsert_key=None, delete_keys=None, chunk_size=None):
        _log.info(
            f'Writing in {target_table} using {write_policy} strategy ...')
        if write_policy not in ('insert', 'upsert', 'bulk_insert', 'delete_insert', 'delete_bulk_insert',
                                'truncate_insert', 'truncate_bulk_insert'):
            _log.error(f'{write_policy} not supported')
            raise LoaderServiceError('Wrong value for parameter write_policy')

        if write_policy in ('bulk_insert', 'delete_bulk_insert', 'truncate_bulk_insert') and not chunk_size:
            _log.error('chunk_size missing')
            raise LoaderServiceError(
                'Bulk loading strategy requires a chunk size')

        try:
            meta = list(map(tuple, meta))
        except:
            _log.error('Bad formated meta')
            raise LoaderServiceError('Bad formated meta')

        if write_policy == 'insert':
            self.datastore.insert(
                target_table, bson.json_util.dumps(records), meta)
        elif write_policy == 'upsert':
            self.datastore.upsert(target_table, upsert_key,
                                  bson.json_util.dumps(records), meta)
        elif write_policy == 'bulk_insert':
            self.datastore.bulk_insert(
                target_table, bson.json_util.dumps(records), meta, chunk_size=chunk_size)
        elif write_policy == 'delete_insert':
            self.datastore.delete(target_table, delete_keys)
            self.datastore.insert(
                target_table, bson.json_util.dumps(records), meta)
        elif write_policy == 'delete_bulk_insert':
            self.datastore.delete(target_table, delete_keys)
            self.datastore.bulk_insert(
                target_table, bson.json_util.dumps(records), meta, chunk_size=chunk_size)
        elif write_policy == 'truncate_insert':
            self.datastore.truncate(target_table)
            self.datastore.insert(
                target_table, bson.json_util.dumps(records), meta)
        else:
            self.datastore.truncate(target_table)
            self.datastore.bulk_insert(
                target_table, bson.json_util.dumps(records), meta, chunk_size=chunk_size)

        _log.info('Datastore microservice wrote all the records !')
        return {'target_table': target_table, 'count': len(records)}

    def _compute_transformation(self, t, param_value=None):
        _log.info(f"Computing transformation {t['id']}")
        try:
            self.datastore.create_or_replace_python_function(
                t['function_name'], t['function'])
        except:
            _log.error(
                'Something went wrong while creating the underlying python function')
            raise LoaderServiceError(
                'An error occured while creating python function in transformation {}'.format(t['id']))

        if t['type'] == 'fit' and t['process_date'] is None:
            _log.info(
                'Transformation has been set as \"fit\" kind. This must be processed !')
            try:
                last_entry = bson.json_util.loads(
                    self.datareader.select(t['output']))
                if last_entry and len(last_entry) > 0:
                    _log.info('Deleting the previous result ...')
                    self.datastore.delete(t['target_table'], {
                                          'id': last_entry[0]['id']})
                _log.info('Computing current result ...')
                self.datastore.insert_from_select(
                    t['target_table'], t['output'], None)
            except:
                _log.error(
                    'Something went wrong while deleting and inserting the result')
                raise LoaderServiceError(
                    'An error occured while fitting transformation {}'.format(t['id']))
            _log.info('Updating process date in metadata ...')
            self.metadata.update_process_date(t['id'])
        elif t['type'] in ('transform', 'predict',) and t['materialized'] is True:
            _log.info(
                'Transformation has been set as materialized \"transform\" or \"predict\" kind. This must be processed !')
            if t['parameters'] is None:
                _log.info('No parameters truncating the table ...')
                self.datastore.truncate(t['target_table'])
                _log.info('Inserting the result ...')
                self.datastore.insert_from_select(
                    t['target_table'], t['output'], None)
            else:
                if len(t['parameters']) > 1:
                    raise LoaderServiceError(
                        'Does not support transformation with multiple parameters')
                param_name = t['parameters'][0]
                if param_value is None:
                    raise LoaderServiceError(
                        'Transformation requires a parameter')
                _log.info(
                    'We will delete the previous result according to the provided parameter')
                self.datastore.delete(t['target_table'], {
                    param_name: param_value})
                _log.info('Inserting the result ...')
                self.datastore.insert_from_select(
                    t['target_table'], t['output'], [param_value])
            _log.info('Updating process date in metadata ...')
            self.metadata.update_process_date(t['id'])

    def update_transformations(self, trigger_table, param_value=None):
        _log.info(f'Updating transformation related to {trigger_table} ...')
        meta = self.metadata.get_update_pipeline(trigger_table)
        if not meta:
            _log.info('Nothing to do ...')
            return {'trigger_table': trigger_table}
        pipeline = bson.json_util.loads(meta)
        for job in pipeline:
            for t in job['transformations']:
                self._compute_transformation(t, param_value)
        return {'trigger_table': trigger_table}

    def apply_transformation(self, transformation_id, param_value=None):
        result = self.metadata.get_transformation(transformation_id)

        transformation = bson.json_util.loads(result)
        self._compute_transformation(transformation, param_value)

        return {'id': transformation_id}

    def update_entry_ngrams(self, entry_id):
        return self.referential.update_entry_ngrams(entry_id)

    def add_entity(self, data):
        self.referential.add_entity(**data)
        return {'id': data['id']}

    def add_event(self, data):
        data = self.referential.add_event(**data)
        return {'id': data['id']}

    @consume(queue=Queue(name='evt_all_inputs',
                         exchange=Exchange(name='all_inputs', type='topic', auto_delete=True)))
    def handle_all_inputs(self, payload):
        input_ = bson.json_util.loads(payload)
        _log.info(f'Handling input {input_["id"]}')
        if input_['status'] == 'UNCHANGED':
            _log.info('Received an unchanged input ... ignoring !')
            return
        ref = input_['referential']
        if ref.get('entities', None):
            _log.info('Handling entities ...')
            for e in ref['entities']:
                ent = self.add_entity(e)
                self.update_entry_ngrams(ent['id'])
        if ref.get('events', None):
            _log.info('Handling events ...')
            for e in ref['events']:
                evt = self.add_event(e)
                self.update_entry_ngrams(evt['id'])
        datastore = input_['datastore']
        for d in datastore:
            res = self.write(**d)
            d_keys = d.get('delete_keys', None)
            param_value = list(d_keys.values())[0] if d_keys else None
            self.update_transformations(
                res['target_table'], param_value=param_value)
        ack = bson.json_util.dumps({
            'id': input_['id'],
            'checksum': input_.get('checksum', None),
            'meta': input_.get('meta', None)})
        self.dispatch('input_loaded', ack)
