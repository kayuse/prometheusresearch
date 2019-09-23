from .base_manager import FHIRResourcesManager
import dateutil.parser
from db import DB
from db.manager import Patient, Encounter, Observation
from multiprocessing import Process
import sys, requests, ndjson, math


class FHIRObservationResourceManager(FHIRResourcesManager):
    pool_count = 30

    def __init__(self):
        super().__init__()
        self.base_url = self.base_url + 'Observation.ndjson'

    def run(self):
        self.fetch()

    def fetch(self):
        print('About to begin fetching from ' + self.base_url)
        with requests.get(self.base_url, stream=True) as r:
            print('Request successful')
            items = r.json(cls=ndjson.Decoder)
            total_items = len(items)
            print('There are ' + str(total_items) + ' observations')
            item_cursor = 0
            batch_count = math.ceil(total_items / self.pool_count)
            print('Splitting into ' + str(batch_count) + ' for each process')
            to_item = batch_count
            processes = []
            for i in range(self.pool_count):
                current_item = items[item_cursor:to_item]
                item_cursor += batch_count
                to_item += batch_count
                p = Process(target=self.process, args=(current_item,))
                processes.append(p)

            [x.start() for x in processes]
            [x.join() for x in processes]

    def store(self, data, db):
        patient_query_data = {
            'fieldname': 'source_id',
            'value': data['patient'].replace('Patient/', '')
        }

        patient_row = Patient(db=db).get(patient_query_data)
        if patient_row is None:
            return None
        patient_id = patient_row[0]
        data['patient_id'] = patient_id
        if data['encounter'] is not None:
            encounter_query_data = {
                'fieldname': 'source_id',
                'value': data['encounter'].replace('Encounter/', '')
            }
            encounter_row = Encounter(db=db).get(encounter_query_data)
            if encounter_row is not None:
                data['encounter_id'] = encounter_row[0]
        if data.get('encounter_id') is None:
            print(data)
        return self.model.insert(data=data)

    def process(self, observations):
        db = DB()
        db.connect()
        self.model = Observation(db=db)
        print('There are ' + str(len(observations)) + ' to be processed in this batch')
        for observation in observations:
            source_id = observation.get('id', None)
            patient = self.get_patient(observation)
            encounter = self.get_encounter(observation)

            date = self.get_date(observation)

            data = {
                'source_id': source_id,
                'patient': patient,
                'encounter': encounter,
                'observation_date': date
            }
            data = {**data, **self.get_type_value_data(observation)}
            if data['value'] is not None:
                self.store(data, db)
        print('Done processing ' + str(len(observations)) + ' for this batch ')

    def get_patient(self, observation):

        if observation.get('subject', None) is not None:
            return observation.get('subject').get('reference')
        return None

    def get_encounter(self, observation):

        if observation.get('context', None) is not None:
            return observation.get('context').get('reference')
        return None

    def get_date(self, observation):
        effective_datetime = observation.get('effectiveDateTime')
        if effective_datetime is None:
            return None
        return dateutil.parser.parse(effective_datetime)

    def get_type_code_data(self, code):

        coding = code.get('coding')
        if not isinstance(coding, list):
            return None, None

        return coding[0].get('code'), coding[0].get('system')

    def get_value_code_data(self, value_quantity):
        return value_quantity.get('value'), value_quantity.get('unit'), value_quantity.get('system')

    def get_code_data(self, observation):
        data = {
            'type_code': None,
            'type_code_system': None
        }
        code = observation.get('code')

        if code is not None:
            data['type_code'], data['type_code_system'] = self.get_type_code_data(code)
        else:
            components = observation.get('component')

            if isinstance(components, list):
                for component in components:

                    if component.get('code') is not None:
                        data['type_code'], data['type_code_system'] = self.get_type_code_data(component.get('code'))

        return data

    def get_value_data(self, observation):
        value_quantity = observation.get('valueQuantity')
        data = {
            'value': None,
            'unit_code': None,
            'unit_code_system': None
        }
        if value_quantity is not None:
            data['value'], data['unit_code'], data['unit_code_system'] = self.get_value_code_data(value_quantity)
        else:
            components = observation.get('component')

            if isinstance(components, list):
                for component in components:
                    if component.get('valueQuantity') is not None:
                        data['value'], data['unit_code'], data['unit_code_system'] = self.get_value_code_data(
                            component.get('valueQuantity'))
        return data

    def get_type_value_data(self, observation):
        data = {
            'type_code': None,
            'type_code_system': None,
            'value': None,
            'unit_code': None,
            'unit_code_system': None
        }
        code_data = self.get_code_data(observation=observation)
        value_data = self.get_value_data(observation)
        data = {**data, **code_data, **value_data}

        return data
