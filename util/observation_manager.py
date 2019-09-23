from .base_manager import FHIRResourcesManager
import dateutil.parser
from db.manager import Patient, Encounter, Observation
import sys, requests, ndjson


class FHIRObservationResourceManager(FHIRResourcesManager):

    def __init__(self, db):
        super().__init__(db=db)
        self.base_url = self.base_url + 'Observation.ndjson'
        self.model = Observation(db=self.db)

    def run(self):
        self.fetch()

    def fetch(self):
        print('About to begin fetching from ' + self.base_url)
        with requests.get(self.base_url, stream=True) as r:
            print('Request successful')
            items = r.json(cls=ndjson.Decoder)
            self.process(items)

    def store(self, data):
        patient_query_data = {
            'fieldname': 'source_id',
            'value': data['patient'].replace('Patient/', '')
        }

        patient_row = Patient(db=self.db).get(patient_query_data)
        if patient_row is None:
            return None
        patient_id = patient_row[0]
        data['patient_id'] = patient_id

        encounter_query_data = {
            'fieldname': 'source_id',
            'value': data['encounter'].replace('Encounter/', '')
        }
        encounter_row = Encounter(db=self.db).get(encounter_query_data)
        data['encounter_id'] = encounter_row[0]
        return self.model.insert(data=data)

    def process(self, observations):
        print('About to begin processing observations')
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
            self.store(data)

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

    def get_type_value_data(self, observation):
        code = observation.get('code')
        value_quantity = observation.get('valueQuantity')
        data = {
            'type_code': None,
            'type_code_system': None,
            'value': None,
            'unit_code': None,
            'unit_code_system': None
        }
        if code is not None and value_quantity is not None:
            data['type_code'], data['type_code_system'] = self.get_type_code_data(code)
            data['value'], data['unit_code'], data['unit_code_system'] = self.get_value_code_data(value_quantity)
        else:
            components = observation.get('component')

            if isinstance(components, list):
                for component in components:
                    if component.get('code') is not None:
                        data['type_code'], data['type_code_system'] = self.get_type_code_data(component.get('code'))
                    if component.get('valueQuantity') is not None:
                        data['value'], data['unit_code'], data['unit_code_system'] = self.get_value_code_data(
                            component.get('valueQuantity'))
        return data
