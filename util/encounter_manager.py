from .base_manager import FHIRResourcesManager
from db.manager import Encounter, Patient
import sys


class FHIREncounterResourceManager(FHIRResourcesManager):

    def __init__(self, db):
        super().__init__(db=db)
        self.base_url = self.base_url + 'Encounter.ndjson'
        self.model = Encounter(db=self.db)

    def run(self):
        pass

    def fetch(self):
        super().fetch()

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
        return self.model.insert(data=data)

    def process(self, encounters):
        for encounter in encounters:
            source_id = encounter.get('id', None)
            patient = self.get_patient(encounter)
            start_date = self.get_start_date(encounter)
            end_date = self.get_end_date(encounter)
            type_code, type_code_system = self.get_type_code_data(encounter)
            data = {
                'source_id': source_id,
                'patient': patient,
                'start_date': start_date,
                'end_date': end_date,
                'type_code': type_code,
                'type_code_system': type_code_system
            }
            self.store(data)

    def get_patient(self, encounter):

        if encounter.get('subject', None) is not None:
            return encounter.get('subject').get('reference')
        return None

    def get_start_date(self, encounter):
        if encounter.get('period', None) is not None:
            return encounter.get('period').get('start')
        return None

    def get_end_date(self, encounter):
        if encounter.get('period', None) is not None:
            return encounter.get('period').get('end')
        return None

    def get_type_code_data(self, encounter):
        type = encounter.get('type', None)
        if not isinstance(type, list):
            return None, None
        coding = type[0].get('coding')
        if coding is None or not isinstance(coding, list):
            return None, None

        return coding[0].get('code'), coding[0].get('system')
