from .base_manager import FHIRResourcesManager
from db.manager import Patient, Encounter, Procedure
import dateutil.parser
import sys, requests, ndjson


class FHIRProcedureResourceManager(FHIRResourcesManager):

    def __init__(self, db):
        super().__init__(db=db)
        self.base_url = self.base_url + 'Procedure.ndjson'
        self.model = Procedure(db=self.db)

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

    def process(self, procedures):

        for procedure in procedures:
            source_id = procedure.get('id', None)
            patient = self.get_patient(procedure)
            encounter = self.get_encounter(procedure)
            date = self.get_date(procedure)
            type_code, type_code_system = self.get_type_code_data(procedure)
            data = {
                'source_id': source_id,
                'patient': patient,
                'encounter': encounter,
                'procedure_date': date,
                'type_code': type_code,
                'type_code_system': type_code_system
            }

            self.store(data)

    def get_patient(self, procedure):

        if procedure.get('subject', None) is not None:
            return procedure.get('subject').get('reference')
        return None

    def get_encounter(self, procedure):

        if procedure.get('context', None) is not None:
            return procedure.get('context').get('reference')
        return None

    def get_date(self, procedure):
        date = procedure.get('performedDateTime', None)
        if date is not None:
            date_obj = dateutil.parser.parse(date)
            return date_obj.date()

        if procedure.get('performedPeriod', None) is not None:
            date = procedure.get('performedPeriod').get('start')
            performed_period_date = dateutil.parser.parse(date)
            return performed_period_date.date()
        return None

    def get_type_code_data(self, procedure):
        code = procedure.get('code', None)
        if code is None:
            return None, None

        coding = code.get('coding')
        if not isinstance(coding, list):
            return None, None

        return coding[0].get('code'), coding[0].get('system')
