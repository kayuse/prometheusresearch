from .base_manager import FHIRResourcesManager
from db.manager import Patient, Encounter, Procedure
from db import DB
import dateutil.parser
from multiprocessing import Process
import sys, requests, ndjson, math


class FHIRProcedureResourceManager(FHIRResourcesManager):
    pool_count = 5

    def __init__(self):
        super().__init__()
        self.base_url = self.base_url + 'Procedure.ndjson'

    def run(self):
        self.fetch()

    def fetch(self):
        print('About to begin fetching from ' + self.base_url)
        with requests.get(self.base_url, stream=True) as r:
            print('Request successful')
            items = r.json(cls=ndjson.Decoder)
            total_items = len(items)
            print('There are ' + str(total_items) + ' procedures')
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
            data['encounter_id'] = encounter_row[0]
        else:
            data['encounter_id'] = None
        return self.model.insert(data=data)

    def process(self, procedures):
        db = DB()
        db.connect()
        print('There are ' + str(len(procedures)) + ' to be processed in this batch')
        self.model = Procedure(db=db)
        for procedure in procedures:
            data = self.run_procedure_process(procedure)
            self.store(data, db)
        db.close()
        print('Done processing ' + str(len(procedures)) + ' procedures')

    def run_procedure_process(self, procedure):
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
        return data

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
