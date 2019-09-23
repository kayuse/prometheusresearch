from .base_manager import FHIRResourcesManager
from db.manager import Encounter, Patient
from db import DB
from multiprocessing import Process
import sys, requests, ndjson, math


class FHIREncounterResourceManager(FHIRResourcesManager):
    pool_count = 3

    def __init__(self):
        super().__init__()
        self.base_url = self.base_url + 'Encounter.ndjson'

    def run(self):
        self.fetch()

    def fetch(self):
        print('About to begin fetching from ' + self.base_url)
        with requests.get(self.base_url, stream=True) as r:
            print('Request successful')
            items = r.json(cls=ndjson.Decoder)
            total_items = len(items)
            print('There are ' + str(total_items) + ' encounters')
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
        return self.model.insert(data=data)

    def process(self, encounters):
        db = DB()
        db.connect()
        self.model = Encounter(db=db)
        print('There are ' + str(len(encounters)) + ' to be processed in this batch')
        for encounter in encounters:
            data = self.run_encounter_process(encounter)
            self.store(data, db)

        print('Done processing ' + str(len(encounters)) + ' for this batch ')
        db.close()

    def run_encounter_process(self, encounter):
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
        return data

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
