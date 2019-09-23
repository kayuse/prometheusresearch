from multiprocessing import Process
import configparser
from db import DB
from datetime import datetime
from util.patient_manager import FHIRPatientResourcesManager
from util.encounter_manager import FHIREncounterResourceManager
from util.procedure_manager import FHIRProcedureResourceManager
from util.observation_manager import FHIRObservationResourceManager
import sys

print('started', datetime.now())
config = configparser.ConfigParser()
config.read('config.ini')

db = DB()

db.init()

print('connected to db')

if __name__ == '__main__':
    patient_resource_manager = FHIRPatientResourcesManager()
    patient_resource_manager.run()

    encounter_resource_manager = FHIREncounterResourceManager()
    encounter_resource_manager.run()

    procedure_manager = FHIRProcedureResourceManager()
    procedure_manager.run()

    observation_manager = FHIRObservationResourceManager()
    observation_manager.run()

end_time = datetime.now()
print('ended', end_time)
