import multiprocessing as mp
import configparser
from db import DB
from util.patient_manager import FHIRPatientResourcesManager
from util.encounter_manager import FHIREncounterResourceManager
from util.procedure_manager import FHIRProcedureResourceManager
from util.observation_manager import FHIRObservationResourceManager

config = configparser.ConfigParser()
config.read('config.ini')

db = DB(host=config['DB']['host'], port=config['DB']['port'],
        user=config['DB']['user'], password=config['DB']['password'], database=config['DB']['database'])

db.connect()
print('connected to db')

patient_resource_manager = FHIRPatientResourcesManager(db=db)
patient_resource_manager.run()

encounter_resource_manager = FHIREncounterResourceManager(db=db)
encounter_resource_manager.run()

procedure_manager = FHIRProcedureResourceManager(db=db)
procedure_manager.run()

observation_manager = FHIRObservationResourceManager(db=db)
observation_manager.run()
