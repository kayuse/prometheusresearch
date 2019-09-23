from multiprocessing import Process
import configparser
from db import DB
from db.manager import Report
from datetime import datetime
from util.patient_manager import FHIRPatientResourcesManager
from util.encounter_manager import FHIREncounterResourceManager
from util.procedure_manager import FHIRProcedureResourceManager
from util.observation_manager import FHIRObservationResourceManager
import sys, unittest


def get_day_of_week(val):
    day = None
    if val == 0:
        day = "Sunday"
    elif val == 1:
        day = "Monday"
    elif val == 2:
        day = "Tuesday"
    elif val == 3:
        day = "Wednessday"
    elif val == 4:
        day = "Thursday"
    elif val == 5:
        day = "Friday"
    elif val == 6:
        day = "Saturday"
    return day


def print_report():
    print("Reports ..................................\n")
    report = Report(db=db)
    total_patients = report.count_patient()
    total_encounters = report.count_encounter()
    total_procedure = report.count_procedure()
    total_observation = report.count_observation()

    report_info = "\t 1.) There are currently {} Patients, {} " \
                  "Encounters {} Procedures {} Observations Recorded \n"

    report_info = report_info.format(total_patients, total_encounters, total_procedure, total_observation)
    print(report_info)

    genders_report = report.no_of_patient_by_gender()
    gender_info = "\t 2.)Reports for Genders {} Males were recorded and {} Females were recorded \n".format(
        genders_report[0][1],
        genders_report[1][1])
    print(gender_info)

    procedure_type_count_info = "\t 3.)The top ten procedure counts are as follows "

    procedure_type_count = report.get_top_procedure_types(10)
    for i in procedure_type_count:
        procedure_type_count_info += "\t {} with {}\n".format(i[1], i[2])

    print(procedure_type_count_info)

    most_popular_day = get_day_of_week(report.get_most_popular_day()[0])
    least_popular_day = get_day_of_week(report.get_least_popular_day()[0])

    days_of_the_week_info = "\t 4.) The most popular day is {} \n         The least popular day is {} \n".format(
        most_popular_day,
        least_popular_day)
    print(days_of_the_week_info)


print('We began at ', datetime.now())
config = configparser.ConfigParser()
config.read('config.ini')

db = DB()

db.init()

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
    print('The processing program ended at ', end_time)

    print_report()
