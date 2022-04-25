import datetime
import time
import click as click
import requests
import ichor
from ichor.api.patients_api import PatientsApi
from ichor.api.data_instances_api import DataInstancesApi
from ichor.api.files_api import FilesApi
from ichor.api.files_aws_api import FilesAwsApi
from ichor.model.data_instance import DataInstance
from ichor.model.patient import Patient
from ichor.model.file import File
from ichor.model.storage_multipart_request import StorageMultipartRequest
from ichor.model.storage_multipart_completion_request import StorageMultipartCompletionRequest
import io
import os
from typing import TypeVar, Callable
import pickle

PATIENT_PKL_FILE = 'patient.pkl' # pickle for patients in database.
FILE_PKL_FILE = 'file.pkl'  # pickle for files in database
# key: barcode, value: id
patient_uploaded = {}
# key: path , value: id
file_uploaded = {}
# key: path , value: id
files_record = {}

T = TypeVar('T')
_ichor_api_client = None
_ichor_api_cache = {}
log_path = "log.txt"


def load_ichor_configuration():
    global _ichor_api_client
    print('ICHOR_API_ENDPOINT: ' + os.environ['ICHOR_API_ENDPOINT'])
    print('ICHOR_API_KEY: ' + os.environ['ICHOR_API_KEY'])
    print()
    # configuration = ichor.Configuration(host=os.environ['ICHOR_API_ENDPOINT'],
    #                                     api_key={'ApiKeyAuth': os.environ['ICHOR_API_KEY']})
    configuration = ichor.Configuration(host="http://172.16.0.111:1234",
                                        api_key={'ApiKeyAuth': "XOKAexeM9L/5JYt1u0gf0A=="})

    _ichor_api_client = ichor.ApiClient(configuration)
    _ichor_api_client.__enter__()


def get_ichor_api(api: Callable[[], T]) -> T:
    if api not in _ichor_api_cache:
        _ichor_api_cache[api] = api(_ichor_api_client)
    return _ichor_api_cache[api]


def pickle_patient(patient_barcode, patient_id):
    patient_uploaded[patient_barcode] = patient_id
    with open(PATIENT_PKL_FILE, 'ab') as pkl:
        dic = {patient_barcode: patient_id}
        pickle.dump(dic, pkl)


def unpickle_and_store_patients():
    global patient_uploaded
    try:
        with open(PATIENT_PKL_FILE, 'rb') as pkl:
            while 1:
                try:
                    dict = pickle.load(pkl)
                    for key, value in dict.items():
                        patient_uploaded[key] = value
                except EOFError:
                    break
    except Exception:
        return []


def pickle_record_file(file_path, file_id):
    files_record[file_path] = file_id
    with open(FILE_PKL_FILE, 'ab') as pkl:
        dic = {file_path: file_id}
        pickle.dump(dic, pkl)


def unpickle_and_store_files_record():
    global files_record
    try:
        with open(FILE_PKL_FILE, 'rb') as pkl:
            while 1:
                try:
                    dict = pickle.load(pkl)
                    for key, value in dict.items():
                        files_record[key] = value
                except EOFError:
                    break
    except Exception:
        return {}


def is_patient_exist(patient_barcode):
    if patient_barcode in patient_uploaded:
        patient = get_ichor_api(PatientsApi).patients_patient_id_get(patient_uploaded[patient_barcode])
        return patient
    return None


def is_file_uploaded(file_path):
    if file_path in file_uploaded:
        file_id = file_uploaded[file_path]
        file = get_ichor_api(FilesApi).files_file_id_get(file_id)
        return file
    return None


def is_record_but_not_in_s3(file_path):
    """check if file in database but not in s3"""
    if file_path in files_record and file_path not in file_uploaded:
        file_id = files_record[file_path]
        file = get_ichor_api(FilesApi).files_file_id_get(file_id)
        print("record in table but not in s3!\nfile ID: {}\nfile path: {}\n".format(file.file_id,
                                                                                    file.original_file_path))
        return file
    return None


def is_data_instance_exist_from_uploaded_file(data_instance_path):
    result = [int(v) for k, v in file_uploaded.items() if k.startswith(data_instance_path)]
    if not result:
        return None
    file = get_ichor_api(FilesApi).files_file_id_get(result[0])
    data_instance_id = file.parent_data_instance_id
    data_instance = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_get(data_instance_id)
    return data_instance


def check_classification(file):
    if file == "Bright.avi":
        return "WIDE_IMAGE"
    elif file == "configuration.txt":
        return "CONFIGURATION_FILE"
    elif file.startswith("LineCam"):
        return "LINE_IMAGE"
    elif file.endswith("timestamp.txt"):
        return "TIME_STAMP"
    elif file == "motors_position_file.txt":
        return "MOTORS_POSITIONS"
    else:
        return "OTHER"


def upload_file(path_file, file_record):
    """upload file to amazon (without create record)"""
    x = get_ichor_api(FilesAwsApi).files_aws_file_id_multipart_post(file_id=file_record.file_id,
                                                                    storage_multipart_request=StorageMultipartRequest())

    byte_size = file_record.file_size
    split = 1024 * 1024 * 10
    i = 0
    upload_id = x['upload_id']
    tags = []

    def pretty_print_POST(req, part_number):
        """
        At this point it is completely built and ready
        to be fired; it is "prepared".

        However pay attention at the formatting used in
        this function because it is programmed to be pretty
        printed and may differ from the actual request.
        """
        print("upload part: " + str(part_number))
        print('{}\r\n{}\r\n'.format(
            req.method + ' ' + req.url,
            '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items())
        ))

    print('-----------START-----------')
    with open(path_file, 'rb') as f:
        while i * split < byte_size:
            f.seek(i * split)
            buffer = io.BytesIO(f.read(split))
            res = get_ichor_api(FilesAwsApi).files_aws_file_id_multipart_post(file_id=file_record.file_id,
                                                                              storage_multipart_request=StorageMultipartRequest(
                                                                                  upload_id=x['upload_id'],
                                                                                  request_part=i + 1))
            url = res['request_part']['url']
            res = requests.Request('PUT', url, data=buffer).prepare()
            pretty_print_POST(res, i + 1)
            res = requests.Session().send(res)
            tags.append(res.headers["ETag"])
            i += 1
    get_ichor_api(FilesAwsApi).files_aws_file_id_multipart_complete_post(file_id=file_record.file_id,
                                                                         storage_multipart_completion_request=StorageMultipartCompletionRequest(
                                                                             tags=tags,
                                                                             upload_id=upload_id))
    print("finish upload {}!".format(str(file_record.file_id)))
    print('-----------END-----------', '\r\n\r\n')


def append_to_log(file_path, file_id):
    f = open(log_path, "a")
    f.write(file_path + "," + str(file_id) + "\r")
    f.close()


def load_files_from_log():
    global file_uploaded
    try:
        log_file = open(log_path, 'r')
        for line in log_file.readlines():
            splits = line.split(',')
            file_uploaded[splits[0]] = int(splits[1])
    except Exception:
        return


def get_free_form_data_of_movie(movie_name, up_path):
    def extract_x_y_z(dir_name):
        import re
        result = re.findall(r'\d+', dir_name)
        return result[0], result[1], result[2]

    x, y, z = extract_x_y_z(movie_name)
    file_path = os.path.join(up_path, "Scan_{}_{}_{}.tif".format(x, y, z))
    file_id = file_uploaded[file_path]
    free_form_data = {"file_belongs": file_id}
    return free_form_data


def create_appropriate_data_instance(scans_and_find_planes_dir, scans_and_find_planes_path, patient,
                                     data_source):
    """create (in database) data_instance  all files - and upload"""
    if scans_and_find_planes_dir.startswith("FindPlane"):
        data_instance = is_data_instance_exist_from_uploaded_file(scans_and_find_planes_path)
        if data_instance is None:
            data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
                data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
                                           type="find_z_plane"))
        create_files(scans_and_find_planes_path, data_instance)
    elif scans_and_find_planes_dir.startswith("scan"):
        data_instance = is_data_instance_exist_from_uploaded_file(scans_and_find_planes_path)
        if data_instance is None:
            data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
                data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
                                           type="cap_plane_scan"))
        create_files(scans_and_find_planes_path, data_instance, reqursive=False)
        for movie in os.listdir(scans_and_find_planes_path):
            movie_path = os.path.join(scans_and_find_planes_path, movie)
            if not os.path.isdir(movie_path):
                continue
            free_form_data = get_free_form_data_of_movie(movie, scans_and_find_planes_path)
            if not os.path.isfile(os.path.join(movie_path, "LineCam0.tif")):
                data_instance = is_data_instance_exist_from_uploaded_file(scans_and_find_planes_path)
                if data_instance is None:
                    data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
                        data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
                                                   type="wide_only_capture", free_form_data=free_form_data))
                create_files(movie_path, data_instance)
            else:
                data_instance = is_data_instance_exist_from_uploaded_file(scans_and_find_planes_path)
                if data_instance is None:
                    data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
                        data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
                                                   type="full_capture", free_form_data=free_form_data))
                create_files(movie_path, data_instance)


def create_file_and_upload(scans_and_find_planes_path, file_path, data_instance):
    """create (in database) file and upload"""
    global file_uploaded
    # create file in file table.
    file_key_name = os.path.relpath(file_path, scans_and_find_planes_path).replace(
        "\\", "/")
    file = is_file_uploaded(file_path)
    if file is None:   # not in log file
        file = is_record_but_not_in_s3(file_path)
        if file is None:  # there is record but not in s3
            last_modified_date = datetime.datetime.strptime(time.ctime(os.path.getmtime(file_path)),
                                                            "%a %b %d %H:%M:%S %Y")
            created_date = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path)),
                                                      "%a %b %d %H:%M:%S %Y")
            oldest = min([last_modified_date, created_date])
            classification = check_classification(os.path.basename(file_path))
            file_size = os.path.getsize(file_path)
            created_file = File(file_created_date=oldest,
                                original_file_path=file_key_name,
                                classification=classification,
                                parent_data_instance_id=data_instance.data_instance_id,
                                file_size=file_size,
                                file_bytes_uploaded=0)
            # try care edge case of file that crash in upload to S3, so it insert to file table but
            # upload file to file table

            file = get_ichor_api(FilesApi).files_post(file=created_file)
            get_ichor_api(FilesApi).files_file_id_put(file.file_id, File(original_file_path=file_path))
            pickle_record_file(file_path, file.file_id)
        # upload file to Amazon aws
        upload_file(file_path, file)
        append_to_log(file_path, file.file_id)
        file_uploaded[file_path] = file.file_id
    else:
        # record and in s3
        data_instance = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_get(
            int(file.parent_data_instance_id))
        patient = get_ichor_api(PatientsApi).patients_patient_id_get(data_instance.patient_id)
        print("try upload file ID: {}, but its uploaded yet.\n in path: {}\{}\n".format(file.file_id,
                                                                                            patient.external_identifier,
                                                                                            file.original_file_path))


def create_files(scans_and_find_planes_path, data_instance, reqursive=True):
    """create (in database) and upload files of data instance"""
    if reqursive:
        for subdir, dirs, files in os.walk(scans_and_find_planes_path):
            for file_name in files:
                if file_name == "configuration.txt" and os.path.basename(
                        subdir) == "PreSequence":  # dont upload configuration file in PreSequnce
                    continue
                file_path = os.path.join(subdir, file_name)
                create_file_and_upload(scans_and_find_planes_path, file_path, data_instance)

    else:
        for filename in os.listdir(scans_and_find_planes_path):
            file_path = os.path.join(scans_and_find_planes_path, filename)
            # checking if it is a file
            if os.path.isfile(file_path):
                create_file_and_upload(scans_and_find_planes_path, file_path, data_instance)


def create_patient(patient_dir_path, data_source):
    """create (in database) patient and all data instance and all files - and upload"""
    patient_barcode = os.path.basename(patient_dir_path)
    patient = is_patient_exist(patient_barcode)
    if patient is None:
        patient = get_ichor_api(PatientsApi).patients_post(patient=Patient(external_identifier=patient_barcode))
        pickle_patient(patient_barcode, patient.patient_id)
    for scans_and_find_planes_dir in os.listdir(patient_dir_path):
        scans_and_find_planes_path = os.path.join(patient_dir_path, scans_and_find_planes_dir)

        create_appropriate_data_instance(scans_and_find_planes_dir, scans_and_find_planes_path, patient,
                                         data_source)

        # data_instance = is_data_instance_exist(scans_and_find_planes_path)
        # if data_instance is None:
        #     data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
        #         data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
        #                                    type=''.join(
        #                                        [i for i in scans_and_find_planes_dir.split('_', 1)[0].upper() if
        #                                         not i.isdigit()])))
        #
        # for subdir, dirs, files in os.walk(scans_and_find_planes_path):
        #     for file_name in files:
        #         if file_name == "configuration.txt" and os.path.basename(
        #                 subdir) == "PreSequence":  # dont upload configuration file in PreSequnce
        #             continue
        #         file_path = os.path.join(subdir, file_name)
        #         # create file in file table.
        #         file_key_name = os.path.relpath(os.path.join(subdir, file_name), scans_and_find_planes_path).replace("\\", "/")
        #         file = is_file_exist(file_path)
        #         if file is None:
        #             last_modified_date = datetime.datetime.strptime(time.ctime(os.path.getmtime(file_path)),
        #                                                             "%a %b %d %H:%M:%S %Y")
        #             created_date = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path)),
        #                                                       "%a %b %d %H:%M:%S %Y")
        #             oldest = min([last_modified_date, created_date])
        #             classification = check_classification(file_name)
        #             file_size = os.path.getsize(file_path)
        #             created_file = File(file_created_date=oldest,
        #                                 original_file_path=file_key_name,
        #                                 classification=classification,
        #                                 parent_data_instance_id=data_instance.data_instance_id,
        #                                 file_size=file_size,
        #                                 file_bytes_uploaded=0)
        #             # try care edge case of file that crash in upload to S3, so it insert to file table but
        #             file = is_record_but_not_in_s3(file_path)
        #             if file is None:
        #                 # upload file to file table
        #                 file = get_ichor_api(FilesApi).files_post(file=created_file)
        #                 get_ichor_api(FilesApi).files_file_id_put(file.file_id, File(original_file_path=file_path))
        #             # upload file to Amazon aws
        #             upload_file(file_path, file)
        #             write_log(file_path, file.file_id)
        #         else:
        #             data_instance = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_get(
        #                 int(file.parent_data_instance_id))
        #             patient = get_ichor_api(PatientsApi).patients_patient_id_get(data_instance.patient_id)
        #             print("try upload file ID: {}, but its uploaded yet.\n in path: {}\{}\n".format(file.file_id,
        #                                                                                             patient.external_identifier,
        #                                                                                             file.original_file_path))


@click.group()
def main():
    pass


@main.command()
@click.argument('path')
@click.option('--data_source', default="10k", help='test location')
@click.option('--destination_path', default="log.txt", help='destination log file')
def upload(path, data_source, destination_path):
    unpickle_and_store_patients()
    load_files_from_log()
    unpickle_and_store_files_record()
    load_ichor_configuration()
    for measurement in os.listdir(path):  # iterate over all measurements
        measurement_path = os.path.join(path, measurement)
        for patient_barcode in os.listdir(measurement_path):
            patient_path = os.path.join(measurement_path, patient_barcode)
            create_patient(patient_path, data_source)
    print_done()


def print_done():
    print("          DONE! ")
    print("      |         |         ")
    print("      |         |         ")
    print("      |         |         ")
    print("|     |         |       |")
    print("|                       |")
    print(" \                     / ")
    print("  \                   / ")
    print("   \                 / ")
    print("    \               / ")
    print("     \_____________/ ")


if __name__ == '__main__':
    # command for upload from path:
    # python ./upload_10k_files.py upload "C:\Users\user\Desktop\test_upload_file"
    main()
    # upload(r"C:\Users\user\Desktop\test_upload_file", "10K")
