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
from ichor.model.s3_multipart_request import S3MultipartRequest
from ichor.model.s3_multipart_completion_request import S3MultipartCompletionRequest
import io
import os
from typing import TypeVar, Callable

T = TypeVar('T')
_ichor_api_client = None
_ichor_api_cache = {}
log_num_lines = 0


def load_ichor_configuration():
    global _ichor_api_client
    print('ICHOR_API_ENDPOINT: ' + os.environ['ICHOR_API_ENDPOINT'])
    print('ICHOR_API_KEY: ' + os.environ['ICHOR_API_KEY'])
    print()
    configuration = ichor.Configuration(host=os.environ['ICHOR_API_ENDPOINT'],
                                        api_key={'ApiKeyAuth': os.environ['ICHOR_API_KEY']})

    _ichor_api_client = ichor.ApiClient(configuration)
    _ichor_api_client.__enter__()


def get_ichor_api(api: Callable[[], T]) -> T:
    if api not in _ichor_api_cache:
        _ichor_api_cache[api] = api(_ichor_api_client)
    return _ichor_api_cache[api]


def is_patient_exist(patient_barcode):
    patients = get_ichor_api(PatientsApi).patients_get()
    for patient in patients:
        if patient.external_identifier == patient_barcode:
            return patient
    return None


def is_file_exist(file_path, log_path):
    try:
        log_file = open(log_path, 'r')
    except Exception:
        return None
    for line in log_file.readlines():
        splits = line.split(',')
        if splits[0] == file_path:
            file_id = int(splits[1])
            file = get_ichor_api(FilesApi).files_file_id_get(file_id)
            return file
    return None


def is_record_in_s3(file):
    import boto3
    s3 = boto3.resource("s3")
    key = file.s3_key
    bucket = file.s3_bucket
    try:
        s3.Object(bucket, key).load()
        return True
    except Exception:
        return False


def is_record_but_not_in_s3(file_path):
    files = get_ichor_api(FilesApi).files_get()
    for file in files:
        if file_path == file.original_file_path:
            if not is_record_in_s3(file):
                print("record in table but not in s3!\nfile ID: {}\nfile path: {}\n".format(file.file_id, file.original_file_path))
                return file
    return None


def is_data_instance_exist(data_instance_path, log_path):
    try:
        log_file = open(log_path, 'r')
    except Exception:
        return None
    for line in log_file.readlines():
        if line.startswith(data_instance_path):
            file_id = int(line.split(',')[1])
            file = get_ichor_api(FilesApi).files_file_id_get(file_id)
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
        # TODO: check other
        return "TBD4"


def upload_file(path_file, file_record):
    x = get_ichor_api(FilesAwsApi).files_aws_s3_file_id_multipart_post(file_id=file_record.file_id,
                                                                       s3_multipart_request=S3MultipartRequest())

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
            res = get_ichor_api(FilesAwsApi).files_aws_s3_file_id_multipart_post(file_id=file_record.file_id,
                                                                                 s3_multipart_request=S3MultipartRequest(
                                                                                     upload_id=x['upload_id'],
                                                                                     request_part=i + 1))
            url = res['request_part']['url']
            res = requests.Request('PUT', url, data=buffer).prepare()
            pretty_print_POST(res, i + 1)
            res = requests.Session().send(res)
            tags.append(res.headers["ETag"])
            i += 1
    get_ichor_api(FilesAwsApi).files_aws_s3_file_id_multipart_complete_post(file_id=file_record.file_id,
                                                                            s3_multipart_completion_request=S3MultipartCompletionRequest(
                                                                                tags=tags,
                                                                                upload_id=upload_id))
    print("finish upload {}!".format(str(file_record.file_id)))
    print('-----------END-----------', '\r\n\r\n')


def write_log(log_path, file_path, file_id):
    f = open(log_path, "a")
    f.write(file_path + "," + str(file_id) + "\r")
    f.close()


def is_file_in_log(log_path, file_path):
    with open(log_path) as f:
        if file_path in f.read():
            return True
        return False


def get_lines_count_in_file(file_path):
    try:
        with open(file_path, 'r') as fp:
            for count, line in enumerate(fp):
                pass
        return count
    except Exception:
        return 0


def create_patient(patient_dir_path, data_source, log_path):  # C:\Users\user\Desktop\test_upload_file\mesurement1\barcode-patient
    patient_barcode = os.path.basename(patient_dir_path)
    patient = is_patient_exist(patient_barcode)
    if patient is None:
        patient = get_ichor_api(PatientsApi).patients_post(patient=Patient(external_identifier=patient_barcode))
    for data_instance_dir in os.listdir(patient_dir_path):
        data_instance_path = os.path.join(patient_dir_path, data_instance_dir)
        data_instance = is_data_instance_exist(data_instance_path, log_path)
        if data_instance is None:
            data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
                data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
                                           type=''.join(
                                               [i for i in data_instance_dir.split('_', 1)[0].upper() if not i.isdigit()])))

        for subdir, dirs, files in os.walk(data_instance_path):
            for file_name in files:
                if file_name == "configuration.txt" and os.path.basename(
                        subdir) == "PreSequence":  # dont upload configuration file in PreSequnce
                    continue
                file_path = os.path.join(subdir, file_name)
                # create file in file table.
                file_key_name = os.path.relpath(os.path.join(subdir, file_name), data_instance_path).replace("\\", "/")
                file = is_file_exist(file_path, log_path)
                if file is None:
                    last_modified_date = datetime.datetime.strptime(time.ctime(os.path.getmtime(file_path)),
                                                                    "%a %b %d %H:%M:%S %Y")
                    created_date = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path)),
                                                              "%a %b %d %H:%M:%S %Y")
                    oldest = min([last_modified_date, created_date])
                    classification = check_classification(file_name)
                    file_size = os.path.getsize(file_path)
                    # TODO: user uploaded
                    user_uploaded = 1
                    created_file = File(file_created_date=oldest,
                                        original_file_path=file_key_name,
                                        classification=classification,
                                        parent_data_instance_id=data_instance.data_instance_id,
                                        file_size=file_size,
                                        file_bytes_uploaded=0,
                                        user_uploaded=user_uploaded)
                    file = is_record_but_not_in_s3(file_path)
                    if file is None:
                        # upload file to file table
                        file = get_ichor_api(FilesApi).files_post(file=created_file)
                        get_ichor_api(FilesApi).files_file_id_put(file.file_id, File(original_file_path=file_path))
                    # upload file to Amazon aws
                    upload_file(file_path, file)
                    write_log(log_path, file_path, file.file_id)
                else:
                    data_instance = get_ichor_api(DataInstancesApi).data_instances_data_instance_id_get(int(file.parent_data_instance_id))
                    patient = get_ichor_api(PatientsApi).patients_patient_id_get(data_instance.patient_id)
                    print("try upload file ID: {}, but its uploaded yet.\n in path: {}\{}\n".format(file.file_id, patient.external_identifier, file.original_file_path))

@click.group()
def main():
    pass


@main.command()
@click.argument('path')
@click.argument('data_source')
@click.argument('destination_path')
def upload(path, data_source, destination_path=r"C:\Users\user\Desktop\log.txt"):
    global log_num_lines
    log_num_lines = get_lines_count_in_file(destination_path)
    load_ichor_configuration()
    for measurement in os.listdir(path):  # iterate over all measurements
        measurement_path = os.path.join(path, measurement)
        for patient_barcode in os.listdir(measurement_path):
            patient_path = os.path.join(measurement_path, patient_barcode)
            create_patient(patient_path, data_source, destination_path)
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
    # python ./upload_10k_files.py upload "C:\Users\user\Desktop\test_upload_file" "10K"
    main()
    # upload(r"C:\Users\user\Desktop\test_upload_file", "10K")
