import datetime
import time
import requests
import ichor
from ichor.api.files_aws_api import FilesAwsApi
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


def load_ichor_configuration():
    global _ichor_api_client
    print(os.environ['ICHOR_API_ENDPOINT'])
    print(os.environ['ICHOR_API_KEY'])
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
    if patient_barcode in patients:
        return True
    return False


def check_classification(file):
    if file == "Bright.avi":
        return "WIDE_IMAGE"
    elif file == "configuration.txt":
        return "CONFIGURATION_FILE"
    elif file.startswith("Line"):
        return "LINE_IMAGE"
    elif file.endswith("timestamp.txt"):
        return "TIME_STAMP"
    elif file == "motors_position_file.txt":
        return "MOTORS_POSITIONS"
    elif file.startswith("scan_positions"):
        return "META_DATA"
    else:
        return "META_DATA"


def upload_file(path_file, file_record):
    x = get_ichor_api(FilesAwsApi).files_aws_s3_file_id_multipart_post(file_id=file_record.file_id,
                                                                       s3_multipart_request=S3MultipartRequest())

    byte_size = file_record.file_size
    split = 1024 * 1024 * 10
    i = 0
    upload_id = x['upload_id']
    tags = []

    def pretty_print_POST(req):
        """
        At this point it is completely built and ready
        to be fired; it is "prepared".

        However pay attention at the formatting used in
        this function because it is programmed to be pretty
        printed and may differ from the actual request.
        """
        print('{}\n{}\r\n{}\r\n\r\n'.format(
            '-----------START-----------',
            req.method + ' ' + req.url,
            '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items())
        ))

    with open(path_file, 'rb') as f:
        while i * split < byte_size:
            f.seek(i * split)
            buffer = io.BytesIO(f.read(split))
            res = get_ichor_api(FilesAwsApi).files_aws_s3_file_id_multipart_post(file_id=file_record.file_id,
                                                                                 s3_multipart_request=S3MultipartRequest(
                                                                                     upload_id=x['upload_id'],
                                                                                     request_part=i + 1))
            url = res['request_part']['url']
            print("url: " + str(url))
            res = requests.Request('PUT', url, data=buffer).prepare()
            pretty_print_POST(res)
            res = requests.Session().send(res)
            tags.append(res.headers["ETag"])
            i += 1
    r = get_ichor_api(FilesAwsApi).files_aws_s3_file_id_multipart_complete_post(file_id=file_record.file_id,
                                                                                s3_multipart_completion_request=S3MultipartCompletionRequest(
                                                                                    tags=tags,
                                                                                    upload_id=upload_id))
    print("finish upload {}!".format(str(file_record.file_id)))


def create_patient(patient_dir_path, data_source):  # C:\Users\user\Desktop\test_upload_file\mesurement1\barcode-patient
    patient_barcode = os.path.basename(patient_dir_path)
    if not is_patient_exist(patient_barcode):
        patient = get_ichor_api(PatientsApi).patients_post(patient=Patient(external_identifier=patient_barcode))
        for data_instance_dir in os.listdir(patient_dir_path):
            data_instance = get_ichor_api(DataInstancesApi).data_instances_post(
                data_instance=DataInstance(patient_id=patient.patient_id, data_source=data_source,
                                           type=data_instance_dir.split('_', 1)[0].upper()))
            data_instance_path = os.path.join(patient_dir_path, data_instance_dir)
            for subdir, dirs, files in os.walk(data_instance_path):
                for file_name in files:
                    if file_name == "configuration.txt" and os.path.basename(
                            subdir) == "PreSequence":  # dont upload configuration file in PreSequnce
                        continue
                    file_path = os.path.join(subdir, file_name)
                    # create file in file table.
                    file_key_name = os.path.relpath(os.path.join(subdir, file_name), data_instance_path)
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
                                                                        user_uploaded=user_uploaded,
                                                                        )
                    file = get_ichor_api(FilesApi).files_post(file=created_file)
                    # upload file to Amazon aws
                    upload_file(file_path, file)


if __name__ == '__main__':
    path = r"C:\Users\user\Desktop\test_upload_file"
    load_ichor_configuration()

    for measurement in os.listdir(path):  # iterate over all measurements
        measurement_path = os.path.join(path, measurement)
        for patient_barcode in os.listdir(measurement_path):
            patient_path = os.path.join(measurement_path, patient_barcode)
            create_patient(patient_path, "10k")
