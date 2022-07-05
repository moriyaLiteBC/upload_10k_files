import concurrent
import datetime
import sys
import threading
import time
import traceback
import base64
from threading import Lock
import re

from ichor.model.recording1 import Recording1
from ichor.model.recording_variable import RecordingVariable

lock = threading.Lock()
import click as click
from concurrent.futures import ThreadPoolExecutor
import requests
import ichor
from ichor.api.patients_api import PatientsApi
from ichor.api.recordings_api import RecordingsApi
from ichor.api.files_api import FilesApi
from ichor.api.files_storage_api import FilesStorageApi
from ichor.model.recording import Recording
from ichor.model.patient import Patient
from ichor.model.file import File
from ichor.model.storage_multipart_request import StorageMultipartRequest
from ichor.api.recording_variables_api import RecordingVariablesApi
from ichor.model.storage_multipart_completion_request import StorageMultipartCompletionRequest
import io
import os
from typing import TypeVar, Callable
import pickle

import hashlib

PATIENT_PKL_FILE = 'patient.pkl'  # pickle for patients in database.
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

AVOID_WORK_HOURS = False

MAX_WORKERS_UPLOAD = 64
MAX_WORKERS_PARENT = +64

def load_ichor_configuration():
    global _ichor_api_client

    # os.environ['ICHOR_API_ENDPOINT'] = 'http://172.16.0.116:1234/'#'https://api.litebc.tech/v1/'
    # os.environ['ICHOR_API_ENDPOINT'] = 'https://api.litebc.tech/v1/'
    # os.environ['ICHOR_API_KEY'] = 'R88O+lhcSXTO/GShPM6OEA=='

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


lock_object = Lock()


def pickle_record_file(file_path, file_id):
    lock_object.acquire()
    with lock:
        files_record[file_path] = file_id
    with open(FILE_PKL_FILE, 'ab') as pkl:
        dic = {file_path: file_id}
        pickle.dump(dic, pkl)
    lock_object.release()


def unpickle_and_store_files_record():
    global files_record
    try:
        with open(FILE_PKL_FILE, 'rb') as pkl:
            while 1:
                try:

                    dict = pickle.load(pkl)
                    for key, value in dict.items():
                        with lock:
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
        return file_id
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


def is_recording_exist(recording_path):
    with lock:
        result = [int(v) for k, v in list(files_record.items()) if k.startswith(recording_path)]
    if not result:
        return None
    file = get_ichor_api(FilesApi).files_file_id_get(result[0])
    recording_id = file.parent_recording_id
    recording = get_ichor_api(RecordingsApi).recordings_recording_id_get(recording_id)
    return recording


def check_classification(file):
    def get_num():
        return re.findall('\d+', file)[0]

    if re.search("^Plane_\d+\.jpg$", file):
        return "LARGE_WIDE_Z", get_num()
    elif re.search("^Scan_\d+_\d+_\d+\.tif$", file):
        return "LARGE_WIDE_X_Y", None
    elif file == "Bright.avi":
        return "WIDE_VIDEO", None
    elif file == "configuration.txt":
        return "CONFIGURATION_FILE", None
    elif re.search("^LineCam\d+\.tif$", file):
        return "LINE_IMAGE", get_num()
    elif file == "wide_cam_timestamp.txt":
        return "WIDE_TIMESTAMP", None
    elif re.search("^Line_\d+\.tif$", file) or file == "LineScan_0.tif":
        return "PRE_LINE", None
    elif re.search("^FastScan_\d+\.tif$", file):
        return "FOCUS_WIDE", get_num()
    elif file == "line_cap.txt":
        return "PRE_LINE_RESULTS", None
    elif file == "best_image.tif":
        return "FOCUS_IMAGE_RESULT", None
    else:
        return "OTHER", None


executor = ThreadPoolExecutor(max_workers=MAX_WORKERS_UPLOAD)
parent_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS_PARENT)
jobs_to_do = []

SIZE_THRESHOLD = 1024 * 1024 * 10


def upload_file(path_file, file_id):
    file_id = file_id.file_id
    now = datetime.datetime.now()
    if (AVOID_WORK_HOURS and now.hour >= 7 and now.hour < 16 and now.weekday() != 5 and now.weekday() != 4):
        f = open(log_path, "a", encoding="utf-8")
        f.write("Day over, exiting " + str(now) + "\n")
        f.close()
        sys.exit(0)

    try:
        x = get_ichor_api(FilesStorageApi).files_storage_file_id_multipart_post(file_id=file_id,
                                                                                storage_multipart_request=StorageMultipartRequest())
    except Exception as e:
        print(":(")
    file_size = os.path.getsize(path_file)

    byte_size = file_size
    split = 1024 * 1024 * 9
    i = 0
    upload_id = x['upload_id']
    tag_results = {}

    configuration = ichor.Configuration(host=os.environ['ICHOR_API_ENDPOINT'],
                                        api_key={'ApiKeyAuth': os.environ['ICHOR_API_KEY']})
    jobs_to_do = []

    with open(path_file, 'rb') as f:
        # print( byte_size / (1 * split))
        while i * split < byte_size:
            f.seek(i * split)
            buffer = io.BytesIO(f.read(split))
            hasm = base64.b64encode(hashlib.md5(buffer.getbuffer()).digest()).decode('ascii')

            def run(ip, buffer, hash_md5, path):
                with ichor.ApiClient(configuration) as api:
                    print(f"{path}: i={ip}, bufferlen={buffer.getbuffer().nbytes}")

                    multipart_request = StorageMultipartRequest(upload_id=upload_id, request_part=ip + 1,
                                                                md5_hash=hash_md5)
                    res = get_ichor_api(FilesStorageApi) \
                        .files_storage_file_id_multipart_post(file_id=file_id,
                                                              storage_multipart_request=multipart_request)

                    s = requests.session()

                    url = res['request_part']['url']

                    res = requests.put(url, data=buffer, headers={'Content-MD5': str(hash_md5)})

                    res.close()
                    print(f"{path}: i={ip}, bufferlen={buffer.getbuffer().nbytes}, complete")

                    tag_results[ip + 1] = res.headers["ETag"]

            jobs_to_do.append(executor.submit(lambda k=i, z=buffer, p=hasm, j=path_file: run(k, z, p, j)))

            i += 1

    concurrent.futures.wait(jobs_to_do)
    for x in jobs_to_do:
        x.result()

    tags = []
    for i in sorted(tag_results.keys()):
        tags.append(tag_results[i])

    res = get_ichor_api(FilesStorageApi).files_storage_file_id_multipart_complete_post(file_id=file_id,
                                                                                       storage_multipart_completion_request=StorageMultipartCompletionRequest(
                                                                                           tags=tags,
                                                                                           upload_id=upload_id))
    if not res:
        raise Exception("Multipart completion failed.")


def append_to_log(file_path, file_id):
    f = open(log_path, "a", encoding="utf-8")
    f.write(file_path + "," + str(file_id) + "\r")
    f.close()


def load_files_from_log():
    global file_uploaded
    try:
        with open(log_path, 'r') as f:
            for line in f:
                # log_file = open(log_path, 'r', encoding="utf-8")
                # for line in log_file.readlines():
                splits = line.split(',')
                if len(splits) == 2:
                    file_uploaded[splits[0]] = int(splits[1])
    except Exception:
        return

def extract_z_x_ya(movie_dir_name):
    z,x,y = extract_z_x_y(movie_dir_name)
    a = movie_dir_name[-1]
    return [z, x, y, a]



def create_appropriate_recording(scans_and_find_planes_dir, scans_and_find_planes_path, patient,
                                 data_source):
    """create (in database) recording  all files - and upload"""
    if scans_and_find_planes_dir.startswith("FindPlane"):
        recording = is_recording_exist(scans_and_find_planes_path)
        if recording is None:
            creation_date = datetime.datetime.fromtimestamp(os.path.getctime(scans_and_find_planes_path))
            recording = get_ichor_api(RecordingsApi).recordings_post(Recording(patient_id=patient.patient_id,
                                                                               data_source=data_source,
                                                                               type="find_z_plane",
                                                                               date_created=creation_date))
        index = re.findall('\d+', scans_and_find_planes_dir)[0]
        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
            variable_name="Z_plane_index",
            recording_id=recording.recording_id,
            float_value=float(index)))

        create_files(scans_and_find_planes_path, recording)
    elif scans_and_find_planes_dir.startswith("scan"):
        recording = is_recording_exist(scans_and_find_planes_path)
        if recording is None:
            creation_date = datetime.datetime.fromtimestamp(os.path.getctime(scans_and_find_planes_path))
            recording = get_ichor_api(RecordingsApi).recordings_post(
                Recording(patient_id=patient.patient_id, data_source=data_source,
                          type="cap_plane_scan", date_created=creation_date))
        files_to_link = create_files(scans_and_find_planes_path, recording, is_cap_plane=True)
        try:
            get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                                                                       variable_name="scan_number",
                                                                       recording_id=recording.recording_id,
                                                                       float_value=float(re.findall('\d+', scans_and_find_planes_dir)[0])))
        except:
            print("variable scan_number for {} recording exist already".format(recording.recording_id))
        for movie in os.listdir(scans_and_find_planes_path):
            movie_path = os.path.join(scans_and_find_planes_path, movie)
            if not os.path.isdir(movie_path):
                continue
            recording = is_recording_exist(movie_path)
            creation_date = datetime.datetime.fromtimestamp(os.path.getctime(movie_path))
            if recording is None:
                recording = get_ichor_api(RecordingsApi).recordings_post(
                    Recording(patient_id=patient.patient_id, data_source=data_source,
                              type="full_capture", date_created=creation_date))
            z_x_ya= extract_z_x_ya(movie)
            try:
                get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                    variable_name="position_index",
                    recording_id=recording.recording_id,
                    json_value=z_x_ya))
            except:
                print("variable position_index for {} recording exist already".format(recording.recording_id))
            try:
                get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                    variable_name="scan_number",
                    recording_id=recording.recording_id,
                    float_value=float(re.findall('\d+', scans_and_find_planes_dir)[0])))
            except:
                print("variable scan_number for {} recording exist already".format(recording.recording_id))
            z, x, y = extract_z_x_y(movie)
            file_to_link_name = 'Scan_{}_{}_{}.tif'.format(z, x, y)
            file_id = files_to_link.get(file_to_link_name)
            while file_id is None:
                time.sleep(1)
                print("wait for " + file_to_link_name + " in recording: " + scans_and_find_planes_dir)
                file_id = files_to_link.get(file_to_link_name)
            get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                variable_name="link_to_scan",
                recording_id=recording.recording_id,
                file_value=file_id))
            create_files(movie_path, recording)

            # continue
            # if recording is not None:
            #    free_form_data = get_free_form_data_of_movie(movie, scans_and_find_planes_path)
            #    recording.free_form_data = free_form_data
            #    get_ichor_api(DataInstancesApi).recordings_recording_id_put(recording.recording_id, recording)


def get_md5(file_path):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)

        return file_hash.hexdigest()


def create_file_and_upload(scans_and_find_planes_path, file_path, recording, counter_seq=None, files_to_link=None):
    """create (in database) file and upload"""
    global file_uploaded
    # create file in file table.
    file_key_name = os.path.relpath(file_path, scans_and_find_planes_path).replace(
        "\\", "/")
    file = is_file_uploaded(file_path)
    if file is None:  # not in log file
        file = is_record_but_not_in_s3(file_path)
        if file is None:  # there is record but not in s3
            last_modified_date = datetime.datetime.strptime(time.ctime(os.path.getmtime(file_path)),
                                                            "%a %b %d %H:%M:%S %Y")
            created_date = datetime.datetime.strptime(time.ctime(os.path.getctime(file_path)),
                                                      "%a %b %d %H:%M:%S %Y")
            oldest = min([last_modified_date, created_date])
            classification = check_classification(os.path.basename(file_path))
            index = classification[1]
            if index is None:
                index = counter_seq
            file_size = os.path.getsize(file_path)
            created_file = File(file_created_date=oldest,
                                original_file_path=file_key_name,
                                classification=classification[0],
                                parent_recording_id=recording.recording_id,
                                file_size=file_size,
                                index=index,
                                file_bytes_uploaded=0,
                                md5=get_md5(file_path))
            # try care edge case of file that crash in upload to S3, so it insert to file table but
            # upload file to file table
            file = get_ichor_api(FilesApi).files_post(file=created_file)
            get_ichor_api(FilesApi).files_file_id_patch(file.file_id, {"original_file_path": file_path})
            if files_to_link is not None:
                files_to_link[os.path.basename(file_path)] = file.file_id
            pickle_record_file(file_path, file.file_id)
        # upload file to Amazon aws
        i = 0
        while i < 3:
            try:
                upload_file(file_path, file)
                break
            except Exception as e:
                with open("errors.txt", "a", encoding="utf-8") as f:
                    f.write(f"error {file_path}, try {(i + 1)}/3\n")
                    print("ERROR: " + str(e))
                i += 1
        append_to_log(file_path, file.file_id)
        file_uploaded[file_path] = file.file_id
    else:
        pass
        # print(f"File {file_path} uploaded.")


semaphor = threading.Semaphore(512)


def create_file_and_upload_wrapper(scans_and_find_planes_path, file_path, recording, counter_seq=None, files_to_link=None):
    # if not file_path.endswith(".avi"):
    #    return

    def t():
        try:
            create_file_and_upload(scans_and_find_planes_path, file_path, recording, counter_seq, files_to_link=files_to_link)
        except Exception as e:
            print(traceback.format_exc())
            exit()
        finally:
            semaphor.release()

    semaphor.acquire()
    parent_executor.submit(t)


def extract_z_x_y(file_name):
    result = re.findall(r'\d+', file_name)
    return int(result[0]), int(result[1]), int(result[2])


def extract_float_z_x_y(line):
    result = line.split("\t")
    return float(result[0]), float(result[1]), float(result[2])


def create_files(scans_and_find_planes_path, recording, is_cap_plane=False):
    """create (in database) and upload files of data instance"""
    file_dont_upload = ["cap_plane.txt", "midway.tif", "fullway.tif", "binary_image.jpg", "debug1.tif", "debug2.tif", "image_ang.jpg"]

    if not is_cap_plane:  # find_z_plane and full-capture
        pre_line_files = 0
        inside_pre_line_files = None
        for subdir, dirs, files in os.walk(scans_and_find_planes_path):
            for file_name in files:
                if file_name == "configuration.txt" and os.path.basename(
                        subdir) == "PreSequence":  # dont upload configuration file in PreSequence
                    continue
                elif file_name in file_dont_upload or re.search("^SmallImage_\d+\.tif$", file_name) or re.search("^stack\d+\.jpg$", file_name) or re.search("^Zn_\d+_\d+\.jpg$", file_name):
                  continue
                elif file_name == "configuration.txt":
                    file_path = os.path.join(subdir, file_name)
                    with open(file_path) as f:
                        lines = f.readlines()
                        data_str = re.findall(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', lines[0])[0]
                        date_created = datetime.datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")
                        get_ichor_api(RecordingsApi).recordings_recording_id_patch(recording_id=recording.recording_id,
                                                                                   recording1=Recording1(date_created=date_created))
                        floats_numbers = re.findall("\d+\.\d+" ,lines[2])
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="glass_position",
                            float_value=(float(floats_numbers[0]) + 1)
                        ))
                        exposure = re.findall("\d+", lines[3])[0]
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="exposure",
                            float_value=float(exposure)
                        ))
                        string_floats_xyz = re.findall("\d+\.\d+", lines[4])
                        floats_xyz = [float(i) for i in string_floats_xyz]
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="position_xyz",
                            json_value=floats_xyz
                        ))
                        continue
                elif file_name == "Line.txt":
                    file_path = os.path.join(subdir, file_name)
                    with open(file_path) as f:
                        lines = f.readlines()
                        result = [int(i) for i in lines]
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="line_timestamps",
                            json_value=result
                        ))
                    continue
                elif file_name == "wide_cam_timestamp.txt":
                    file_path = os.path.join(subdir, file_name)
                    with open(file_path) as f:
                        lines = f.readlines()
                        result = [int(i) for i in lines]
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="wide_timestamps",
                            json_value=result
                        ))
                    continue
                elif re.search("^Line_\d+\.tif", file_name):
                    inside_pre_line_files = subdir
                    pre_line_files += 1
                    continue
                elif file_name == "angle.txt":
                    file_path = os.path.join(subdir, file_name)
                    with open(file_path) as f:
                        lines = f.readlines()
                        angle_deg = re.findall("\d+\.\d+" ,lines[0])[0]
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="angle",
                            float_value=float(angle_deg)
                        ))
                    continue
                elif file_name == "fast_scan.txt":
                    file_path = os.path.join(subdir, file_name)
                    with open(file_path) as f:
                        lines = f.readlines()
                        floats_fast_scan = []
                        for i in range(int((len(lines)-2)/2)):
                            floats_fast_scan.append(float(re.findall("\d+\.\d+", lines[i])[0]))
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="focus_wide_positions",
                            json_value=floats_fast_scan
                        ))
                        focus_best_position = float(re.findall("\d+\.\d+", lines[len(lines)-1])[0])
                        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
                            recording_id=recording.recording_id,
                            variable_name="focus_best_position",
                            float_value=focus_best_position
                        ))
                    continue
                # elif file_name == "motors_position_file.txt":
                #     #TODO
                #     continue

                file_path = os.path.join(subdir, file_name)
                create_file_and_upload_wrapper(scans_and_find_planes_path, file_path, recording)
        if inside_pre_line_files:
            upload_index = int(pre_line_files/2)
            file_name = "Line_{}.tif".format(upload_index)
            file_path = os.path.join(inside_pre_line_files, file_name)
            create_file_and_upload_wrapper(scans_and_find_planes_path, file_path, recording)

    else:
        # cap_plane type is the only one that not recursive inside another directories
        files_to_link = {}
        counter = 0
        positions_index = {}
        positions = {}
        curr_z = -1
        count_num_line_in_scan_result = 0
        last_file_dont_exist = False
        counter_seq = 0
        for filename in os.listdir(scans_and_find_planes_path):
            file_path = os.path.join(scans_and_find_planes_path, filename)
            # checking if it is a file
            if os.path.isfile(file_path) and re.search("^Scan_\d+_\d+_\d+\.tif$", filename):
                create_file_and_upload_wrapper(scans_and_find_planes_path, file_path, recording,
                                               counter_seq=counter_seq, files_to_link=files_to_link)
                z, x, y = extract_z_x_y(filename)
                if z != curr_z:
                    curr_z = z
                    count_num_line_in_scan_result = 0
                    scan_positions_path = os.path.join(scans_and_find_planes_path, 'scan_positions_{}.txt'.format(curr_z))
                    try:
                        with open(scan_positions_path) as f:
                            lines = f.readlines()
                            print(scan_positions_path + " open!")
                    except FileNotFoundError:
                        last_file_dont_exist = True
                        print(scan_positions_path + " no exist!")
                else:
                    count_num_line_in_scan_result += 1
                positions_index[counter] = [z, x, y]
                if not last_file_dont_exist:
                    float_z, float_x, float_y = extract_float_z_x_y(lines[count_num_line_in_scan_result])
                else:
                    float_z, float_x, float_y = None, None, None
                positions[counter] = [float_z, float_x, float_y]
                counter += 1
                counter_seq += 1
                last_file_dont_exist = False
        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
            variable_name="positions_index", recording_id=recording.recording_id, json_value=positions_index))
        get_ichor_api(RecordingVariablesApi).recordings_variables_post(RecordingVariable(
            variable_name="positions", recording_id=recording.recording_id, json_value=positions))
        return files_to_link


def create_patient(patient_dir_path, data_source):
    """create (in database) patient and all data instance and all files - and upload"""
    patient_barcode = os.path.basename(patient_dir_path)
    patient = is_patient_exist(patient_barcode)
    patient_added = datetime.datetime.fromtimestamp(os.path.getctime(patient_dir_path))
    if patient is None:
        patient = get_ichor_api(PatientsApi).patients_post(Patient(external_identifier=patient_barcode,
                                                                   date_created=patient_added))
        pickle_patient(patient_barcode, patient.patient_id)
    for scans_and_find_planes_dir in os.listdir(patient_dir_path):
        scans_and_find_planes_path = os.path.join(patient_dir_path, scans_and_find_planes_dir)

        create_appropriate_recording(scans_and_find_planes_dir, scans_and_find_planes_path, patient,
                                     data_source)


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
