import os
import re
import time
import urllib.parse
from urllib.parse import urljoin

import requests

from dpdispatcher import dlog

from .config import API_HOST, API_LOGGER_STACK_INFO, HTTP_TIME_OUT
from .retcode import RETCODE

try:
    import oss2
    from oss2 import SizedFileAdapter, determine_part_size
    from oss2.models import PartInfo
except ImportError:
    pass
ENABLE_STACK = True if API_LOGGER_STACK_INFO else False


class RequestInfoException(Exception):
    pass


class Client:
    def __init__(
        self, source_code, debug=False, base_url=API_HOST
    ):
        self.source_code = source_code
        self.debug = debug
        self.debug = os.getenv("LBG_CLI_DEBUG_PRINT", debug)
        self.base_url = base_url
        self.last_log_offset = 0

    def post(self, url, data=None, header=None, params=None, retry=5):
        return self._req(
            "POST", url, data=data, header=header, params=params, retry=retry
        )

    def get(self, url, header=None, params=None, retry=5):
        return self._req("GET", url, header=header, params=params, retry=retry)

    def _req(self, method, url, data=None, header=None, params=None, retry=5):
        short_url = url
        url = urllib.parse.urljoin(self.base_url, url)
        if header is None:
            header = {}
        header["sourceCode"] = self.source_code
        resp_code = None
        err = None
        for i in range(retry):
            resp = None
            try:
                if method == "GET":
                    resp = requests.get(url, params=params, headers=header)
                else:
                    if self.debug:
                        print(data)
                    resp = requests.post(url, json=data, params=params, headers=header)
            except Exception as e:
                dlog.error(f"request({i}) error {e}", i, stack_info=ENABLE_STACK)
                err = e
                time.sleep(1 * i)
                continue

            resp_code = resp.status_code
            if not resp.ok:
                if self.debug:
                    print(f"retry: {i},statusCode: {resp.status_code}")
                try:
                    result = resp.json()
                    err = result.get("error")
                except Exception:
                    pass
                time.sleep(0.1 * i)
                continue
            result = resp.json()
            if result["code"] == "0000" or result["code"] == 0:
                return result.get("data", {})
        raise RequestInfoException(resp_code, short_url, err)

    def _get_oss_bucket(self, endpoint, bucket_name):
        oss_ak = os.getenv("OSS_ACCESS_KEY", "")
        oss_sk = os.getenv("OSS_SECRET_KEY", "")
        oss_endpoint = os.getenv("OSS_ENDPOINT", "")
        oss_bucket = os.getenv("OSS_BUCKET", "")
        return oss2.Bucket(oss2.Auth(oss_ak, oss_sk), oss_endpoint,  oss_bucket)
        res = self.get("/data/get_sts_token", {})
        dlog.debug(f"debug: _get_oss_bucket: res:{res}")
        auth = oss2.StsAuth(
            res["AccessKeyId"], res["AccessKeySecret"], res["SecurityToken"]
        )
        return oss2.Bucket(auth, endpoint, bucket_name)

    def download(self, oss_file, save_file, endpoint, bucket_name):
        bucket = self._get_oss_bucket(endpoint, bucket_name)
        dlog.debug(f"debug: download: oss_file:{oss_file}; save_file:{save_file}")
        bucket.get_object_to_file(oss_file, save_file)
        return save_file

    def download_from_url(self, url, save_file):
        ret = None
        for retry_count in range(3):
            try:
                ret = requests.get(
                    url, stream=True
                )
            except Exception as e:
                dlog.error(f"request error {e}", stack_info=ENABLE_STACK)
                continue
            if ret.ok:
                break
            else:
                dlog.error(
                    f"request error status_code:{ret.status_code} reason: {ret.reason} body: \n{ret.text}"
                )
                time.sleep(retry_count)
                ret = None
        if ret is not None:
            ret.raise_for_status()
            with open(save_file, "wb") as f:
                for chunk in ret.iter_content(chunk_size=8192):
                    f.write(chunk)
            ret.close()

    def upload(self, oss_task_zip, zip_task_file, endpoint, bucket_name):
        dlog.debug(
            f"debug: upload: oss_task_zip:{oss_task_zip}; zip_task_file:{zip_task_file}"
        )
        bucket = self._get_oss_bucket(endpoint, bucket_name)
        total_size = os.path.getsize(zip_task_file)
        part_size = determine_part_size(total_size, preferred_size=1000 * 1024)
        upload_id = bucket.init_multipart_upload(oss_task_zip).upload_id
        parts = []
        with open(zip_task_file, "rb") as fileobj:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                result = bucket.upload_part(
                    oss_task_zip,
                    upload_id,
                    part_number,
                    SizedFileAdapter(fileobj, num_to_upload),
                )
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1
        # result = bucket.complete_multipart_upload(oss_task_zip, upload_id, parts)
        result = bucket.complete_multipart_upload(oss_task_zip, upload_id, parts)
        # print('debug:upload_result:', result, dir())
        return result

    def job_create(
        self, job_type, oss_path, input_data, program_id=None, group_id=None
    ):
        if input_data.get("source_code") != "Bohrium" and isinstance(oss_path, list):
            oss_path = oss_path[0]
        post_data = {
            "job_type": job_type,
            "oss_path": oss_path,
        }
        if program_id is not None:
            post_data["program_id"] = program_id
        if group_id is not None:
            post_data["job_group_id"] = group_id
        for k, v in input_data.items():
            post_data[k] = v
        if input_data.get("backward_files"):
            post_data["out_files"] = input_data.get("backward_files")
        if input_data.get("command"):
            post_data["cmd"] = input_data.get("command")
        if input_data.get("machine_type"):
            post_data["scass_type"] = input_data.get("machine_type")
        if input_data.get("source_code"):
            post_data["source_code"] = input_data.get("source_code")
        log = input_data.get(
            "logFiles", input_data.get("log_files", input_data.get("log_file"))
        )
        if log:
            if input_data.get("source_code") == "Bohrium" and isinstance(log, str):
                post_data["log_file"] = [log]
        if (
            "checkpoint_files" in post_data
            and post_data["checkpoint_files"] == "sync_files"
        ):
            post_data["checkpoint_files"] = ["*"]
        ret = self.post("/data/v2/insert_job", post_data)
        group_id = ret.get("job_group_id")
        return ret["job_id"], group_id

    def get_job_detail(self, job_id):
        try:
            ret = self.get(
                f"data/job/{job_id}",
            )
        except RequestInfoException as e:
            if e.args[0] != 200:
                raise e

            dlog.error(f"get job detail error {e}", stack_info=ENABLE_STACK)
            return None

        return ret

    def get_log(self, job_id):
        return ""

    def get_job_result_url(self, job_id):
        try:
            if not job_id:
                return None
            if "job_group_id" in job_id:
                ids = job_id.split(":job_group_id:")
                job_id, _ = int(ids[0]), int(ids[1])
            ret = self.get(f"/data/job/{job_id}", {})
            if "result_url" in ret and len(ret["result_url"]) != 0:
                return ret.get("result_url")
            else:
                return None
        except ValueError as e:
            dlog.error(e, stack_info=ENABLE_STACK)
            return None

    def kill(self, job_id):
        try:
            if not job_id:
                return None
            if "job_group_id" in job_id:
                ids = job_id.split(":job_group_id:")
                job_id, _ = int(ids[0]), int(ids[1])
            ret = self.post(f"/data/job/{job_id}/kill", {})
            return ret
        except ValueError as e:
            dlog.error(e, stack_info=ENABLE_STACK)
            return None
