import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import json
import threading
from queue import Queue
from PIL import Image
from io import BytesIO
import base64
import time
import sys
import os
from datetime import datetime


CAPTCHA_DIR = "captcha"
INDIR = "indir"
OUTDIR = "outdir"

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

def cek_validitas_ktp(input_data, session):
    url = (
        "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/c"
        "all_nik/{}".format(input_data["ktp"])
    )
    params = {"captchaAnswer": ""}
    url_captcha = (
        "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/p"
        "ublic/reloadCaptchaImg/"
    )
    count = 0
    while count <= 10:
        with session.get(url_captcha) as cpta:
            cpta_raw = cpta.text
            cpta_jsn = json.loads(cpta_raw)
            cpa = Image.open(BytesIO(base64.b64decode(cpta_jsn["captcha"])))
            cpa.show()
            img_txt = input("enter captcha for {}: ".format(input_data["ktp"]))
            params = {"captchaAnswer": img_txt}

        with session.get(url, params=params) as resp:
            content = resp.text
            try:
                lol = json.loads(content)
                captcha_dir = os.path.join(os.getcwd(), CAPTCHA_DIR)
                create_dir_ifn_exist(captcha_dir)
                if lol != 99:
                    json_cont = lol["content"]
                    if any("RESPONSE_DESC" in d for d in json_cont):
                        filename = os.path.join(captcha_dir, "{}.png".format(img_txt))
                        cpa.save(filename)
                        return None
                    only_key = [
                        d for d in json_cont if d['NIK'] == input_data["ktp"]
                    ]
                    filename = os.path.join(captcha_dir, "{}.png".format(img_txt))
                    cpa.save(filename)
                    return only_key[0]
                count += 1
                print("Wrong captcha")
            except json.decoder.JSONDecodeError:
                print("JSONDecodeError")
                login_to_site(session)
                count += 1
            except KeyError:
                print("KeyError")
                count += 1
            except TypeError:
                print("TypeError")
                count += 1


def do_check(q, list_data, session, length):
    counter = 0
    while True:
        sys.stdout.write("\r%d%%\r" % ((counter / length) * 100))
        sys.stdout.flush()
        to_get = q.get()
        per_ktp = cek_validitas_ktp(to_get, session)
        time.sleep(1)
        if per_ktp:
            list_data.append(per_ktp)
        counter += 1
        q.task_done()


def login_to_site(session):
    session.mount("https://portal-dukcapil.bpjsketenagakerjaan.go.id", HTTPAdapter(max_retries=5))
    session.get(
            "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/login"
    )
    with session.get(
        "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/publi"
        "c/captchaImg"
    ) as captcha:
        img = captcha.text
        img_jsn = json.loads(img)
        cpa = Image.open(BytesIO(base64.b64decode(img_jsn["captcha"])))
        cpa.show()
        img_txt = input("enter captcha: ")

    payload_login = {
        "username": "D06",
        "password": "BPJSTK123",
        "captchaAnswer": img_txt
    }
    session.post(
        "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/pos"
        "tlogin",
        data=payload_login
    )

def run(data, length):
    img_txt = ""
    list_data = []
    with requests.Session() as session:
        # Login
        login_to_site(session)
        q = Queue(maxsize=0)
        for i in range(1):
            t = threading.Thread(target=do_check, args=(
                q, list_data, session, length))
            t.setDaemon(True)
            t.start()
        for single in data:
            q.put(single)
        q.join()
    return list_data


def create_excel(datas):
    data = []
    for tk in datas:
        if tk:
            data.append(tk)
    df = pd.DataFrame(
        data,
        columns=[
            "NIK", "NO_KK", "NAMA_LGKP", "TMPT_LHR", "TGL_LHR",
            "JENIS_KLMIN", "STATUS_KAWIN", "GOL_DARAH", "NAMA_LGKP_IBU",
            "STAT_HBKEL", "ALAMAT", "NO_RT", "NO_RW",
            "KEC_NAME", "KEL_NAME", "KAB_NAME", "PROP_NAME",
            "JENIS_PKRJN"
        ]
    )
    df["NIK"] = df["NIK"].astype(str)
    df["NO_KK"] = df["NO_KK"].astype(str)
    df["TGL_LHR"] = pd.to_datetime(df["TGL_LHR"], format='%Y-%m-%d')
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    outname = os.path.join(
        outdir,
        "KTP_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    writer = pd.ExcelWriter(
        outname, engine="xlsxwriter",
        datetime_format='dd-mm-yyyy'
    )
    df.to_excel(
        writer, sheet_name="Merged", index=False,
    )
    writer.save()


if __name__ == "__main__":
    input_file = "new_adminduk_part.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    df = pd.read_excel(input_abs, 3)
    data_dict = df.to_dict('records')
    results = run(data_dict, df.size)
    create_excel(results)
