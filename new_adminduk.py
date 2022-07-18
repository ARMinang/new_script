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
import os
from datetime import datetime
from solve_captcha import captcha_detection
import argparse
from tqdm import tqdm
import difflib


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
            img_txt = captcha_detection(cpa)
            params = {"captchaAnswer": img_txt}

        with session.get(url, params=params) as resp:
            content = resp.text
            try:
                lol = json.loads(content)
                if lol != 99:
                    json_cont = lol["content"]
                    if any("RESPONSE_DESC" in d for d in json_cont):
                        return None
                    only_key = [
                        d for d in json_cont if d['NIK'] == input_data["ktp"]
                    ]
                    # input_data.pop("ktp")
                    similarity = difflib.SequenceMatcher(
                        None,only_key[0]["NAMA_LGKP"], input_data["reff_nama"]
                    ).ratio()*100
                    return {**only_key[0], **input_data, **{"similarity": similarity}}
                count += 1
            except json.decoder.JSONDecodeError:
                login_to_site(session)
                count += 1
            except (KeyError, TypeError) as e:
                print(e)
                count += 1


def do_check(q, list_data, session, length):
    with tqdm(total=length) as pbar:
        while True:
            to_get = q.get()
            pbar.set_description("Processing %s" % to_get["ktp"])
            per_ktp = cek_validitas_ktp(to_get, session)
            time.sleep(1)
            if per_ktp:
                list_data.append(per_ktp)
            pbar.update(1)
            q.task_done()


def login_to_site(session):
    count = 0
    while count < 10:
        try:
            header = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            session.mount("https://portal-dukcapil.bpjsketenagakerjaan.go.id", HTTPAdapter(max_retries=5))
            session.get(
                    "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/login",
                    headers=header
            )
            with session.get(
                "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/publi"
                "c/captchaImg"
            ) as captcha:
                img = captcha.text
                img_jsn = json.loads(img)
                cpa = Image.open(BytesIO(base64.b64decode(img_jsn["captcha"])))
                img_txt = captcha_detection(cpa)

            payload_login = {
                "username": "D00",
                "password": "BPJSTK123",
                "captchaAnswer": img_txt
            }
            session.post(
                "https://portal-dukcapil.bpjsketenagakerjaan.go.id/webportal/pos"
                "tlogin",
                headers=header,
                data=payload_login
            )
            return
        except requests.exceptions.ConnectionError as e:
            time.sleep(5)
            count += 1


def run(data, length):
    list_data = []
    with requests.Session() as session:
        # Login
        login_to_site(session)
        q = Queue(maxsize=0)
        for i in range(1):
            t = threading.Thread(target=do_check, args=(
                q, list_data, session, length), daemon=True)
            t.start()
        for single in data:
            q.put(single)
        q.join()
    return list_data


def create_excel(datas, outfile):
    data = []
    for tk in datas:
        if tk:
            data.append(tk)
    df = pd.DataFrame(
        data,
        columns=[
            "reff_kpj",	"reff_nama", "reff_tempat_lahir", "reff_tgl_lahir",
            "NIK", "NO_KK", "NAMA_LGKP", "TMPT_LHR", "TGL_LHR",
            "JENIS_KLMIN", "STATUS_KAWIN", "GOL_DARAH", "NAMA_LGKP_IBU",
            "STAT_HBKEL", "ALAMAT", "NO_RT", "NO_RW",
            "KEC_NAME", "KEL_NAME", "KAB_NAME", "PROP_NAME",
            "JENIS_PKRJN", "similarity"
        ]
    )
    df["reff_kpj"] = df["reff_kpj"].astype(str)
    df["NIK"] = df["NIK"].astype(str)
    df["NO_KK"] = df["NO_KK"].astype(str)
    df["TGL_LHR"] = pd.to_datetime(df["TGL_LHR"], format='%Y-%m-%d')
    # sort by similarity
    df.sort_values(by=['similarity'], ascending=False, inplace=True)
    writer = pd.ExcelWriter(
        outfile, engine="xlsxwriter",
        datetime_format='dd-mm-yyyy'
    )
    df.to_excel(
        writer, sheet_name="Merged", index=False,
    )
    # color scale on similarity
    worksheet = writer.sheets["Merged"]
    nrows = df.shape[0]
    worksheet.conditional_format('W1:W{}'.format(nrows+1), {'type': '3_color_scale'})
    writer.save()


if __name__ == "__main__":
    input_file = "new_adminduk_disduk.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    outname = os.path.join(
        outdir,
        "KTP_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    parser = argparse.ArgumentParser(description='Script untuk ambil data dari Portal Adminduk')
    parser.add_argument(
        '-sheet', type=int, default=0, help='Sheet index start from 0, default=0'
    )
    parser.add_argument(
        '--infile', type=str, help='input file xlsx', default=input_abs, action="store"
    )
    parser.add_argument(
        '--outfile', type=str, help='output file xlsx', default=outname, action="store"
    )
    args = parser.parse_args()
    start_time = time.time()
    df = pd.read_excel(
        args.infile,
        sheet_name=args.sheet
        # converters={'ktp': str}
    )
    data_dict = df.to_dict('records')
    results = run(data_dict, df.shape[0])
    create_excel(results, args.outfile)
    print("--- %s seconds ---" % (time.time() - start_time))
