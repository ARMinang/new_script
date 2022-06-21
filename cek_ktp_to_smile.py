import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
import json
import random
from datetime import datetime
import os

INDIR = "indir"
OUTDIR = "outdir"

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)


def generate_status_kawin(status_kawin=str):
    if status_kawin == "KAWIN":
        return "Y"
    else:
        return "T"


def generate_jenis_kelamin(jenis_kelamin=str):
    if jenis_kelamin == "PEREMPUAN" or jenis_kelamin == "P":
        return "P"
    else:
        return "L"


def get_kode_kabupaten(kabupaten=str):
    json_file = "kode_kabupaten.json"
    json_data = dict()
    json_data = json.loads(open(json_file).read())
    to_return = None
    for i in json_data:
        if i['nama_kabupaten'] == kabupaten:
            to_return = int(i['kode'])
            break
    return to_return


async def cek_validitas_ktp(input_data, session):
    url = (
        'http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn5000_'
        'cek_ws.php?{}'.format(random.random())
    )
    payload = {
        'TYPE': "NIK",
        'NIK': input_data['ktp'],
        'TYPE2': "",
        'KEYWORD2A': "",
        'KEYWORD2B': "",
        'KEYWORD2C': "",
        'SYNC': 0,
    }
    loop = 0
    while loop <= 10:
        async with session.post(url, data=payload) as resp:
            content = await resp.text()
            try:
                lol = json.loads(content)["return"]
                lol.pop("keluarga")
                lol.update(input_data)
                lol["kode_kabupaten"] = get_kode_kabupaten(lol["nilaiNamaKab"])
                lol["jenis_kelamin"] = generate_jenis_kelamin(lol["nilaijenisKelamin"])
                lol["status_kawin"] = generate_status_kawin(lol["nilaiStatKwn"])
                lol["gol_darah"] = "O"
                lol["kode_negara"] = "ID"
                lol["jenis_identitas"] = "KTP"
                lol["masa_laku_identitas"] = "01-01-2025"
                lol["npwp"] = 0
                lol["hp"] = "08012345623"
                if lol["nilaitanggalLahir"] is not None:
                    lol["nilaitanggalLahir"] = datetime.strptime(
                        lol["nilaitanggalLahir"], "%m/%d/%Y"
                    )
                if lol["msg"] != "Sukses":
                    lol["nilainamaLengkap"] = input_data["nama"]
                    lol["nilaitempatLahir"] = "None"
                    lol["nilaitanggalLahir"] = input_data["tgl_lahir"]
                    lol["nilainamaIbu"] = "None"
                    lol["nilaiAlamat"] = "None"
                    lol["kode_kabupaten"] = "1471"
                if lol["nilainamaIbu"]:
                    if len(lol["nilainamaIbu"]) < 4:
                        lol["nilainamaIbu"] = None
                return lol
            except json.decoder.JSONDecodeError:
                print(content)
                loop += 1
                await asyncio.sleep(5)
            except KeyError:
                print(content)
                loop += 1
                await asyncio.sleep(5)


async def safe_download(data, session):
    sem = asyncio.Semaphore(3)
    async with sem:
        return await cek_validitas_ktp(data, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for single in data:
            task = asyncio.ensure_future(safe_download(single, session))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results


def create_excel(datas):
    data = []
    for tk in datas:
        if tk:
            data.append(tk)
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "TO_SMILE_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    df = pd.DataFrame(
        data,
        columns=[
            "nama", "tgl_lahir", "nik", "nilainamaLengkap", "nilaitempatLahir",
            "nilaitanggalLahir", "jenis_kelamin", "status_kawin",
            "gol_darah", "kode_negara", "jenis_identitas", "ktp",
            "masa_laku_identitas", "npwp", "upah", "nilainamaIbu",
            "nilaiAlamat", "kode_kabupaten", "hp"
        ],
    )
    df["ktp"] = df["ktp"].astype(str)
    try:
        df["nilaitanggalLahir"] = pd.to_datetime(
            df["nilaitanggalLahir"],
            format="%m/%d/%Y"
        )
    except ValueError:
        print("ValueError")
    with pd.ExcelWriter(
       save_filename, engine="xlsxwriter", 
       datetime_format='dd-mm-yyyy'
    ) as writer:
        df.to_excel(
            writer, sheet_name="Sheet1", index=False,
        )


if __name__ == "__main__":
    input_file = "to_smile.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    df = pd.read_excel(
        input_abs,
        sheet_name=0
    )
    df['ktp'] = df['ktp'].astype('int64')
    data_dict = df.to_dict('records')
    results = asyncio.run(run(data_dict))
    create_excel(results)
