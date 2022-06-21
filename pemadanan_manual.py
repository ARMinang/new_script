import pandas as pd
import asyncio
import aiohttp
from aiohttp import ClientSession as cs
import os
from bs4 import BeautifulSoup
import json5
import json
import random
from datetime import datetime
from itertools import groupby
from aiohttp import client_exceptions

INDIR = "indir"
OUTDIR = "outdir"


def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

async def get_tk_data(input_data, session):
    url = "http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn502755_lov_tk.php"
    payload = {
        'pilihsearch': "sc_kpj",
        'searchtxt': input_data['kpj'],
        'cari2': "GO"
    }
    count = 0
    while count < 5:
        try:
            async with session.post(url, data=payload) as resp:
                content = await resp.text()
                soup = BeautifulSoup(content, 'html.parser')
                table = soup.find(id="mydata")
                td = table.find_all('td')
                data_tk = json5.loads(td[0].a["onclick"][21:-58])
                if data_tk:
                    return data_tk
                data_tk = input_data
                data_tk["MSG"] = "Data Tidak ditemukan"
                return data_tk
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)
        except IndexError:
            ret_data = dict()
            ret_data["MSG"] = "Data Tidak ditemukan"
            return ret_data
        except ValueError:
            ret_data = dict()
            ret_data["MSG"] = "Data mengandung Apostrophe, silahkan lakukan pemadanan manual"
            return ret_data


async def cek_eligible(input_data, session):
    url = "http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn502755_popup_dukcapil.php"
    params = {
        'nik': input_data['NOMOR_IDENTITAS'],
        'nama_lengkap': input_data['NAMA_LENGKAP'],
        'tempat_lahir': input_data['TEMPAT_LAHIR'],
        'tgl_lahir': input_data['TGL_LAHIR'],
        'form': "NEW",
        "kode_agenda": "",
        'kode_tk': ""
    }
    count = 0
    while count < 5:
        try:
            async with session.get(url, params=params) as resp:
                content = await resp.text()
                soup = BeautifulSoup(content, 'html.parser')
                nama_lengkap = int(soup.find(id="span_nama_lengkap").span.string[:-2])
                tgl_lahir = int(soup.find(id="span_tgl_lahir").span.string[:-2])
                tempat_lahi = int(soup.find(id="span_tempat_lahi").span.string[:-2])
                eligible = soup.find(id="span_status").get_text()[2:]
                if eligible == "ELIGIBLE":
                    ret_value = dict()
                    ret_value["hdn_persen_nama_lengkap_adm"] = nama_lengkap
                    ret_value["hdn_persen_tempat_lahir_adm"] = tempat_lahi
                    ret_value["hdn_persen_tgl_lahir_adm"] = tgl_lahir
                    return ret_value
                return None
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)
    
async def cek_similarity(um_data, aju_data, session):
    url = "http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn502755_action.php"
    params = {
        'AJAX_TYPE': 'hitung_similarity_data_ajuan',
        'nomor_identitas_um': um_data['NOMOR_IDENTITAS'],
        'nama_lengkap_um': um_data['NAMA_LENGKAP'],
        'tgl_lahir_um': um_data['TGL_LAHIR'],
        'tempat_lahir_um': um_data['TEMPAT_LAHIR'],
        'nomor_identitas_aju': aju_data["NOMOR_IDENTITAS"],
        'nama_lengkap_aju': aju_data['NAMA_LENGKAP'],
        'tgl_lahir_aju': aju_data['TGL_LAHIR'],
        'tempat_lahir_aju': aju_data['TEMPAT_LAHIR'],
    }
    count = 0
    while count < 5:
        try:
            async with session.get(url, params=params) as resp:
                content = await resp.text()
                lines = content.splitlines()
                ret_value = dict()
                ret_value["hdn_persen_sim_nik"] = int(lines[0][54:-1])
                ret_value["hdn_persen_sim_nama_lengkap"] = int(lines[1][63:-1])
                ret_value["hdn_persen_sim_tgl_lahir"] = int(lines[2][60:-1])
                ret_value["hdn_persen_sim_tempat_lahir"] = int(lines[3][63:-1])
                return ret_value
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)

async def create_pp(session):
    url = "http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn5055_action.php?{}".format(random.random())
    payload = {
        'TYPE': "New",
        'lov_segmen': "PU",
        'tb_kode_perihal': "PP03",
        'tb_kode_perihal_detil': "PP0324",
        'tb_path_perihal': "kn502755.php",
        'tb_keterangan': "",
        'lov_sumber_informasi': "CABANG",
        'tb_kode_sumber_data': 3,
        'jml_doc': 0,
    }
    count = 0
    while count < 5:
        try:
            async with session.post(url, data=payload) as resp:
                content = await resp.text()
                pp = json5.loads(content)
                if pp["msg"] == "Agenda berhasil dibuat dan dokumen berhasil di upload!":
                    return pp["dataid"]
                return None
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)
        except IndexError:
            print("IndexError")
            count += 1
            await asyncio.sleep(5)

async def submit_pp(data, data_um, data_similarity, pp, session):
    url = "http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn502755_action.php?{}".format(random.random())
    payload = {
        "TYPE": "New",
        "DATAID": "",
        "gs_kode_segmen": "PU",
        "lov_sumber_informasi": "CABANG",
        "tb_kode_sumber_data": 3,
        "lov_segmen": "PU",
        "kd_agenda": pp,
        "tb_kode_perihal": "PP03",
        "tb_kode_perihal_detil": "PP0324",
        "tb_nama_perihal": "KOREKSI DATA TK",
        "detil_status": "",
        "tb_path_perihal": "kn502755.php",
        "tb_nama_perihal_detil": "PEMADANAN DATA INDIVIDU",
        "tb_keterangan": "",
        "kode_form": "",
        "task_form": "New",
        "txt_diajukan_ke_fungsi_approval": "Kepala Kantor Cabang Perintis", # "Kepala Bidang Pemasaran", # "Kepala Kantor Cabang Perintis",
        "kpj": data["kpj"],
        "nama_lengkap": data_um["NAMA_LENGKAP"],
        "hdn_kode_tk": data_um["KODE_TK"],
        "hdn_nomor_identitas": data_um["NOMOR_IDENTITAS"],
        "hdn_kpj": data["kpj"],
        "hdn_nama_lengkap": data_um["NAMA_LENGKAP"],
        "hdn_tempat_lahir": data_um["TEMPAT_LAHIR"],
        "hdn_tgl_lahir": data_um["TGL_LAHIR"],
        "hdn_jenis_kelamin": data_um["JENIS_KELAMIN"],
        "hdn_nama_ibu_kandung": data_um["NAMA_IBU_KANDUNG"],
        "hdn_nama_perusahaan": data_um["NAMA_PERUSAHAAN"],
        "hdn_alamat": data_um["ALAMAT"],
        "hdn_kode_kepesertaan": data_um["KODE_KEPESERTAAN"],
        "hdn_kode_perusahaan": data_um["KODE_PERUSAHAAN"],
        "hdn_kode_divisi": data_um["KODE_DIVISI"],
        "hdn_npp": data_um["NPP"],
        "hdn_no_mutasi_tk": data_um["NO_MUTASI_TK"],
        "hdn_jenis_identitas": data_um["JENIS_IDENTITAS"],
        "hdn_kode_segmen": data_um["KODE_SEGMEN"],
        "hdn_persen_nama_lengkap_adm": data_similarity["hdn_persen_sim_nama_lengkap"],
        "hdn_persen_tempat_lahir_adm": data_similarity["hdn_persen_sim_tgl_lahir"],
        "hdn_persen_tgl_lahir_adm": data_similarity["hdn_persen_sim_tempat_lahir"],
        "npp": data_um["NPP"],
        "kode_divisi": data_um["KODE_DIVISI"],
        "nama_perusahaan": data_um["NAMA_PERUSAHAAN"],
        "nomor_identitas_um": data_um["JENIS_IDENTITAS"],
        "nama_lengkap_um": data_um["NAMA_LENGKAP"],
        "tgl_lahir_um": data_um["TEMPAT_LAHIR"],
        "tempat_lahir_um": data_um["TEMPAT_LAHIR"],
        "hdn_persen_sim_nik": data_similarity["hdn_persen_sim_nik"],
        "hdn_persen_sim_nama_lengkap": data_similarity["hdn_persen_sim_nama_lengkap"],
        "hdn_persen_sim_tgl_lahir": data_similarity["hdn_persen_sim_tgl_lahir"],
        "hdn_persen_sim_tempat_lahir": data_similarity["hdn_persen_sim_tempat_lahir"],
        "nomor_identitas_aju": data["NOMOR_IDENTITAS"],
        "nama_lengkap_aju": data["NAMA_LENGKAP"],
        "tgl_lahir_aju": data["TGL_LAHIR"],
        "tempat_lahir_aju": data["TEMPAT_LAHIR"]
    }
    count = 0
    while count < 5:
        try:
            async with session.post(url, data=payload) as resp:
                content = await resp.text()
                sukses = json5.loads(content)
                ret_data = data.copy()
                ret_data["MSG"] = sukses["msg"]
                return ret_data
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)
        except IndexError:
            print("IndexError")
            count += 1
            await asyncio.sleep(5)

async def safe_download(data, session):
    sem = asyncio.Semaphore(10)
    async with sem:
        data_eli = False
        data_similarity = False
        pp = False
        data_process = await get_tk_data(data, session)
        if not 'MSG' in data_process:
            data_eli = await cek_eligible(data, session)
        if data_eli:
            data_similarity = await cek_similarity(data_process, data, session)
        if data_eli and data_similarity:
            pp = await create_pp(session)
        if data_eli and data_similarity and pp:
            return await submit_pp(data, data_process, data_similarity, pp, session)
        ret_data = data.copy()
        try:
            if data_process:
                ret_data["MSG"] = data_process['MSG']
        except KeyError:
            ret_data["MSG"] = "Something went wrong"
        return ret_data


async def run(data):
    tasks = []
    async with cs() as session:
        # payload_login = {
        #     "login": "SE112229",
        #     "password": "KAISAR0315" # KAISAR0315, ARIN0314
        # }
        # payload_role = "rule=9%7CD00"
        # query_role = {
        #     "role": "9|D00",
        #     "rolename": "PAP - Petugas Administrasi Peserta ( D00 )"
        # }
        payload_login = {
            "login": "AR188760",
            "password": "@ARIF111"
        }
        payload_role = "rule=27%7CD14"
        query_role = {
            "role": "27|D14",
            "rolename": "PMPPU - Penata Madya Pelayanan dan Umum ( D14 )"
        }
        # Login
        async with session.post(
            "http://smile.bpjsketenagakerjaan.go.id/smile/act/login.bpjs",
            data=payload_login
        ) as login:
            await login.text()
        # Set Role
        async with session.post(
            "http://smile.bpjsketenagakerjaan.go.id/smile/act/setrule.bpjs",
            data=payload_role,
            params=query_role
        ) as set_role:
            await set_role.text()
        for single in data:
            task = asyncio.ensure_future(safe_download(single, session))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results


def create_excel(data):
    df = pd.DataFrame(
        data,
        columns=[
            "kpj",
            "NOMOR_IDENTITAS",
            "NAMA_LENGKAP",
            "TGL_LAHIR",
            "TEMPAT_LAHIR",
            "MSG"
        ]
    )
    df["kpj"] = df["kpj"].astype(str)
    df["NOMOR_IDENTITAS"] = df["NOMOR_IDENTITAS"].astype(str)
    try:
        df["TGL_LAHIR"] = pd.to_datetime(
            df["TGL_LAHIR"],
            format="%d/%m/%Y"
        )
    except ValueError:
        print("ValueError")
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "PEMADANAN_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    with pd.ExcelWriter(
        save_filename,
        engine="xlsxwriter",
        datetime_format='dd-mm-yyyy',
    ) as writer:
        df.to_excel(
            writer, sheet_name="Lol",
            index=False
        )


if __name__ == "__main__":
    input_file = "test_pemadanan.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    df = pd.read_excel(
        input_abs
    )
    df["TGL_LAHIR"] = df["TGL_LAHIR"].dt.strftime("%d/%m/%Y")
    data_dict = df.to_dict('records')
    # data_dict = []
    # tk = dict()
    # tk["kpj"] = "22016164588"
    # tk["NOMOR_IDENTITAS"] = "1204060303940004"
    # tk["NAMA_LENGKAP"] = "YELISMAN LAIA"
    # tk["TGL_LAHIR"] = "03/03/1994"
    # tk["TEMPAT_LAHIR"] = "HILI'OTALUA"
    # data_dict.append(tk)
    result = asyncio.run(run(data_dict))
    if result:
        create_excel(result)
