from ast import Param
import pandas as pd
import asyncio
import aiohttp
from aiohttp import ClientSession as cs
import json
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from bs4 import BeautifulSoup
import json5

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
                return None
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)

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
                ret_value["hdn_persen_sim_nik"] = lines[0][54:-1]
                ret_value["hdn_persen_sim_nama_lengkap"] = lines[1][63:-1]
                ret_value["hdn_persen_sim_tgl_lahir"] = lines[2][60:-1]
                ret_value["hdn_persen_sim_tempat_lahir"] = lines[3][63:-1]
                return ret_value
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)

async def safe_download(data, session):
    sem = asyncio.Semaphore(10)
    async with sem:
        data_process = await get_tk_data(data, session)
        if data_process:
            data_eli = await cek_eligible(data, session)
            print(data_eli)
            if data_eli:
                data_similarity = await cek_similarity(data_process, data, session)
                print(data_similarity)


async def run(data):
    tasks = []
    async with cs() as session:
        payload_login = {
            "login": "SE112229",
            "password": "KAISAR0315"
        }
        payload_role = "rule=9%7CD00"
        query_role = {
            "role": "9|D00",
            "rolename": "PAP - Petugas Administrasi Peserta ( D00 )"
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


if __name__ == "__main__":
    # df = pd.read_excel(
    #     r"D:\Documents\cek_ktp_nama.xlsx",
    #     sheet_name=3
    # )
    # data_dict = df.to_dict('records')
    data_dict = []
    tk = dict()
    tk["kpj"] = "22032844734"
    tk["NOMOR_IDENTITAS"] = "1673041605830001"
    tk["NAMA_LENGKAP"] = "ERWIN"
    tk["TGL_LAHIR"] = "16/05/1983"
    tk["TEMPAT_LAHIR"] = "TANJUNG RAYA"
    data_dict.append(tk)
    result = asyncio.run(run(data_dict))
    
