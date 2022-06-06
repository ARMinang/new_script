import pandas as pd
import asyncio
import aiohttp
from aiohttp import ClientSession as cs
import json
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

INDIR = "indir"
OUTDIR = "outdir"


def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

async def cek_validitas_ktp(input_data, session):
    url = (
        'http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax'
        '/kn5000_query.php?{}'.format(random.random())
    )
    payload = {
        'TYPE': "NAMA_LENGKAP",
        'KEYWORD': input_data['nama'],
        'TYPE2': "TGL_LAHIR",
        'KEYWORD2A': input_data['tgl_lahir'].strftime("%d/%m/%Y"),
        'KEYWORD2B': "",
        'KEYWORD2C': ""
    }
    count = 0
    while count < 5:
        try:
            async with session.post(url, data=payload) as resp:
                content = await resp.text()
                ret_data = []
                try:
                    lol = json.loads(content)["data"]
                    for tk in lol:
                        ttl = tk["TTL"]
                        tempat_tanggal_lahir = ttl.split(",")
                        tk["TEMPAT_LAHIR"] = tempat_tanggal_lahir[0]
                        tgl_lahir = datetime.strptime(
                            tempat_tanggal_lahir[-1].strip(), "%d-%b-%y"
                        )
                        if tgl_lahir > datetime.now():
                            tgl_lahir -= relativedelta(years=100)
                        tk["TGL_LAHIR"] = tgl_lahir
                        ret_data.append(tk)
                    return ret_data
                except json.decoder.JSONDecodeError:
                    count += 1
                    await asyncio.sleep(5)
                except KeyError:
                    count += 1
                    await asyncio.sleep(5)
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
        return await cek_validitas_ktp(data, session)


async def run(data):
    tasks = []
    async with cs() as session:
        payload_login = {
            "login": "AK153580",
            "password": "TROLOLO10"
        }
        payload_role = "rule=8%7CD00"
        query_role = {
            "role": "8|D00",
            "rolename": "RO%20-%Account%20Representative%20%28%20D00%20%29"
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


def create_excel(datas):
    data = []
    for tk in datas:
        if tk:
            for t in tk:
                data.append(t)
    df = pd.DataFrame(
        data,
        columns=[
            "NAMA_LGKP", "TEMPAT_LAHIR", "TGL_LAHIR", "NIK", "NO_KK",
            "JENIS_KLMIN", "NAMA_LGKP_IBU", "ALAMAT_LENGKAP", "KODE_POS"
        ],
    )
    df["NIK"] = df["NIK"].astype(str)
    df["NO_KK"] = df["NO_KK"].astype(str)
    try:
        df["TGL_LAHIR"] = pd.to_datetime(
            df["TGL_LAHIR"],
            format="%d-%b-%y"
        )
    except ValueError:
        print("ValueError")
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "NAMA_TGL{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    writer = pd.ExcelWriter(
        save_filename, engine="xlsxwriter",
        datetime_format='dd-mm-yyyy'
    )
    df.to_excel(
        writer, sheet_name="Merged", index=False,
    )
    writer.save()


if __name__ == "__main__":
    df = pd.read_excel(
        r"D:\Documents\cek_ktp_nama.xlsx",
        sheet_name=3
    )
    data_dict = df.to_dict('records')
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(data_dict))
    results = loop.run_until_complete(future)
    create_excel(results)
