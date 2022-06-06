import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from aiohttp import client_exceptions
import json
import random
from itertools import groupby
import os
from datetime import datetime


INDIR = "indir"
OUTDIR = "outdir"


def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)


async def cek_data(data, session):
    url = (
        'http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn5004_'
        'grid_tk_aktif_query.php?{}'.format(random.random())
    )
    payload = {
        'TYPE': "getTK",
        'SEARCHA': "sc_kpj",
        'SEARCHB': data["kpj"],
        'SEARCHC': "",
        'SEARCHD': "",
        'SEARCHE': "",
        'SEARCHF': "",
        'SEARCHG': "",
        'PAGE': 1,
    }
    count = 0
    while count <= 5:
        try:
            async with session.post(url, data=payload) as response:
                acc_kode_kantor = ["D14", "D15", "D00"]
                content = await response.text()
                data_tk = json.loads(content[71:])
                pku_kota = [d for d in data_tk if d['KODE_KANTOR'] in acc_kode_kantor]
                if len(pku_kota) > 1:
                    filtered_recs = []
                    for key, group_iter in groupby(pku_kota, lambda data_tk: data_tk["KPJ"]):
                        recent_rec = max(group_iter, key = lambda rec: datetime.strptime(rec["TGL_KEPESERTAAN"], "%d-%m-%Y"))
                        filtered_recs.append(recent_rec)
                    return filtered_recs
                return pku_kota
        except asyncio.exceptions.TimeoutError:
            count += 1
            print("%s - Timeouterror - count %d" % (data["kpj"], count))
            await asyncio.sleep(5)
        except client_exceptions.ClientConnectorError:
            count += 1
            print("%s - ClientConnectorError -  count %d" % (data["kpj"], count))
            await asyncio.sleep(5)
        except client_exceptions.ClientOSError:
            count += 1
            print("%s - ClientOSError - count %d" % (data["kpj"], count))
            await asyncio.sleep(5)
        except json.decoder.JSONDecodeError:
            count += 1
            print("%s - JSONDecodeError - count %d" % (data["kpj"], count))
            await asyncio.sleep(5)


def do_check(data, session):
    return cek_data(data, session)


async def safe_download(data, session):
    sem = asyncio.Semaphore(10)
    async with sem:
        return await do_check(data, session)


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
    for tks in datas:
        for tk in tks:
            if tk:
                data.append(tk)
    df = pd.DataFrame(data)
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "CEK_KPJ_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    with pd.ExcelWriter(
        save_filename, engine="xlsxwriter",
        datetime_format='mm-yyyy'
    ) as writer:
        df.to_excel(
            writer, sheet_name="Merged", index=False,
            columns=[
                "NOMOR_IDENTITAS", "KPJ", "NAMA_LENGKAP", "TGL_LAHIR", "NPP",
                "NAMAPRS", "TGL_KEPESERTAAN", "TGL_NA", "NM_PRG", "STATUS_VALID_IDENTITAS",
                "AKTIF", "PEMBINA"
            ]
        )


if __name__ == '__main__':
    input_file = "test_cek_kpj.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    df = pd.read_excel(
        input_abs,
        0,
        converters={'ktp': str}
    )
    data_dict = df.to_dict('records')
    results = asyncio.run(run(data_dict))
    create_excel(results)
