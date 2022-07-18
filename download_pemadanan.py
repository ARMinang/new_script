import json
import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from datetime import datetime
import aiohttp.client_exceptions as ce
import os
import csv
from dw_pemadanan_npp import dw_tk_invalid
INDIR = "indir"
OUTDIR = "outdir"


URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?butt"
    "on=Submit&username=smile&password=smilekanharimu&authtype=D&mask=GQ%253D%"
    "253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26desformat%3DDELI"
    "MITEDDATA%26delimiter%3D|%26report%3DKNR1415.rdf%26userid%3D%2Fdata%2Frep"
    "orts%2Fkn%26%26P_KODE_KANTOR%3D'{kode_kantor}'%26P_KODE_USER%3D'{pembina}"
    "'"
)

HEADER = ["NPP", "NIK_INVALID_AKTIF", "NIK_INVALID_NA", "PEMBINA"]
DT_INTEGER = ["NIK_INVALID_AKTIF", "NIK_INVALID_NA"]

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

async def dowload_pengkinian(data, session):
    url = URL.format(**data)
    loop = 0
    while loop < 10:
        try:
            async with session.get(url) as response:
                content = await response.text()
                lines = content.splitlines()
                a = [
                    {
                        k: v for k, v in row.items()
                    } for row in csv.DictReader(
                        lines, delimiter="|", skipinitialspace=True
                    )
                ]
                list_npp = []
                for x in a:
                    try:
                        if int(x["NIK_INVALID_NA"]) > 0 and int(x["NIK_INVALID_AKTIF"]) > 0:
                            list_npp.append(x)
                    except ValueError:
                        pass
                return list_npp
        except ce.ServerDisconnectedError:
            print("ServerDisconnectedError: " + data["pembina"] + " - " + str(loop))
            loop += 1
            await asyncio.sleep(10)


async def safe_download(npp, session):
    sem = asyncio.Semaphore(2)
    async with sem:
        return await dowload_pengkinian(npp, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for npp in data:
            task = asyncio.ensure_future(safe_download(npp, session))
            tasks.append(task)
        result = await asyncio.gather(*tasks)
        return result


def create_excel(npps, tks):
    all_tk = []
    for data in tks:
        all_tk.extend(data)
    df = pd.DataFrame(
        npps,
        columns=HEADER
    )
    df_tk = pd.DataFrame.from_dict(all_tk)
    df_tk["TGL_LAHIR"] = df_tk['TGL_LAHIR'].dt.date
    for num in DT_INTEGER:
        df[num] = pd.to_numeric(df[num], downcast="float")

    # Create re-cap of data invalid per pembina
    df_rec = df_tk.groupby(["PEMBINA"])["PEMBINA"].count()
    # Create output filename
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "NIK_INVALID_TK_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )

    # Save downloaded data as xslx file
    with pd.ExcelWriter(
        save_filename,
        engine="xlsxwriter",
        datetime_format='dd-mm-yyyy',
    ) as writer:
        df_tk.to_excel(
            writer, sheet_name="Data Invalid",
            index=False,
            columns=[
                "NPP", "DIV", "NAMA_PERUSAHAAN", "NIK", "KPJ", "NAMA_TK", "TEMPAT_LAHIR",
                "TGL_LAHIR", "TGL_KEPS", "BLTH_NA", "PEMBINA"
            ]
        )
        df.to_excel(
            writer, sheet_name="Rekap",
            index=False
        )
        df_rec.to_excel(
            writer, sheet_name="Rekap-pembina",
            index=False
        )

def start_download():
    pembinas = []
    with open(os.path.join(os.getcwd(), "pembinas.json")) as f:
        pembinas = json.load(f)
    result = asyncio.run(run(pembinas))
    npps = []
    for npp in result:
        if npp is not None:
            npps.extend(npp)
    data_download_npp = [d["NPP"] for d in npps]
    data_tk = asyncio.run(dw_tk_invalid(data_download_npp))
    create_excel(npps, data_tk)


if __name__ == '__main__':
    start_download()
