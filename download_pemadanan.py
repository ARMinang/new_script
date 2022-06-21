from turtle import xcor
import xml.etree.ElementTree as etree
import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from datetime import datetime, date
import aiohttp
import aiohttp.client_exceptions as ce
import os
import csv

INDIR = "indir"
OUTDIR = "outdir"


URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?butt"
    "on=Submit&username=smile&password=smilekanharimu&authtype=D&mask=GQ%253D%"
    "253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26desformat%3DDELI"
    "MITEDDATA%26delimiter%3D|%26report%3DKNR1415.rdf%26userid%3D%2Fdata%2Frep"
    "orts%2Fkn%26%26P_KODE_KANTOR%3D'D00'%26P_KODE_USER%3D'{pembina}'"
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
                        print("Nope")
                return list_npp
        except ce.ServerDisconnectedError:
            print("ServerDisconnectedError: " + data["pembina"] + " - " + str(loop))
            loop += 1
            await asyncio.sleep(10)


def do_download(npp, session):
    return dowload_pengkinian(npp, session)


async def safe_download(npp, session):
    sem = asyncio.Semaphore(2)
    async with sem:
        return await do_download(npp, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for npp in data:
            task = asyncio.ensure_future(safe_download(npp, session))
            tasks.append(task)
        result = await asyncio.gather(*tasks)
        return result


def create_excel(dutks):
    data = []
    for dutk in dutks:
        if dutk is not None:
            data.extend(dutk)
            
    df = pd.DataFrame(
        data,
        columns=HEADER
    )
    for num in DT_INTEGER:
        df[num] = pd.to_numeric(df[num], downcast="float")
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "NIK_INVALID_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    with pd.ExcelWriter(
        save_filename,
        engine="xlsxwriter",
        datetime_format='mm-yyyy',
    ) as writer:
        df.to_excel(
            writer, sheet_name="Lol",
            index=False
        )


def start_download(sheet):
    # variable npp dan periode
    pembinas = [
        "AK153580", "AL160740", "AR134060", "ED160810", "FA165960", "FE174690",
        "FI252510", "GR153600", "HA258950", "NI273920", "RA174700", "RA248900",
        "RO259060", "SU122530", "AD167270", "SR234440", "RA160700"
    ]
    # pembinas = [
    #     "ER174750", "RA179850", "SE251740", "TI277790", "FA251440", "TR178810"
    #     "AN188630"
    # ]
    today = datetime.today().strftime("%d-%m-%Y")
    all_data = []
    for pembina in pembinas:
        data_dict = dict()
        data_dict["pembina"] = pembina
        data_dict["tanggal"] = today
        all_data.append(data_dict)
    result = asyncio.run(run(all_data))
    create_excel(result)


if __name__ == '__main__':
    start_download(0)
