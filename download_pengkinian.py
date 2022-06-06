import xml.etree.ElementTree as etree
import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from datetime import datetime, date
import aiohttp
import os


INDIR = "indir"
OUTDIR = "outdir"


URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?butt"
    "on=Submit&username=smile&password=smilekanharimu&authtype=D&mask=GQ%253D%"
    "253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26desformat%3DXML%"
    "26report%3DKNR4001.rdf%26userid%3D%2Fdata%2Freports%2Fkn%26%26P_PEMBINA%3"
    "D%27{pembina}%27%26P_USER%3D%27{pembina}%27%26P_KANTOR_PEMBINA%3D%27D00%2"
    "7%26P_KODE_KANTOR%3D%27D00%27%26P_KODE_SEGMEN%3D%27PU%27%26P_TGL_PROSES%3"
    "D%27{tanggal}%27"
)

HEADER = [
    "NO", "NOMOR_IDENTITAS", "KPJ", "NPP", "KODE_DIVISI", "NAMA_PERUSAHAAN",
    "NAMA_LENGKAP", "TEMPAT_LAHIR", "TGL_LAHIR", "NAMA_IBU_KANDUNG",
    "BLTH_KEPESERTAAN", "EMAIL", "HP", "AKTIF", "BLTH_NA", "NO_REKENING",
    "NAMA_BANK", "NAMA_REKENING", "NAMA_SUMBER_DATA", "STATUS_VALID_IDENTITAS",
    "VALID_IBU_KANDUNG", "VALID_EMAIL", "VALID_HP", "VALID_NOREK"
]

DT_DATE = [
    "tgl_lahir", "blth_na", "blth_kepesertaan"
]

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

async def dowload_pengkinian(data, session):
    url = URL.format(**data)
    data_tk = []
    loop = 0
    while loop < 10:
        try:
            async with session.get(url) as response:
                content = await response.read()
                context = etree.fromstring(content)
                for elem in context.iter("G_NAMA_KANTOR_WIL"):
                    for child in elem:
                        for list_tk in child.findall("G_NAMA_KANTOR"):
                            singleTk = dict(
                                (
                                    e.lower(),
                                    list_tk.find(e).text
                                    if list_tk.find(e) is not None else ""
                                )
                                for e in HEADER
                            )
                            singleTk["pembina"] = data["pembina"]
                            data_tk.append(singleTk)
                return data_tk
        except asyncio.TimeoutError:
            print("TimeoutError: %s, loop: %s" % (data["npp"], loop))
            loop += 1
            await asyncio.sleep(5)
        except aiohttp.ClientError:
            print(
                "ServerDisconnectedError: %s, loop: %s" % (data["npp"], loop)
            )
            loop += 1
            await asyncio.sleep(5)
        except etree.ParseError:
            print("ParserError: %s, loop: %s" % (data["npp"], loop))
            loop += 1
            await asyncio.sleep(5)
    return None


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
    data_invalid = []
    for dutk in dutks:
        if dutk is not None:
            for tk in dutk:
                if tk:
                    if tk["status_valid_identitas"] == "T":
                        data_invalid.append(tk)
            data.extend(dutk)
            
    df = pd.DataFrame(
        data,
        columns=list(map(str.lower, HEADER)).append("pembina")
    )
    for dtime in DT_DATE:
        df[dtime] = pd.to_datetime(df[dtime], format="%d-%b-%y")
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "PENGKINIAN_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
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
        if data_invalid:
            dfi = pd.DataFrame(
                data_invalid,
                columns=list(map(str.lower, HEADER)).append("pembina")
            )
            for dtime in DT_DATE:
                dfi[dtime] = pd.to_datetime(dfi[dtime], format="%d-%b-%y")
            dfi.to_excel(
                writer, sheet_name="invalid",
                index=False
            )


def start_download(sheet):
    # variable npp dan periode
    pembinas = [
        "AK153580", "AL160740", "AR134060", "ED160810", "FA165960", "FE174690",
        "FI252510", "GR153600", "HA258950", "NI273920", "RA174700", "RA248900",
        "RO259060", "SU122530", "AD167270", "SR234440", "RA160700"
    ]
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
