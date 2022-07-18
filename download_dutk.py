import xml.etree.ElementTree as etree
import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from datetime import datetime, date
import aiohttp
import os

URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?butt"
    "on=Submit&username=smile&password=smilekanharimu&authtype=D&mask=GQ%253D%"
    "253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26desformat%3DXML%"
    "26report%3DKNR1101.rdf%26userid%3D%2Fdata%2Freports%2Fkn%26%26P_NPP%3D"
    "'{npp}'%26P_PERIODE%3D'{periode}'%26P_USER%3D'SE251740'"
)

HEADER_DUTK = [
    "CS_NO", "NOMOR_PEGAWAI", "BLTH", "NIK", "KPJ", "NAMA", "TANGGAL_LAHIR",
    "PERIODE_KEPESERTAAN", "UPAH_TK", "RAPEL_UPAH_TK", "JHT", "JKM", "JKK",
    "JPN", "IURAN", "CF_IURAN"
]

DT_INTEGER = [
    "upah_tk", "rapel_upah_tk", "jht", "jkm", "jkk", "jpn", "iuran", "cf_iuran"
]

DT_DATE = [
    "periode_kepesertaan", "blth"
]

CAPTCHA_DIR = "captcha"
INDIR = "indir"
OUTDIR = "outdir"

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

def compare_age(bornday):
    try:
        born = datetime.strptime(bornday, "%d-%b-%y")
        today = date.today()
        if born > datetime.now():
            born = datetime(born.year - 100, born.month, born.day)
        age = (
            today.year - born.year - (
                (today.month, today.day) < (born.month, born.day)
            )
        )
        return age, born
    except TypeError:
        return 0, datetime.today()


async def download_dutk(data, session):
    url = URL.format(**data)
    data_dutk = []
    loop = 0
    while loop < 10:
        try:
            async with session.get(url) as response:
                content = await response.read()
                context = etree.fromstring(content)
                for elem in context.iter("G_KODE_CABANG"):
                    npp = elem[2].text
                    nama_perusahaan = elem[4].text
                    for child in elem:
                        for list_tk in child.findall("G_NIK"):
                            singleTk = dict(
                                (
                                    e.lower(),
                                    list_tk.find(e).text
                                    if list_tk.find(e) is not None else ""
                                )
                                for e in HEADER_DUTK
                            )
                            singleTk["npp"] = npp
                            singleTk["nama_perusahaan"] = nama_perusahaan
                            age, born = compare_age(singleTk["tanggal_lahir"])
                            singleTk["tanggal_lahir"] = born
                            data_dutk.append(singleTk)
                return data_dutk
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
    return download_dutk(npp, session)


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
        results = await asyncio.gather(*tasks)
        return results


def create_excel(dutks):
    time_signature = datetime.now().strftime('%Y%m%d_%H%M%S')
    data = []
    for dutk in dutks:
        if dutk is not None:
            data.extend(dutk)
    df = pd.DataFrame(
        data,
        columns=[
            "npp", "nama_perusahaan", "nomor_pegawai", "blth", "nik",
            "kpj", "nama", "tanggal_lahir", "periode_kepesertaan",
            "upah_tk", "rapel_upah_tk", "jht", "jkm", "jkk", "jpn",
            "iuran", "cf_iuran"
        ]
    )
    for num in DT_INTEGER:
        df[num] = pd.to_numeric(df[num], downcast="float")
    for dtime in DT_DATE:
        df[dtime] = pd.to_datetime(df[dtime], format="%d-%b-%y")
    outpath = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outpath)
    outname = os.path.join(outpath, "dutk-{}.xlsx".format(time_signature))
    with pd.ExcelWriter(
        outname,
        engine="xlsxwriter",
        datetime_format='mm-yyyy',
    ) as writer:
        df.to_excel(
            writer, sheet_name="DUTK",
            index=False
        )

def start_download(sheet):
    # variable npp dan periode
    infile = os.path.join(os.getcwd(), INDIR, "download_dutk.xlsx")
    df = pd.read_excel(
        infile, sheet  # 72
    )
    df["periode"] = df["periode"].dt.strftime("%d-%m-%Y")
    data_dict = df.to_dict('records')
    results = asyncio.run(run(data_dict))
    create_excel(results)


if __name__ == '__main__':
    # for i in range(0, 4):
    start_download(0)
