import asyncio
from aiohttp import ClientSession as cs
import aiohttp.client_exceptions as ce
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, date
import os


INDIR = "indir"
OUTDIR = "outdir"

HEADER_ESERVICE = [
    "NOMOR_IDENTITAS", "NO_PESERTA", "NAMA_PESERTA", "TEMPAT_LAHIR", "TGL_LAHIR",
    "VALID_NIK", "NO_HP", "EMAIL", "CF_MOBILE"
]

URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?"
    "button=Submit&username=smile&password=smilekanharimu&authtype=D&mask="
    "GQ%253D%253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26desf"
    "ormat%3DXML%26report%3DKNRECH00001.rdf%26userid%3D%2Fdata%2Freports%2"
    "Fkn%26%26P_USER%3D%27{pembina}%27%26P_NPP%3D%27{npp}%27"
)

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


async def download_eservice(data, session):
    url = URL.format(**data)
    loop = 0
    while loop < 10:
        try:
            async with session.get(url) as response:
                content = await response.text()
                tree = ET.fromstring(content)
                tks = tree.iter('G_DIV')
                all_tk = []
                for tk in tks:
                    single_tk = dict(
                        (
                            e.lower(),
                            tk.find(e).text
                            if tk.find(e) is not None else ""
                        )
                        for e in HEADER_ESERVICE
                    )
                    age, born = compare_age(single_tk["tgl_lahir"])
                    single_tk["tgl_lahir"] = born
                    single_tk["npp"] = data["npp"]
                    all_tk.append(single_tk)
                return all_tk
        except ce.ServerDisconnectedError:
            print("ServerDisconnectedError: " + data["pembina"] + " - " + str(loop))
            loop += 1
            await asyncio.sleep(10)


async def safe_download(npp, session):
    sem = asyncio.Semaphore(2)
    async with sem:
        return await download_eservice(npp, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for npp in data:
            task = asyncio.ensure_future(safe_download(npp, session))
            tasks.append(task)
        result = await asyncio.gather(*tasks)
        return result

def create_excel(data, outname):
    # flattened the data
    final_data = [x for xs in data for x in xs]
    df = pd.DataFrame(
        final_data
    )
    df["tgl_lahir"] = pd.to_datetime(df["tgl_lahir"], format="%d-%b-%y")
    df_list = [d for _, d in df.groupby(['npp'])]
    with pd.ExcelWriter(
            outname,
            engine="xlsxwriter",
            datetime_format='dd-mm-yyyy',
        ) as writer:
            for a_df in df_list:
                a_df.to_excel(
                    writer,
                    sheet_name=a_df["npp"].iloc[0],
                    columns=[x.lower() for x in HEADER_ESERVICE],
                    index=False
    )

if __name__ == "__main__":
    input_file = "eservice.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    outname = os.path.join(
        outdir,
        "ESERVICE_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    df = pd.read_excel(
        input_abs,
    )
    data_dict = df.to_dict('records')
    result = asyncio.run(run(data_dict))
    if result:
        create_excel(result, outname)