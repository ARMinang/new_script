from datetime import datetime
import pandas as pd
import os
from datetime import datetime
from download_dutk import DT_INTEGER

INDIR = "indir"
OUTDIR = "outdir"
DT_DATE = [
    "TGL_NA", "TGL_LAHIR"
]

def compare_age(bornday):
    try:
        born = datetime.strptime(bornday, "%d-%b-%y")
        today = datetime.today()
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

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

def naming_file(name):
    outpath = os.path.join(os.getcwd(), OUTDIR, "split")
    create_dir_ifn_exist(outpath)
    return os.path.join(outpath, "{}.xlsx".format(name))

def split_data_column(col_value):
    infile = os.path.join(os.getcwd(), INDIR, "tahun_2022_na.xlsx")
    df = pd.read_excel(
        infile, 0  # 72
    )
    df["KODE_TK"] = df["KODE_TK"].apply(str)
    for dtime in DT_DATE:
        df[dtime] = compare_age(df[dtime])[1]
    for c in df.NPP.unique():
        with pd.ExcelWriter(
        naming_file(c),
        engine="xlsxwriter",
        datetime_format='dd-mm-yyyy',
    ) as writer:
         (df[df.NPP == c]).to_excel(
            writer, sheet_name="DUTK",
            index=False
        )

if __name__ == '__main__':
    split_data_column(0)