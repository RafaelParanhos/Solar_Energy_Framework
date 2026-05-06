import time

import pandas as pd

import math

import numpy as np

start = time.time()

qty_strings = 18
pattern = 1

inv_step = 9842.2311

PATTERNS = {
    1: [1,2,3,5,6,7,10,11,12,15,16,17,19,20,21,24,25,26],
    2: [1,2,3,5,6,7,10,11,12,15,16,17,19,20,21,24,25],
    3: [1,2,3,5,6,7,10,11,15,16,17,19,20,21,24,25]
}

with pd.ExcelFile(r"C:\Users\Usuario\Ecotechnee\Office - Documentos\Projetos\302 - Zagope - UFV Sendim\01 - Projetos\01 - Em Confecção\02 - Edição\02 - Planilhas\UFV Sendim - Unifilar BT.xlsx") as xlsx:
    df_parameter = pd.read_excel(xlsx, sheet_name="Parametros", usecols="C:D", header=1)
    df_inv = pd.read_excel(xlsx, sheet_name="INV", usecols="A:H", header=1)
    df_pot = pd.read_excel(xlsx, sheet_name="Planilha1")
    df_type = pd.read_excel(xlsx, sheet_name="Sheet1")


df_inv['X'] = df_parameter.iloc[0, 1]+(df_inv["PCS"]*2-3+df_inv["QG"])*df_parameter.iloc[2, 1]+df_parameter.iloc[3, 1]*np.trunc((df_inv["DJ"]-1)/df_parameter.iloc[5, 1])
df_inv['Y'] = (inv_step * (8 - df_inv['UFV'])) + (df_parameter.iloc[1, 1] + (df_inv['DJ'] - np.ceil(df_inv['DJ'] / df_parameter.iloc[5, 1]) * df_parameter.iloc[5, 1] + df_parameter.iloc[5, 1] - 1.0) * df_parameter.iloc[4, 1])
df_inv['X tag'] = df_inv['X'] + df_parameter.iloc[6, 1]
df_inv['Y tag'] = df_inv['Y'] + df_parameter.iloc[7, 1]
df_inv['X cabo'] = df_parameter.iloc[0, 1]+(df_inv['PCS']*2+df_inv['QG']-3)*df_parameter.iloc[2, 1]+df_parameter.iloc[15, 1]
df_inv['Y cabo'] = np.nan

df_inv = df_inv.sort_values(by=["UFV", "PCS", "QG", "DJ"]).reset_index(drop=True)

df_inv['Y cabo'] = np.where(
    (df_inv['QG'] == 1) & (df_inv['DJ'] == 1),
    (inv_step * (8 - df_inv["UFV"])) + (df_parameter.iloc[1, 1] + df_parameter.iloc[16, 1]),
    np.where(
        (df_inv['QG'] == 2) & (df_inv['DJ'] == 1),
        (inv_step * (8 - df_inv["UFV"])) + (df_parameter.iloc[1, 1] + df_parameter.iloc[17, 1]),
        df_inv['Y cabo'].shift() + df_parameter.iloc[18,1]
    )
)

df_inv['TAG Cabo CA'] = "CLV-" + df_inv["UFV"].astype(str) + "." + \
    df_inv["PCS"].astype(str) + "." + \
    df_inv['INV'].astype(str).str.zfill(2) + \
    ".(L1-L2-L3) - " + \
    df_inv["Dist"].round(1).astype(str) + "m"

df_inv['Script 1'] = "-insert Inversor " + \
                     df_inv['X'].astype(str) + \
                     df_inv['Y'].astype(str) + \
                     " 1 1 0"

df_inv['Script 2'] = "(command-s \"._mtext\" \""+ df_inv['X tag'].astype(str) + "," + \
                     df_inv['Y tag'].astype(str) + \
                     "\" \"h\" 30  \"j\" \"ML\" \"r\" -90  \"w\" 0 \"" + \
                     df_inv['TAG'].astype(str) + \
                     "\" \"\")"

df_inv['Script 3'] = "(command-s \"._mtext\" \""+ df_inv['X cabo'].astype(str) + "," + \
                     df_inv['Y cabo'].astype(str) + \
                     "\" \"h\" 20  \"j\" \"TL\" \"r\" 0  \"w\" 0 \"" + \
                     df_inv['TAG Cabo CA'].astype(str) + \
                     "\" \"\")"


with open (r"C:\Users\Usuario\Desktop\Pessoal\Python\Diagrama Unifilar\Script Inversor.scr", 'w') as f:
    f.write('-LAYER\nSET\nEMF_unifilar_inversor\n\n')
    f.write('\n'.join(df_inv["Script 1"].astype(str)) + '\n')

with open (r"C:\Users\Usuario\Desktop\Pessoal\Python\Diagrama Unifilar\Script TAG Inversor.scr", 'w') as f:
    f.write('-LAYER\nSET\nEMF_TAG_inversor\n\n')
    f.write('\n'.join(df_inv["Script 2"].astype(str)) + '\n')

with open(r"C:\Users\Usuario\Desktop\Pessoal\Python\Diagrama Unifilar\Script Cabos CA.scr", 'w') as f:
    f.write('-LAYER\nSET\nCA_Cabos\n\n')
    f.write('\n'.join(df_inv["Script 3"].astype(str)) + '\n')

df_inv = df_inv.rename(columns={df_inv.columns[0]: "TAG INV"})



 # Strings TAGs
df_strings = pd.DataFrame({
    'TAG': df_type["Tag completa"],
    'TAG Especial': None,
    'TAG INV': None,
    'TAG PST': None,
    'UFV': df_type["UFV"],
    'PST': df_type["PST"],
    'INV': df_type["INV"],
    'STR': df_type["STR"],
    'Type': df_type["Type"],
    'Type N': df_type["Type N"],
    'Qty. of Strings': qty_strings,
    'PD': pattern,
    'PV': None,
    'X': None,
    'Y': None,
    'XX': None,
    'YY': None,
    'Script TAG String': None,
    'Script Potência': None
})

df_strings["TAG Especial"] = "("+df_strings['Type'].astype(str)+") "+df_strings['TAG'].astype(str)
df_strings["TAG INV"] = "INV-" + df_strings["TAG"].astype(str).str.slice(4, 10)
df_strings["TAG PST"] = "PST-" + df_strings["TAG"].astype(str).str.slice(4, 7)
df_strings = df_strings.merge(df_pot, on="TAG INV", how="left")

df_strings["Qty. of Strings"] = np.where(
    (df_strings["TAG PST"] == "PST-5.2") | (df_strings["TAG PST"] == "PST-7.2") | (df_strings["TAG PST"] == "PST-8.2"),
    17,
    np.where(
        (df_strings["TAG PST"] == "PST-5.1") | (df_strings["TAG PST"] == "PST-7.1"),
        16,
        df_strings["Qty. of Strings"]
    )
)

df_strings["PD"] = np.where(
    (df_strings["TAG PST"] == "PST-5.2") | (df_strings["TAG PST"] == "PST-7.2") | (df_strings["TAG PST"] == "PST-8.2"),
    2,
    np.where(
        (df_strings["TAG PST"] == "PST-5.1") | (df_strings["TAG PST"] == "PST-7.1"),
        3,
        df_strings["PD"]
    )
)

df_strings["Qty. of Strings"] = np.where(
    (df_strings["TAG INV"] == "INV-5.2.11") | (df_strings["TAG INV"] == "INV-5.2.15") | (df_strings["TAG INV"] == "INV-5.2.19"),
    16,
    np.where(
        (df_strings["TAG INV"] == "INV-6.1.18") | (df_strings["TAG INV"] == "INV-6.1.19") | (df_strings["TAG INV"] == "INV-6.1.20"),
        17,
        df_strings["Qty. of Strings"]
    )
)

df_strings["PD"] = np.where(
    (df_strings["TAG INV"] == "INV-5.2.11") | (df_strings["TAG INV"] == "INV-5.2.15") | (df_strings["TAG INV"] == "INV-5.2.19"),
    3,
    np.where(
        (df_strings["TAG INV"] == "INV-6.1.18") | (df_strings["TAG INV"] == "INV-6.1.19") | (df_strings["TAG INV"] == "INV-6.1.20"),
        2,
        df_strings["PD"]
    )
)

all_tags = df_strings["TAG INV"].unique()
c_counts = (df_strings[df_strings["Type N"] == 3]["TAG INV"].value_counts().reindex(all_tags, fill_value=0))

for tag in df_strings["TAG"]:
    index = df_strings[df_strings["TAG"] == tag].index[0]
    tag_inv = df_strings.at[index, "TAG INV"]
    c_count = c_counts[tag_inv]
    if df_strings.at[index, "Qty. of Strings"] == 17 and c_count == 1:
        if df_strings.at[index, "Type N"] == 2:
            df_strings.at[index, "Type N"] = 5
        elif df_strings.at[index, "Type N"] == 3:
            df_strings.at[index, "Type N"] = 4


df_strings = df_strings.sort_values(by=["UFV", "PST", "INV", "Type N"]).reset_index(drop=True)

df_strings["Position in group"] = df_strings.groupby("PD").cumcount()

# adicionar C,T,B
df_strings['PV'] = df_strings.apply(
    lambda row: PATTERNS[row['PD']][row['Position in group'] % len(PATTERNS[row['PD']])],
    axis=1
)

df_strings = df_strings.merge(df_inv[["TAG INV", "X", "Y"]], on="TAG INV", how="left")

df_strings['X_x'] = df_strings['X_y'].astype(float)+df_parameter.iloc[9,1]
df_strings['Y_x'] = df_strings['Y_y'].astype(float)+df_parameter.iloc[10,1]+df_parameter.iloc[11, 1]*(df_strings["PV"].astype(float)-1)
df_strings['XX'] = df_strings['X_y'].astype(float)+df_parameter.iloc[12,1]
df_strings['YY'] = df_strings['Y_y'].astype(float)+df_parameter.iloc[13,1]+df_parameter.iloc[14, 1]*(df_strings["PV"].astype(float)-1)

df_strings['Script TAG String'] = "(command-s \"._mtext\" \""+ df_strings['X_x'].astype(str) + "," + \
                     df_strings['Y_x'].astype(str) + \
                     "\" \"h\" 20  \"j\" \"MR\" \"r\" 0  \"w\" 0 \"" + \
                     df_strings['TAG Especial'].astype(str) + \
                     "\" \"\")"

df_strings['Script Potência'] = "(command-s \"._mtext\" \""+ df_strings['XX'].astype(str) + "," + \
                     df_strings['YY'].astype(str) + \
                     "\" \"h\" 20  \"j\" \"MC\" \"r\" 0  \"w\" 0 \"" + \
                     df_strings['Module Power'].astype(str) + \
                     "\" \"\")"

df_strings.to_excel(r"C:\Users\Usuario\Desktop\Pessoal\Python\Diagrama Unifilar\Diagrama Unifilar BT.xlsx")

with open (r"C:\Users\Usuario\Desktop\Pessoal\Python\Diagrama Unifilar\tag string.scr", 'w') as f:
    f.write('-LAYER\nSET\nTAG_strings\n\n')
    f.write('\n'.join(df_strings["Script TAG String"].astype(str)) + '\n')

with open (r"C:\Users\Usuario\Desktop\Pessoal\Python\Diagrama Unifilar\potencia.scr", 'w') as f:
    f.write('-LAYER\nSET\nPotência_Módulo\n\n')
    f.write('\n'.join(df_strings["Script Potência"].astype(str)) + '\n')


end = time.time()
elapsed = end - start
print(elapsed)