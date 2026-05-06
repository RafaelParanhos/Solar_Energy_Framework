import pandas as pd
import numpy as np

cards = pd.read_excel(r"C:\Users\Usuario\Desktop\Pessoal\magic.xlsx", sheet_name="Planilha4", usecols = "A:B")


cards['quantity'] = cards['moxfield'].astype(str).str[0].astype(int)

cards_grouped = cards.groupby("Name").sum()
cards_grouped = cards_grouped.drop(columns='moxfield')
print(cards_grouped)

duplicates = cards_grouped[cards_grouped['quantity'] > 1]
print(duplicates)