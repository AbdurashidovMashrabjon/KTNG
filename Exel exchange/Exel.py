import pandas as pd
from tkinter import Tk, filedialog
import os

Tk().withdraw()

# Выбираем первый файл (маленький – 686 строк)
print("Выбери файл 1 (Ex: POG and Backwall Iyul transactions 1.xlsx):")
file1 = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])

# Выбираем второй файл (большой – 7000+ строк)
print("Выбери файл 2 (Ex: POG and Backwall Iyul transactions 2.xlsx):")
file2 = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])

# Читаем Excel файлы
df1 = pd.read_excel(file1)
df2 = pd.read_excel(file2)

# Убираем пробелы в названиях колонок
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

print("Файл 1:", df1.columns.tolist())
print("Файл 2:", df2.columns.tolist())

# Объединяем по колонке "Phone"
merged = pd.merge(
    df1,
    df2,
    on="Phone",
    how="left"   # оставляем все строки из df1 (686 строк)
)

# Оставляем только нужные колонки
final = merged[[
    "Phone",
    "Описание",
    "Сумма (UZS)",
    "Data",
    "Код ТТ",
    "Название",
    "Город",
    "Дата регистрации"
]]

# Создаём папку "output" в проекте (если её нет)
os.makedirs("output", exist_ok=True)

# Сохраняем результат
result_file = os.path.join("output", "result.xlsx")
final.to_excel(result_file, index=False)

print("✅ Готово! Файл сохранён в:", result_file)
